"""
데이터 누락 감지 및 복구 모듈

WebSocket에서 누락된 데이터를 감지하고 REST API를 통해 복구합니다.
"""

import pandas as pd
import numpy as np
import logging
import time
import threading
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple
import requests
import json

logger = logging.getLogger(__name__)

class DataRecoveryManager:
    """데이터 누락 감지 및 복구 관리자"""
    
    def __init__(self, symbol: str = 'BTCUSDT', testnet: bool = True):
        """
        초기화
        
        Args:
            symbol: 거래 심볼
            testnet: 테스트넷 사용 여부
        """
        self.symbol = symbol
        self.testnet = testnet
        self.base_url = "https://api.bitget.com" if not testnet else "https://api-sandbox.bitget.com"
        
        # 데이터 누락 추적
        self.missing_periods = []  # 누락된 시간대 리스트
        self.last_check_time = None
        self.check_interval = 60  # 60초마다 체크
        
        # 복구 상태
        self.is_recovering = False
        self.recovery_lock = threading.Lock()
        
        logger.info(f"데이터 복구 관리자 초기화 - 심볼: {symbol}, 테스트넷: {testnet}")
    
    def check_missing_data(self, current_df: pd.DataFrame) -> List[datetime]:
        """
        누락된 데이터 기간 감지
        
        Args:
            current_df: 현재 데이터프레임
            
        Returns:
            List[datetime]: 누락된 시간대 리스트
        """
        if current_df.empty:
            return []
        
        # 시간 간격 계산
        time_diffs = current_df.index.to_series().diff()
        expected_interval = pd.Timedelta(minutes=1)
        
        # 1.5분 이상 간격이 있는 경우 누락으로 판단
        gaps = time_diffs[time_diffs > expected_interval * 1.5]
        
        missing_periods = []
        for gap_time, gap_duration in gaps.items():
            # 누락된 기간의 시작과 끝 시간 계산
            prev_time = gap_time - gap_duration
            missing_start = prev_time + expected_interval
            missing_end = gap_time - expected_interval
            
            # 누락된 각 1분봉 시간 생성
            current = missing_start
            while current <= missing_end:
                missing_periods.append(current)
                current += expected_interval
        
        return missing_periods
    
    def fetch_missing_data(self, start_time: datetime, end_time: datetime) -> Optional[pd.DataFrame]:
        """
        REST API를 통해 누락된 데이터 가져오기
        
        Args:
            start_time: 시작 시간
            end_time: 종료 시간
            
        Returns:
            pd.DataFrame: 복구된 데이터 또는 None
        """
        try:
            # 시간 범위를 1분 단위로 분할 (API 제한 고려)
            current = start_time
            all_data = []
            
            while current <= end_time:
                # 각 1분봉 데이터 요청
                data = self._fetch_single_minute_data(current)
                if data is not None:
                    all_data.append(data)
                
                current += timedelta(minutes=1)
                time.sleep(0.1)  # API Rate Limit 고려
            
            if not all_data:
                return None
            
            # DataFrame 생성
            df = pd.DataFrame(all_data)
            df.set_index('timestamp', inplace=True)
            df.sort_index(inplace=True)
            
            logger.info(f"누락 데이터 복구 완료: {len(df)}개 봉 ({start_time} ~ {end_time})")
            return df
            
        except Exception as e:
            logger.error(f"누락 데이터 복구 실패: {e}")
            return None
    
    def _fetch_single_minute_data(self, target_time: datetime) -> Optional[Dict]:
        """
        특정 1분봉 데이터 가져오기
        
        Args:
            target_time: 대상 시간
            
        Returns:
            Dict: OHLC 데이터 또는 None
        """
        try:
            # 비트겟 API 엔드포인트
            url = f"{self.base_url}/api/v2/mix/market/candles"
            
            # 시작 시간 (해당 분의 시작)
            start_time_ms = int(target_time.timestamp() * 1000)
            # 종료 시간 (해당 분의 끝)
            end_time_ms = int((target_time + timedelta(minutes=1)).timestamp() * 1000)
            
            params = {
                'symbol': self.symbol,
                'granularity': '1m',
                'startTime': start_time_ms,
                'endTime': end_time_ms,
                'limit': 1,
                'productType': 'usdt-futures'
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('code') != '00000' or not data.get('data'):
                logger.warning(f"데이터 없음: {target_time}")
                return None
            
            kline = data['data'][0]
            
            # 비트겟 kline 데이터 구조: [timestamp, open, high, low, close, volume, volCcy]
            timestamp_ms = int(kline[0])
            timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
            
            return {
                'timestamp': timestamp,
                'open': float(kline[1]),
                'high': float(kline[2]),
                'low': float(kline[3]),
                'close': float(kline[4]),
                'volume': float(kline[5])
            }
            
        except Exception as e:
            logger.error(f"1분봉 데이터 가져오기 실패 ({target_time}): {e}")
            return None
    
    def recover_missing_data(self, current_df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        누락된 데이터 복구
        
        Args:
            current_df: 현재 데이터프레임
            
        Returns:
            pd.DataFrame: 복구된 데이터가 포함된 DataFrame 또는 None
        """
        with self.recovery_lock:
            if self.is_recovering:
                logger.debug("이미 복구 중입니다.")
                return None
            
            self.is_recovering = True
            
            try:
                # 누락된 데이터 감지
                missing_periods = self.check_missing_data(current_df)
                
                if not missing_periods:
                    logger.debug("누락된 데이터가 없습니다.")
                    return current_df
                
                logger.warning(f"누락된 데이터 감지: {len(missing_periods)}개 기간")
                
                # 누락된 데이터 복구
                recovered_data = []
                for missing_time in missing_periods:
                    data = self._fetch_single_minute_data(missing_time)
                    if data is not None:
                        recovered_data.append(data)
                    time.sleep(0.1)  # API Rate Limit 고려
                
                if not recovered_data:
                    logger.warning("복구할 수 있는 데이터가 없습니다.")
                    return current_df
                
                # 복구된 데이터를 DataFrame으로 변환
                recovered_df = pd.DataFrame(recovered_data)
                recovered_df.set_index('timestamp', inplace=True)
                
                # 기존 데이터와 병합
                combined_df = pd.concat([current_df, recovered_df])
                combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
                combined_df.sort_index(inplace=True)
                
                logger.info(f"데이터 복구 완료: {len(recovered_data)}개 봉 추가")
                return combined_df
                
            except Exception as e:
                logger.error(f"데이터 복구 중 오류: {e}")
                return current_df
            finally:
                self.is_recovering = False
    
    def should_check_missing_data(self) -> bool:
        """
        누락 데이터 체크가 필요한지 확인
        
        Returns:
            bool: 체크 필요 여부
        """
        if self.last_check_time is None:
            return True
        
        return (datetime.now(timezone.utc) - self.last_check_time).total_seconds() >= self.check_interval
    
    def update_check_time(self):
        """체크 시간 업데이트"""
        self.last_check_time = datetime.now(timezone.utc)
