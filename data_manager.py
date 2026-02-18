"""
비트겟 실시간 데이터 관리 모듈

1분봉 데이터를 35분봉으로 리샘플링하고 지표를 계산합니다.
과거 데이터와 실시간 데이터를 통합하여 관리합니다.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List, Tuple
import logging
import threading
import time
import queue
from collections import deque
import os
import requests
import sys
import os

# 상위 디렉토리의 모듈 import를 위한 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from indicators import BitgetIndicatorCalculator
from bitget_client import BitgetRealtimeClient
from bitget_account import BitgetAccountManager
from data_recovery import DataRecoveryManager

# 로거 설정
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_bitget_server_time():
    """비트겟 서버 시간 가져오기 (밀리초) - 로컬 시간 사용"""
    try:
        # 비트겟에는 서버 시간 API가 없으므로 로컬 시간 사용
        now = datetime.now(timezone.utc)
        server_time_ms = int(now.timestamp() * 1000)
        # logger.debug(f"비트겟 로컬 시간 사용: {now.strftime('%Y-%m-%d %H:%M:%S.%f')} UTC")
        return server_time_ms, now
    except Exception as e:
        logger.error(f"비트겟 시간 가져오기 실패: {e}")
        # 실패 시 현재 시간 사용
        now = datetime.now(timezone.utc)
        return int(now.timestamp() * 1000), now


class BitgetDataManager:
    """
    비트겟 실시간 데이터 관리 클래스
    
    1분봉 데이터를 35분봉으로 리샘플링하고 지표를 계산합니다.
    """
    
    def __init__(self, symbol='BTCUSDT', testnet=False, strategy_callback=None, candle_1min_callback=None, candle_30min_callback=None, enable_account_sync=False):
        """
        초기화
        
        Args:
            symbol: 거래 심볼
            testnet: 테스트넷 사용 여부
            strategy_callback: 35분봉 완성 콜백 함수 (사용 안 함)
            candle_1min_callback: 1분봉 완성 콜백 함수 (진입 조건 체크용)
            candle_30min_callback: 30분봉 지표 계산 완료 콜백 함수 (진입 조건 체크용)
            enable_account_sync: 계정 동기화 활성화 여부
        """
        self.symbol = symbol
        self.testnet = testnet
        self.strategy_callback = strategy_callback
        self.candle_1min_callback = candle_1min_callback
        self.candle_30min_callback = candle_30min_callback
        self.enable_account_sync = enable_account_sync
        
        # 포지션 상태 저장 (WebSocket 재연결 시 복구용)
        self._position_state = {
            'position_size': 0.0,
            'position_value': 0.0,
            'avg_price': 0.0,
            'liquidation_price': None,
            'risk_level': 'LOW'
        }
        
        # 데이터 저장소 (메모리 최적화)
        self.minute_df = pd.DataFrame()
        self.last_35min_candle: Optional[datetime] = None
        self.max_data_points = 2000  # 최대 데이터 포인트 수 (메모리 제한)
        
        # 30분봉 및 4시간봉 데이터 저장소 (스퀴즈 모멘텀 전략용)
        self.df_30min: Optional[pd.DataFrame] = None
        self.df_4h: Optional[pd.DataFrame] = None
        self._last_30min_update: Optional[float] = None
        self._last_4h_update: Optional[float] = None
        self._update_interval_30min = 60  # 30분봉 데이터 60초마다 업데이트
        self._update_interval_4h = 300  # 4시간봉 데이터 5분마다 업데이트
        
        # 마지막 완성된 봉 시간 추적 (새로운 완성된 봉 감지용)
        self._last_completed_30min_candle_time: Optional[datetime] = None
        self._last_completed_4h_candle_time: Optional[datetime] = None
        
        # 로그 출력 시간 추적 (30분봉은 30분마다, 4시간봉은 4시간마다)
        self._last_30min_log_time: Optional[float] = None
        self._last_4h_log_time: Optional[float] = None
        self._log_interval_30min = 1800  # 30분 = 1800초
        self._log_interval_4h = 14400  # 4시간 = 14400초
        
        # 30분봉/4시간봉 지표 캐시 (계산 주기 사이에는 마지막 값 재사용)
        self._last_atr_30min: Optional[float] = None
        self._last_squeeze_val_30min: Optional[float] = None
        self._last_squeeze_prev_30min: Optional[float] = None
        self._last_squeeze_state_30min: Optional[Dict] = None
        self._last_close_30min: Optional[float] = None
        self._last_ema_4h: Optional[float] = None
        
        # REST API 데이터의 마지막 종가 (진입 조건용)
        self._last_completed_candle_close = None
        self._last_completed_candle_time = None
        self._last_candle_open = None
        self._last_candle_high = None
        self._last_candle_low = None
        
        # 지표 계산기
        self.indicator_calculator = BitgetIndicatorCalculator()
        
        # 계정 매니저 (선택적)
        self.account_manager = None
        if enable_account_sync:
            try:
                self.account_manager = BitgetAccountManager(testnet=testnet)
                logger.info("계정 동기화 활성화됨")
            except Exception as e:
                logger.warning(f"계정 동기화 활성화 실패: {e}")
                self.enable_account_sync = False
        
        # 데이터 복구 관리자
        self.data_recovery = DataRecoveryManager(symbol=symbol, testnet=testnet)
        logger.info("데이터 복구 시스템 활성화됨")
        
        # 실시간 클라이언트
        self.realtime_client = BitgetRealtimeClient(symbol=symbol, testnet=testnet)
        self.realtime_client.set_kline_callback(self._on_kline_data)
        try:
            cb_name = getattr(self._on_kline_data, '__qualname__', getattr(self._on_kline_data, '__name__', str(self._on_kline_data)))
        except Exception:
            cb_name = str(self._on_kline_data)
        logger.info(f"[DM] kline 콜백 연결 완료 -> {cb_name}")
        
        # 수신 카운터 (진단용)
        self._kline_receive_count = 0
        self._last_dm_receive_time: Optional[float] = None
        
        # 스레드 안전을 위한 락
        self.data_lock = threading.RLock()
        
        # 비동기 계산 워커 초기화 (콜백을 빠르게 반환하기 위함)
        self._stop_event = threading.Event()
        self._calc_queue: "queue.Queue[datetime]" = queue.Queue()
        self._calc_thread = threading.Thread(target=self._calc_worker, daemon=True)
        self._calc_thread.start()
        logger.info("비동기 계산 워커 시작")
        
        # 계정 동기화 관련
        self._last_account_sync = 0
        self._account_sync_interval = 30  # 30초마다 계정 동기화
        
        # 지표 값 저장 (0 값 처리용)
        self._last_valid_resistance: Optional[float] = None
        self._last_valid_support: Optional[float] = None
        
        # 시간 기반 지표 계산 스케줄러 시작
        self._start_indicator_scheduler()  # 1분봉
        self._start_30min_indicator_scheduler()  # 30분봉
        self._start_4h_indicator_scheduler()  # 4시간봉
        
        # 초기화 시점에 1분봉, 30분봉, 4시간봉 지표를 한 번 계산 (스케줄러가 경계를 기다리기 전에)
        self._calculate_initial_indicators()
        
        logger.info(f"비트겟 데이터 매니저 초기화 완료 - 심볼: {symbol}, 테스트넷: {testnet}")
    
    def initialize_with_historical_data(self, historical_df: pd.DataFrame):
        """
        과거 데이터로 초기화
        
        Args:
            historical_df: 과거 1분봉 데이터 (timestamp, open, high, low, close, volume)
        """
        with self.data_lock:
            logger.info(f"과거 데이터 초기화 시작 - 데이터 수: {len(historical_df)}")
            
            # 과거 데이터 설정
            self.minute_df = historical_df.copy()
            
            # 마지막 완성된 35분봉 시간 계산 (현재 시간 기준)
            now = datetime.now(timezone.utc)
            # 현재 시간에서 35분 단위로 마지막 완성된 봉 시간 계산
            minutes_since_midnight = now.hour * 60 + now.minute
            completed_35min_periods = minutes_since_midnight // 35
            last_completed_minutes = completed_35min_periods * 35
            
            # UTC 00:00부터 시작하여 마지막 완성된 35분봉 시간 계산
            midnight_today = now.replace(hour=0, minute=0, second=0, microsecond=0)
            self.last_35min_candle = midnight_today + timedelta(minutes=last_completed_minutes)
            
            # logger.info(f"마지막 완성된 35분봉 시간: {self.last_35min_candle}")
            
            # 과거 데이터로 35분봉 생성하지 않음 (사용자 요청에 따라)
            # logger.info("과거 데이터 초기화 완료 (35분봉 생성 안함)")
    
    def _on_kline_data(self, kline_data: Dict):
        """1분봉 데이터 수신 처리"""
        try:
            with self.data_lock:
                # 수신 확인 로그 (매 분 1회 기대)
                # try:
                #     logger.info(f"[DM] 수신: {kline_data['timestamp']} close={float(kline_data['close']):.2f}")
                # except Exception:
                #     logger.info(f"[DM] 수신: {kline_data}")
                self._kline_receive_count += 1
                # logger.debug(f"[DM] 수신 카운터: {self._kline_receive_count}")
                self._last_dm_receive_time = time.time()
                # 디버깅: 받은 데이터 구조 확인
                # logger.debug(f"받은 kline_data: {kline_data}")
                # logger.debug(f"timestamp 타입: {type(kline_data.get('timestamp'))}")
                
                # 새로운 1분봉 데이터 추가
                new_row = pd.DataFrame([{
                    'open': kline_data['open'],
                    'high': kline_data['high'],
                    'low': kline_data['low'],
                    'close': kline_data['close'],
                    'volume': kline_data['volume']
                }], index=[kline_data['timestamp']])
                
                # 기존 데이터에 추가
                self.minute_df = pd.concat([self.minute_df, new_row])
                
                # 중복 제거 (같은 timestamp가 있는 경우)
                self.minute_df = self.minute_df[~self.minute_df.index.duplicated(keep='last')]
                
                # 데이터 크기 관리 (메모리 최적화)
                self._manage_data_size()
                
                # logger.info(f"1분봉 데이터 추가: {kline_data['timestamp']} - Open: {kline_data['open']:.2f}, High: {kline_data['high']:.2f}, Low: {kline_data['low']:.2f}, Close: {kline_data['close']:.2f}, Volume: {kline_data['volume']:.2f}")
                
                
                
                
            # 지표 계산은 시간 기반으로만 트리거 (WebSocket 데이터 기반이 아님)
            # 완성된 봉의 정확한 지표 계산을 위해 REST API 사용
            # 매 분마다 완성된 봉의 지표를 계산하는 별도 스케줄러 사용
                
        except Exception as e:
            logger.error(f"1분봉 데이터 처리 오류: {e}")
            logger.error(f"kline_data 내용: {kline_data}")

    def _calc_worker(self):
        """비동기 지표 계산 워커: 큐에 쌓인 신호를 받아 계산 수행"""
        logger.info("비동기 계산 워커 시작됨")
        worker_start_time = time.time()
        processed_count = 0
        
        while True:
            try:
                timestamp = self._calc_queue.get(timeout=1.0)
                processed_count += 1
                
                # 워커 상태 로그 (10개마다)
                if processed_count % 10 == 0:
                    elapsed_time = time.time() - worker_start_time
                    logger.info(f"[워커] 처리된 신호: {processed_count}개, 경과시간: {elapsed_time:.1f}초")
                
                # logger.info(f"[워커] 신호 수신: {timestamp}")
                
            except queue.Empty:
                if self._stop_event.is_set():
                    logger.info("워커 종료 신호 수신 - 워커 중지")
                    break
                continue
            try:
                # logger.info("[워커] 지표 계산 시작")
                # 지표 계산 및 로그 출력
                self._calculate_and_log_indicators()
                # logger.info("[워커] 지표 계산 완료")
                
                # logger.info("[워커] 35분봉 확인 시작")
                # 35분봉 생성 확인
                self._check_and_notify_35min_candle()
                # logger.info("[워커] 35분봉 확인 완료")
                
            except Exception as calc_err:
                logger.error(f"비동기 계산 오류: {calc_err}")
                import traceback
                logger.error(f"워커 오류 상세: {traceback.format_exc()}")
            finally:
                try:
                    self._calc_queue.task_done()
                    # logger.info("[워커] 작업 완료 처리")
                except Exception as e:
                    logger.error(f"워커 작업 완료 처리 오류: {e}")
        
        logger.info(f"비동기 계산 워커 종료 - 총 처리: {processed_count}개")
    
    def _start_indicator_scheduler(self):
        """매 분마다 완성된 봉의 지표 계산"""
        def scheduler_worker():
            logger.info("시간 기반 지표 계산 스케줄러 시작")
            
            while not self._stop_event.is_set():
                try:
                    # 현재 로컬 시간
                    local_time = datetime.now()
                    current_second = local_time.second
                    
                    # 매 분 0초에 완성된 봉의 지표 계산
                    if current_second == 0:
                        # 완성된 봉 시간 계산 (로컬 시간 기준)
                        completed_candle_local = local_time.replace(second=0, microsecond=0) - timedelta(minutes=1)
                        completed_candle_utc = (completed_candle_local - timedelta(hours=9)).replace(tzinfo=timezone.utc)  # UTC 변환 + timezone 정보 추가
                        
                        # 중복 계산 방지
                        if not hasattr(self, '_last_scheduled_time') or self._last_scheduled_time != completed_candle_utc:
                            # 직접 지표 계산 (큐 없이)
                            self._calculate_completed_candle_indicators(completed_candle_utc)
                            self._last_scheduled_time = completed_candle_utc
                            # logger.info(f"[스케줄러] 완성된 봉 지표 계산: {completed_candle_utc} (로컬: {completed_candle_local})")
                            
                            # 1분봉 완성 콜백 호출 (진입 조건 체크용)
                            if self.candle_1min_callback:
                                try:
                                    # 1분봉 데이터 생성
                                    with self.data_lock:
                                        if not self.minute_df.empty and completed_candle_utc in self.minute_df.index:
                                            candle_row = self.minute_df.loc[completed_candle_utc]
                                            candle_data = {
                                                'timestamp': completed_candle_utc,
                                                'open': candle_row['open'],
                                                'high': candle_row['high'],
                                                'low': candle_row['low'],
                                                'close': candle_row['close'],
                                                'volume': candle_row['volume']
                                            }
                                            # logger.info(f"[1분봉 완성] {completed_candle_utc} - Close: {candle_data['close']:.2f}")
                                            self.candle_1min_callback(candle_data)
                                        else:
                                            logger.warning(f"[1분봉] 데이터 없음: {completed_candle_utc}")
                                except Exception as e:
                                    logger.error(f"1분봉 콜백 호출 오류: {e}")
                                    import traceback
                                    logger.error(f"상세 오류: {traceback.format_exc()}")
                    
                    # 1초 대기
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"스케줄러 워커 오류: {e}")
                    time.sleep(1)
            
            logger.info("시간 기반 지표 계산 스케줄러 종료")
        
        # 스케줄러 스레드 시작
        self._scheduler_thread = threading.Thread(target=scheduler_worker, daemon=True)
        self._scheduler_thread.start()
    
    def _start_30min_indicator_scheduler(self):
        """매 30분마다 완성된 30분봉의 지표 계산"""
        def scheduler_worker():
            logger.info("30분봉 지표 계산 스케줄러 시작")
            
            while not self._stop_event.is_set():
                try:
                    # 현재 UTC 시간
                    current_utc = datetime.now(timezone.utc)
                    current_minute = current_utc.minute
                    current_second = current_utc.second
                    
                    # 매 30분 경계(00분, 30분)에서 정확히 0초에 완성된 봉의 지표 계산
                    if (current_minute == 0 or current_minute == 30) and current_second == 0:
                        # 완성된 봉 시간 계산 (30분 전)
                        if current_minute == 0:
                            # 00분이면 이전 30분(이전 시간의 30분)
                            completed_candle_utc = current_utc.replace(minute=30, second=0, microsecond=0) - timedelta(hours=1)
                        else:
                            # 30분이면 이전 00분
                            completed_candle_utc = current_utc.replace(minute=0, second=0, microsecond=0)
                        
                        # 중복 계산 방지
                        if not hasattr(self, '_last_30min_scheduled_time') or self._last_30min_scheduled_time != completed_candle_utc:
                            # 30분봉 지표 계산
                            self._calculate_30min_candle_indicators(completed_candle_utc)
                            self._last_30min_scheduled_time = completed_candle_utc
                    
                    # 1초 대기
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"30분봉 스케줄러 워커 오류: {e}")
                    time.sleep(1)
            
            logger.info("30분봉 지표 계산 스케줄러 종료")
        
        # 스케줄러 스레드 시작
        self._scheduler_30min_thread = threading.Thread(target=scheduler_worker, daemon=True)
        self._scheduler_30min_thread.start()
    
    def _calculate_initial_indicators(self):
        """초기화 시점에 1분봉, 30분봉, 4시간봉 지표를 한 번 계산"""
        try:
            logger.info("[초기화] 지표 초기 계산 시작...")
            
            # 현재 UTC 시간
            current_utc = datetime.now(timezone.utc)
            
            # 1분봉: 가장 최근 완성된 1분봉 시간 계산 (1분 전)
            completed_1min_utc = current_utc.replace(second=0, microsecond=0) - timedelta(minutes=1)
            
            # 30분봉: 가장 최근 완성된 30분봉 시간 계산
            current_minute = current_utc.minute
            if current_minute < 30:
                # 00분 이전이면 이전 시간의 30분
                completed_30min_utc = current_utc.replace(minute=30, second=0, microsecond=0) - timedelta(hours=1)
            else:
                # 30분 이후면 현재 시간의 00분
                completed_30min_utc = current_utc.replace(minute=0, second=0, microsecond=0)
            
            # 4시간봉: 가장 최근 완성된 4시간봉 시간 계산
            current_hour = current_utc.hour
            completed_4h_hour = (current_hour // 4) * 4  # 현재 시간 이전의 4시간 경계
            completed_4h_utc = current_utc.replace(hour=completed_4h_hour, minute=0, second=0, microsecond=0) - timedelta(hours=4)
            
            logger.info(f"[초기화] 1분봉 계산 대상: {completed_1min_utc}")
            logger.info(f"[초기화] 30분봉 계산 대상: {completed_30min_utc}")
            logger.info(f"[초기화] 4시간봉 계산 대상: {completed_4h_utc}")
            
            # 1분봉 지표 계산
            self._calculate_completed_candle_indicators(completed_1min_utc)
            
            # 30분봉 지표 계산
            self._calculate_30min_candle_indicators(completed_30min_utc)
            
            # 4시간봉 지표 계산
            self._calculate_4h_candle_indicators(completed_4h_utc)
            
            logger.info("[초기화] 지표 초기 계산 완료")
            
        except Exception as e:
            logger.error(f"[초기화] 지표 초기 계산 오류: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _start_4h_indicator_scheduler(self):
        """매 4시간마다 완성된 4시간봉의 지표 계산"""
        def scheduler_worker():
            logger.info("4시간봉 지표 계산 스케줄러 시작")
            
            while not self._stop_event.is_set():
                try:
                    # 현재 UTC 시간
                    current_utc = datetime.now(timezone.utc)
                    current_hour = current_utc.hour
                    current_minute = current_utc.minute
                    current_second = current_utc.second
                    
                    # 매 4시간 경계(00:00, 04:00, 08:00, 12:00, 16:00, 20:00)에서 정확히 0초에 완성된 봉의 지표 계산
                    if (current_hour % 4 == 0 and current_minute == 0) and current_second == 0:
                        # 완성된 봉 시간 계산 (4시간 전)
                        # 예: 12:00:00이면 08:00:00 ~ 12:00:00 봉이 완성됨 (12:00:00에 완성)
                        # 하지만 EMA 계산에는 08:00:00 ~ 12:00:00 봉을 포함해야 함
                        # 따라서 completed_candle_utc는 12:00:00이 아니라 08:00:00이어야 함
                        completed_candle_utc = current_utc.replace(minute=0, second=0, microsecond=0) - timedelta(hours=4)
                        
                        logger.info(f"[4시간봉 스케줄러] 현재 시간: {current_utc}, 완성된 봉 시작 시간: {completed_candle_utc}")
                        
                        # 중복 계산 방지
                        if not hasattr(self, '_last_4h_scheduled_time') or self._last_4h_scheduled_time != completed_candle_utc:
                            # 4시간봉 지표 계산
                            self._calculate_4h_candle_indicators(completed_candle_utc)
                            self._last_4h_scheduled_time = completed_candle_utc
                    
                    # 1초 대기
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"4시간봉 스케줄러 워커 오류: {e}")
                    time.sleep(1)
            
            logger.info("4시간봉 지표 계산 스케줄러 종료")
        
        # 스케줄러 스레드 시작
        self._scheduler_4h_thread = threading.Thread(target=scheduler_worker, daemon=True)
        self._scheduler_4h_thread.start()
    
    def _calculate_30min_candle_indicators(self, completed_candle_time: datetime):
        """완성된 30분봉의 지표 계산 (REST API 데이터만 사용)"""
        try:
            # REST API로 30분봉 데이터 가져오기
            df_30min = self.realtime_client.fetch_historical_klines(
                symbol=self.symbol,
                interval='30m',
                limit=500  # 충분한 데이터 (약 10일치)
            )
            
            if df_30min is None or df_30min.empty:
                logger.warning(f"30분봉 데이터를 가져올 수 없음: {completed_candle_time}")
                return
            
            # 마지막 봉 제외 (진행 중인 봉일 수 있음)
            if len(df_30min) > 1:
                completed_df_30min = df_30min.iloc[:-1].copy()
            else:
                completed_df_30min = df_30min
            
            if len(completed_df_30min) < 35:
                logger.warning(f"[30분봉 지표] 완성된 봉 데이터 부족 (필요: 35개, 현재: {len(completed_df_30min)}개)")
                return
            
            # 데이터 저장
            self.df_30min = df_30min
            
            # 지표 계산
            indicators_30min = self.indicator_calculator.calculate_all_indicators(completed_df_30min)
            
            if not indicators_30min:
                logger.warning("[30분봉 지표] 지표 계산 결과가 없습니다.")
                return
            
            # ATR 30분봉
            if 'atr_14' in indicators_30min and not indicators_30min['atr_14'].empty:
                atr_30min = indicators_30min['atr_14'].iloc[-1]
                self._last_atr_30min = float(atr_30min)
                logger.info(f"[30분봉 지표] ATR(14): {atr_30min:.2f}")
            
            # 스퀴즈 모멘텀 30분봉
            if 'squeeze' in indicators_30min and isinstance(indicators_30min['squeeze'], dict):
                squeeze_state_30min = indicators_30min['squeeze']
                if 'val' in squeeze_state_30min:
                    squeeze_series_30min = squeeze_state_30min['val']
                    if not squeeze_series_30min.empty:
                        valid_squeeze_30min = squeeze_series_30min.dropna()
                        if not valid_squeeze_30min.empty:
                            squeeze_val_30min = valid_squeeze_30min.iloc[-1]
                            if len(valid_squeeze_30min) > 1:
                                squeeze_prev_30min = valid_squeeze_30min.iloc[-2]
                            else:
                                squeeze_prev_30min = np.nan
                            
                            # 캐시에 저장
                            self._last_squeeze_val_30min = float(squeeze_val_30min)
                            self._last_squeeze_prev_30min = float(squeeze_prev_30min) if not np.isnan(squeeze_prev_30min) else None
                            self._last_squeeze_state_30min = squeeze_state_30min
                            
                            # 스퀴즈 상태 확인
                            sqz_on = squeeze_state_30min.get('sqzOn', pd.Series()).iloc[-1] if isinstance(squeeze_state_30min.get('sqzOn'), pd.Series) and not squeeze_state_30min.get('sqzOn').empty else False
                            sqz_off = squeeze_state_30min.get('sqzOff', pd.Series()).iloc[-1] if isinstance(squeeze_state_30min.get('sqzOff'), pd.Series) and not squeeze_state_30min.get('sqzOff').empty else False
                            
                            if sqz_on:
                                squeeze_status = "SQUEEZE ON"
                            elif sqz_off:
                                squeeze_status = "SQUEEZE OFF"
                            else:
                                squeeze_status = "NO SQUEEZE"
                            
                            logger.info(f"[30분봉 지표] 스퀴즈 모멘텀: {squeeze_val_30min:.2f} (이전: {squeeze_prev_30min:.2f}) | 상태: {squeeze_status}")
            
            # 마지막 완성된 30분봉 종가 및 시간
            if not completed_df_30min.empty:
                last_candle_close_30min = float(completed_df_30min['close'].iloc[-1])
                self._last_close_30min = last_candle_close_30min
                # 봉의 시작 시간 저장 (같은 봉인지 확인용)
                self._last_completed_30min_candle_time = completed_candle_time
                logger.info(f"[30분봉 지표] 마지막 종가: {last_candle_close_30min:.2f} (봉 시간: {completed_candle_time})")
                
                # 30분봉 지표 계산 완료 후 콜백 호출 (진입 조건 확인용)
                if self.candle_30min_callback:
                    try:
                        self.candle_30min_callback(completed_candle_time)
                    except Exception as e:
                        logger.error(f"[30분봉 콜백] 오류: {e}")
                        import traceback
                        logger.error(traceback.format_exc())
            
            # 계산 시간 업데이트
            self._last_30min_calc_time = time.time()
            
        except Exception as e:
            logger.error(f"30분봉 지표 계산 오류: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _calculate_4h_candle_indicators(self, completed_candle_time: datetime):
        """완성된 4시간봉의 지표 계산 (REST API 데이터만 사용)"""
        try:
            # REST API로 4시간봉 데이터 가져오기
            # TradingView처럼 정확한 EMA 계산을 위해 최대한 많은 과거 데이터 사용
            # 여러 번 API 호출로 최대 3000개까지 가져오기 시도
            all_dfs = []
            end_time = None
            max_batches = 3  # 최대 3번 호출 (3000개)
            
            for batch_num in range(max_batches):
                try:
                    df_batch = self.realtime_client.fetch_historical_klines(
                        symbol=self.symbol,
                        interval='4H',
                        limit=1000,
                        end_time=end_time
                    )
                    
                    if df_batch is None or df_batch.empty:
                        break
                    
                    all_dfs.append(df_batch)
                    
                    # 다음 배치를 위한 endTime 설정
                    if len(df_batch) > 0:
                        first_time = df_batch.index[0]
                        end_time = first_time - timedelta(seconds=1)
                    else:
                        break
                    
                    # 이미 충분한 데이터를 가져왔으면 중단
                    if sum(len(df) for df in all_dfs) >= 3000:
                        break
                        
                except Exception as e:
                    logger.debug(f"[4시간봉 EMA] 배치 {batch_num + 1} 가져오기 실패: {e}")
                    break
            
            if not all_dfs:
                logger.warning(f"4시간봉 데이터를 가져올 수 없음: {completed_candle_time}")
                return
            
            # 모든 DataFrame 병합
            df_4h = pd.concat(all_dfs)
            df_4h = df_4h[~df_4h.index.duplicated(keep='last')]
            df_4h.sort_index(inplace=True)
            
            logger.info(f"[4시간봉 EMA] 총 {len(df_4h)}개 봉 데이터 가져옴 ({len(all_dfs)}번 API 호출)")
            
            if df_4h.empty:
                logger.warning(f"4시간봉 데이터를 가져올 수 없음: {completed_candle_time}")
                return
            
            # 마지막 봉 제외 (진행 중인 봉일 수 있음)
            if len(df_4h) > 1:
                completed_df_4h = df_4h.iloc[:-1].copy()
            else:
                completed_df_4h = df_4h
            
            if len(completed_df_4h) < 200:
                logger.warning(f"[4시간봉 지표] 완성된 봉 데이터 부족 (필요: 200개, 현재: {len(completed_df_4h)}개)")
                return
            
            # 데이터 저장
            self.df_4h = df_4h
            
            # TradingView 방식: 전체 과거 데이터를 사용해서 EMA 계산 (마지막 200개만 사용하지 않음)
            # EMA는 전체 과거 데이터로 계산하되, 마지막 완성봉의 EMA 값만 사용
            ema_df_4h = completed_df_4h.copy()  # 전체 데이터 사용
            
            # 데이터 개수 확인 및 경고
            if len(completed_df_4h) < 400:
                logger.warning(f"[4시간봉 EMA] 과거 데이터가 부족할 수 있습니다. (현재: {len(completed_df_4h)}개, 권장: 400개 이상)")
                logger.warning(f"[4시간봉 EMA] TradingView는 더 많은 과거 데이터를 사용하므로 값이 다를 수 있습니다.")
            
            # 디버깅: 사용하는 데이터 확인
            # logger.info(f"[4시간봉 EMA 디버깅] 전체 데이터: {len(df_4h)}개, 완성된 봉: {len(completed_df_4h)}개, EMA 계산용: {len(ema_df_4h)}개")
            # logger.info(f"[4시간봉 EMA 디버깅] 전체 데이터 마지막 봉: {df_4h.index[-1]} (종가: {df_4h['close'].iloc[-1]:.2f})")
            # logger.info(f"[4시간봉 EMA 디버깅] 완성된 봉 마지막: {completed_df_4h.index[-1]} (종가: {completed_df_4h['close'].iloc[-1]:.2f})")
            # logger.info(f"[4시간봉 EMA 디버깅] EMA 계산용 첫 번째 봉: {ema_df_4h.index[0]} (종가: {ema_df_4h['close'].iloc[0]:.2f})")
            # logger.info(f"[4시간봉 EMA 디버깅] EMA 계산용 마지막 봉: {ema_df_4h.index[-1]} (종가: {ema_df_4h['close'].iloc[-1]:.2f})")
            # logger.info(f"[4시간봉 EMA 디버깅] EMA 계산용 마지막 5개 봉:")
            for idx in ema_df_4h.index[-5:]:
                close_val = ema_df_4h.loc[idx, 'close']
                logger.info(f"  {idx}: {close_val:.2f}")
            
            # EMA 200 계산 (4시간봉) - indicators.py의 calculate_ema 사용 (talib 내부 사용)
            ema_4h_series = self.indicator_calculator.calculate_ema(ema_df_4h['close'], period=200)
            if not ema_4h_series.empty:
                ema_4h = float(ema_4h_series.iloc[-1])
                current_price_4h = float(ema_df_4h['close'].iloc[-1])
                
                # 디버깅: EMA 계산 결과 확인
                # logger.info(f"[4시간봉 EMA 디버깅] EMA(200) 계산 결과: {ema_4h:.2f}")
                # logger.info(f"[4시간봉 EMA 디버깅] EMA 시리즈 마지막 5개 값:")
                for idx in ema_4h_series.index[-5:]:
                    ema_val = ema_4h_series.loc[idx]
                    logger.info(f"  {idx}: {ema_val:.2f}")
                
                # 캐시에 저장
                self._last_ema_4h = ema_4h
                
                logger.info(f"[4시간봉 지표] EMA(200): {ema_4h:.2f} | 현재가: {current_price_4h:.2f} | 차이: {current_price_4h - ema_4h:.2f} ({((current_price_4h - ema_4h) / ema_4h * 100):.2f}%)")
            
            # 계산 시간 업데이트
            self._last_4h_calc_time = time.time()
            
        except Exception as e:
            logger.error(f"4시간봉 지표 계산 오류: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _calculate_completed_candle_indicators(self, completed_candle_time: datetime):
        """완성된 봉의 지표 계산 (REST API 데이터만 사용)"""
        try:
            # REST API로 완성된 봉 데이터 가져오기
            historical_data = self._fetch_completed_candle_data_smart(completed_candle_time)
            
            if historical_data is None or historical_data.empty:
                logger.warning(f"완성된 봉 데이터를 가져올 수 없음: {completed_candle_time}")
                return
            
            # 지표 계산
            indicators = self.indicator_calculator.calculate_all_indicators(historical_data)
            
            if not indicators:
                logger.warning("지표 계산 결과가 없습니다.")
                return
            
            # 마지막 1분봉 종가 저장 (진입 조건용)
            self._last_completed_candle_close = historical_data.iloc[-1]['close']
            self._last_completed_candle_time = historical_data.index[-1]
            
            # 마지막 완성 봉의 OHLC 저장
            self._last_candle_open = historical_data.iloc[-1]['open']
            self._last_candle_high = historical_data.iloc[-1]['high']
            self._last_candle_low = historical_data.iloc[-1]['low']
            
            # REST API 데이터를 minute_df에 병합 (WebSocket 데이터 보완)
            with self.data_lock:
                # 기존 minute_df와 병합 (중복 제거)
                self.minute_df = pd.concat([self.minute_df, historical_data])
                self.minute_df = self.minute_df[~self.minute_df.index.duplicated(keep='last')]
                self.minute_df.sort_index(inplace=True)
                # logger.debug(f"[REST API] minute_df 업데이트: {len(historical_data)}개 추가, 총 {len(self.minute_df)}개")
            
            # 지표 값 추출 및 로그 출력
            self._log_indicator_results(indicators, historical_data)
            
        except Exception as e:
            logger.error(f"완성된 봉 지표 계산 오류: {e}")
            import traceback
            logger.error(f"상세 오류: {traceback.format_exc()}")
    
    def _log_indicator_results(self, indicators: dict, data_df: pd.DataFrame):
        """지표 결과 로그 출력"""
        try:
            # # 지표 키 확인
            # logger.info(f"지표 계산 결과 키들: {list(indicators.keys())}")
            
            # # 데이터 정보 확인
            # logger.info(f"입력 데이터 길이: {len(data_df)}")
            # logger.info(f"최근 3개 봉 OHLC:")
            
            # 최근 3개 봉 출력 (중복 제거 없이)
            final_data = data_df.tail(3)  # 최근 3개 봉
            
            # for i, (idx, row) in enumerate(final_data.iterrows()):
            #     logger.info(f"  {i+1}. {idx} - O:{row['open']:.2f} H:{row['high']:.2f} L:{row['low']:.2f} C:{row['close']:.2f}")
            
            # ATR 값 추출
            atr_14_val = np.nan
            if 'atr_14' in indicators and not indicators['atr_14'].empty:
                atr_14_val = indicators['atr_14'].iloc[-1]
                # logger.info(f"[ATR] 계산 결과: {atr_14_val}, 시리즈 길이: {len(indicators['atr_14'])}")
                # logger.info(f"[ATR] 마지막 5개 값: {indicators['atr_14'].tail(5).tolist()}")
            else:
                logger.warning("[ATR] atr_14 데이터가 없거나 비어있음")
            
            # Squeeze 값 추출
            squeeze_val = np.nan
            if 'squeeze' in indicators:
                squeeze_data = indicators['squeeze']
                # logger.info(f"[Squeeze] squeeze 데이터 타입: {type(squeeze_data)}")
                if isinstance(squeeze_data, dict) and 'val' in squeeze_data:
                    squeeze_series = squeeze_data['val']
                    if not squeeze_series.empty:
                        valid_squeeze = squeeze_series.dropna()
                        if not valid_squeeze.empty:
                            squeeze_val = valid_squeeze.iloc[-1]
                            # logger.info(f"[Squeeze] 계산 결과: {squeeze_val}, 유효 데이터 수: {len(valid_squeeze)}")
                        else:
                            logger.warning("[Squeeze] 유효한 squeeze 데이터가 없음")
                    else:
                        logger.warning("[Squeeze] squeeze 시리즈가 비어있음")
                else:
                    logger.warning("[Squeeze] squeeze 데이터 구조가 예상과 다름")
            else:
                logger.warning("[Squeeze] squeeze 키가 없음")
            
            # Support/Resistance 값 추출
            support_val = np.nan
            resistance_val = np.nan
            
            if 'support_resistance' in indicators:
                pivot_high_series = indicators['support_resistance']['pivot_high']
                if not pivot_high_series.empty:
                    # Pine Script의 fixnan() 로직 적용 - 이전 값으로 채움
                    filled_highs = pivot_high_series.ffill()
                    if not filled_highs.empty and not np.isnan(filled_highs.iloc[-1]):
                        resistance_val = filled_highs.iloc[-1]
                        # logger.info(f"[저항] 마지막 5개 값: {filled_highs.tail(5).tolist()}")
                        
                        # 저항 값이 마지막으로 변경된 시간 찾기
                        non_na_highs = pivot_high_series.dropna()
                        if not non_na_highs.empty:
                            last_pivot_time = non_na_highs.index[-1]
                            last_pivot_value = non_na_highs.iloc[-1]
                            # logger.info(f"[저항] 마지막 Pivot High: {last_pivot_value:.2f} (시간: {last_pivot_time})")
                
                pivot_low_series = indicators['support_resistance']['pivot_low']
                if not pivot_low_series.empty:
                    # Pine Script의 fixnan() 로직 적용 - 이전 값으로 채움 (저항과 동일)
                    filled_lows = pivot_low_series.ffill()
                    if not filled_lows.empty and not np.isnan(filled_lows.iloc[-1]):
                        support_val = filled_lows.iloc[-1]
                        # logger.info(f"[지지] 마지막 5개 값: {filled_lows.tail(5).tolist()}")
                        
                        # 최근 10개 봉의 저가 확인
                        recent_lows = data_df['low'].tail(10)
                        # logger.info(f"[지지] 최근 10개 봉의 저가: {recent_lows.tolist()}")
                        # logger.info(f"[지지] 최근 10개 봉 시간: {recent_lows.index.tolist()}")
                        
                        
                            
                        
            
            # 현재 가격 (마지막 봉의 종가)
            current_price = data_df.iloc[-1]['close']
            latest_time = data_df.index[-1]
            
            # 로그 출력 (1분봉 로그 주석처리)
            # atr_str = f"{atr_14_val:.2f}" if not np.isnan(atr_14_val) else "N/A"
            # squeeze_str = f"{squeeze_val:.2f}" if not np.isnan(squeeze_val) else "N/A"
            # resistance_str = f"{resistance_val:.2f}" if not np.isnan(resistance_val) else "N/A"
            # support_str = f"{support_val:.2f}" if not np.isnan(support_val) else "N/A"
            
            # logger.info("=" * 80)  # 1분봉 로그 주석처리
            # logger.info(f"[실시간 지표] 시간: {latest_time}")  # 1분봉 로그 주석처리
            # logger.info(f"   가격: {current_price:.2f} | ATR: {atr_str} | Squeeze: {squeeze_str}")  # 1분봉 로그 주석처리
            # logger.info(f"   저항: {resistance_str} | 지지: {support_str}")  # 1분봉 로그 주석처리
            # logger.info("=" * 80)  # 1분봉 로그 주석처리
            
        except Exception as e:
            logger.error(f"지표 결과 로그 출력 오류: {e}")

    def get_health(self) -> Dict[str, any]:
        """DataManager 수신/워커 상태 리포트"""
        try:
            worker_alive = self._calc_thread.is_alive() if hasattr(self, '_calc_thread') else False
            return {
                'kline_receive_count': self._kline_receive_count,
                'last_receive_age_sec': (time.time() - self._last_dm_receive_time) if self._last_dm_receive_time else None,
                'worker_alive': worker_alive,
            }
        except Exception as e:
            logger.error(f"헬스체크 오류: {e}")
            return {
                'kline_receive_count': self._kline_receive_count,
                'last_receive_age_sec': None,
                'worker_alive': False,
                'error': str(e)
            }

    def _convert_squeeze_state_to_safe_dict(self, squeeze_state: Dict) -> Dict:
        """
        squeeze_state dict 안의 Series 값들을 boolean으로 변환
        
        Args:
            squeeze_state: squeeze_state dict (sqzOn, sqzOff, noSqz 등이 Series일 수 있음)
            
        Returns:
            Dict: Series 값들이 boolean으로 변환된 dict
        """
        if not isinstance(squeeze_state, dict):
            return {}
        
        import pandas as pd
        
        safe_dict = {}
        for key, value in squeeze_state.items():
            if isinstance(value, pd.Series):
                # Series인 경우 마지막 값 추출 후 boolean으로 변환
                if not value.empty:
                    last_value = value.iloc[-1]
                    # numpy bool_ 타입도 처리
                    if hasattr(last_value, 'item'):
                        safe_dict[key] = bool(last_value.item())
                    else:
                        safe_dict[key] = bool(last_value)
                else:
                    safe_dict[key] = False
            else:
                # Series가 아닌 경우 그대로 사용
                safe_dict[key] = value
        
        return safe_dict
    
    def ensure_worker(self):
        """워커가 중단되었으면 재시작"""
        try:
            if not self._calc_thread.is_alive():
                self._calc_thread = threading.Thread(target=self._calc_worker, daemon=True)
                self._calc_thread.start()
                logger.warning("비동기 계산 워커 재시작")
        except Exception as e:
            logger.error(f"워커 재시작 오류: {e}")
    
    def _manage_data_size(self):
        """데이터 크기 관리 (메모리 최적화)"""
        try:
            if len(self.minute_df) > self.max_data_points:
                # 오래된 데이터 제거 (최신 데이터 유지)
                keep_count = self.max_data_points
                self.minute_df = self.minute_df.tail(keep_count)
                logger.debug(f"데이터 크기 관리: {len(self.minute_df)}개 유지")
            
            # 메모리 사용량 로그 (1000개마다)
            if len(self.minute_df) % 1000 == 0:
                memory_usage = self.minute_df.memory_usage(deep=True).sum() / 1024 / 1024  # MB
                # logger.info(f"메모리 사용량: {memory_usage:.2f}MB (데이터: {len(self.minute_df)}개)")
        except Exception as e:
            logger.error(f"데이터 크기 관리 오류: {e}")
    
   
    
    def _calculate_and_log_indicators(self):
        """지표 계산 및 로그 출력 (REST API로 완성된 봉 데이터 사용)"""
        try:
            # logger.info("=== 지표 계산 시작 ===")
            
            # 현재 시간 기준으로 완성된 봉 확인
            current_time = datetime.now(timezone.utc)
            current_minute = current_time.replace(second=0, microsecond=0)
            
            # 완성된 봉 시간 (현재 분 - 1분)
            completed_candle_time = current_minute - timedelta(minutes=1)
            
            # 중복 계산 방지: 같은 봉에 대해 이미 계산했으면 건너뛰기
            if hasattr(self, '_last_calculated_candle_time'):
                if self._last_calculated_candle_time == completed_candle_time:
                    logger.debug(f"지표 계산 건너뛰기 (같은 1분봉: {completed_candle_time})")
                    return
            
            # REST API로 완성된 봉의 정확한 데이터 가져오기 (중복 호출 방지)
            completed_df = self._fetch_completed_candle_data_smart(completed_candle_time)
            
            if completed_df is None or completed_df.empty:
                logger.warning(f"완성된 봉 데이터를 가져올 수 없음: {completed_candle_time}")
                return
            
            # 마지막 계산한 봉 시간 저장
            self._last_calculated_candle_time = completed_candle_time
            
            # logger.info(f"현재 시간: {current_time}")
            # logger.info(f"현재 분: {current_minute}")
            # logger.info(f"전체 데이터 수: {len(self.minute_df)}")
            # logger.info(f"완성된 데이터 수: {len(completed_df)}")
            
            if completed_df.empty:
                logger.warning("완성된 1분봉 데이터가 없어 지표 계산을 건너뜁니다.")
                return
            
            # 최적화: 마지막 계산한 1분봉 확인 (같은 봉이면 건너뛰기)
            if hasattr(self, '_last_calculated_minute'):
                latest_minute = completed_df.index.max()
                if latest_minute == self._last_calculated_minute:
                    logger.debug(f"지표 계산 건너뛰기 (같은 1분봉: {latest_minute})")
                    return
            
            # 지표 계산
            logger.info(f"[확인] 계산 데이터 수: {len(completed_df)}")
            logger.info(f"[확인] 최신 봉 시간: {completed_df.index[-1]}")
            
            # 데이터 정렬 확인
            if not completed_df.index.is_monotonic_increasing:
                logger.warning("데이터가 시간순으로 정렬되지 않음 - 정렬 중...")
                completed_df = completed_df.sort_index()
            
            # 데이터 연속성 확인 및 누락 데이터 복구
            time_diffs = completed_df.index.to_series().diff()
            expected_interval = pd.Timedelta(minutes=1)
            gaps = time_diffs[time_diffs > expected_interval * 1.5]  # 1.5분 이상 간격
            
            if not gaps.empty:
                logger.warning(f"데이터 누락 감지 - {len(gaps)}개 간격:")
                for gap_time, gap_duration in gaps.items():
                    logger.warning(f"  {gap_time} - 간격: {gap_duration}")
                
                # WebSocket 연결 상태 확인 및 재연결 트리거
                if hasattr(self, 'realtime_client') and self.realtime_client:
                    if not self.realtime_client.is_connected:
                        logger.warning("[RECONNECT] WebSocket 연결 끊어짐 감지 - 재연결 시도")
                        # 재연결은 _websocket_thread에서 자동으로 처리됨
                    else:
                        logger.warning("[CONNECTION] WebSocket 연결은 유지되지만 데이터 누락 발생")
                
                # 데이터 복구 시도
                logger.info("[RECOVERY] 누락된 데이터 복구 시도 중...")
                recovered_df = self.data_recovery.recover_missing_data(completed_df)
                
                if recovered_df is not None and len(recovered_df) > len(completed_df):
                    logger.info(f"[OK] 데이터 복구 성공: {len(recovered_df) - len(completed_df)}개 봉 추가")
                    completed_df = recovered_df
                    # 복구된 데이터로 minute_df 업데이트
                    self.minute_df = completed_df
                else:
                    logger.warning("[FAIL] 데이터 복구 실패 - 지표 계산을 건너뜁니다.")
                    return
            
            # 최근 3개 봉의 OHLC 확인
            logger.info(f"[확인] 최근 3개 봉 OHLC:")
            for i, (idx, row) in enumerate(completed_df.tail(3).iterrows()):
                logger.info(f"  {i+1}. {idx} - O:{row['open']:.2f} H:{row['high']:.2f} L:{row['low']:.2f} C:{row['close']:.2f}")
            
            logger.info("지표 계산기 호출 시작...")
            indicators = self.indicator_calculator.calculate_all_indicators(completed_df)
            logger.info("지표 계산기 호출 완료")
            
            # 마지막 계산한 1분봉 저장 (차트 데이터 기준)
            latest_minute = completed_df.index.max()
            self._last_calculated_minute = latest_minute
            
            if not indicators:
                logger.warning("지표 계산 결과가 없습니다.")
                return
            
            logger.info(f"지표 계산 결과 키들: {list(indicators.keys())}")
            
            # ATR 값 추출
            atr_14_val = np.nan
            if not indicators['atr_14'].empty:
                atr_14_val = indicators['atr_14'].iloc[-1]
                logger.info(f"[ATR] 계산 결과: {atr_14_val}")
                logger.info(f"[ATR] 시리즈 길이: {len(indicators['atr_14'])}")
                logger.info(f"[ATR] 마지막 5개 값: {indicators['atr_14'].tail(5).tolist()}")
            
            # Squeeze 값 추출
            squeeze_val = np.nan
            if 'squeeze' in indicators and 'val' in indicators['squeeze']:
                squeeze_series = indicators['squeeze']['val']
                if not squeeze_series.empty:
                    valid_squeeze = squeeze_series.dropna()
                    if not valid_squeeze.empty:
                        squeeze_val = valid_squeeze.iloc[-1]
                        logger.info(f"[Squeeze] 계산 결과: {squeeze_val}")
                        logger.info(f"[Squeeze] 시리즈 길이: {len(squeeze_series)}")
                        logger.info(f"[Squeeze] 마지막 5개 값: {squeeze_series.tail(5).tolist()}")
            
            # Support/Resistance 값 추출
            support_val = np.nan
            resistance_val = np.nan
            
            if 'support_resistance' in indicators:
                pivot_high_series = indicators['support_resistance']['pivot_high']
                if not pivot_high_series.empty:
                    valid_highs = pivot_high_series.dropna()
                    if not valid_highs.empty:
                        resistance_val = valid_highs.iloc[-1]
                        logger.info(f"[Resistance] 계산 결과: {resistance_val}")
                
                pivot_low_series = indicators['support_resistance']['pivot_low']
                if not pivot_low_series.empty:
                    valid_lows = pivot_low_series.dropna()
                    if not valid_lows.empty:
                        support_val = valid_lows.iloc[-1]
                        logger.info(f"[Support] 계산 결과: {support_val}")
            
            # 현재 가격
            current_price = self.get_current_price()
            logger.info(f"현재 가격: {current_price}")
            
            # 로그 출력 (1분봉 로그 주석처리)
            # atr_str = f"{atr_14_val:.2f}" if not np.isnan(atr_14_val) else "N/A"
            # squeeze_str = f"{squeeze_val:.2f}" if not np.isnan(squeeze_val) else "N/A"
            # resistance_str = f"{resistance_val:.2f}" if not np.isnan(resistance_val) else "N/A"
            # support_str = f"{support_val:.2f}" if not np.isnan(support_val) else "N/A"
            
            # 중요 정보만 로그 출력 (부하 감소) - 1분봉 로그 주석처리
            # logger.info("=" * 80)  # 1분봉 로그 주석처리
            # logger.info(f"[실시간 지표] 시간: {latest_minute}")  # 1분봉 로그 주석처리
            # logger.info(f"   가격: {current_price:.2f} | ATR: {atr_str} | Squeeze: {squeeze_str}")  # 1분봉 로그 주석처리
            # logger.info(f"   저항: {resistance_str} | 지지: {support_str}")  # 1분봉 로그 주석처리
            # logger.info("=" * 80)  # 1분봉 로그 주석처리
            
        except Exception as e:
            logger.error(f"지표 계산 및 로그 출력 오류: {e}")
            import traceback
            traceback.print_exc()
    
    def save_position_state(self, position_info: Dict[str, any]):
        """포지션 상태 저장 (WebSocket 재연결 시 복구용)"""
        try:
            self._position_state.update({
                'position_size': position_info.get('position_size', 0.0),
                'position_value': position_info.get('position_value', 0.0),
                'avg_price': position_info.get('avg_price', 0.0),
                'liquidation_price': position_info.get('liquidation_price'),
                'risk_level': position_info.get('risk_level', 'LOW')
            })
            logger.info(f"포지션 상태 저장: {self._position_state}")
        except Exception as e:
            logger.error(f"포지션 상태 저장 오류: {e}")
    
    def get_position_state(self) -> Dict[str, any]:
        """저장된 포지션 상태 반환"""
        return self._position_state.copy()
    
    def restore_position_state(self, risk_manager) -> bool:
        """포지션 상태 복구 (WebSocket 재연결 시)"""
        try:
            if self._position_state['position_size'] > 0:
                logger.info(f"포지션 상태 복구 시작: {self._position_state}")
                
                # Risk Manager에 포지션 상태 복구
                risk_manager.position_size = self._position_state['position_size']
                risk_manager.position_value = self._position_state['position_value']
                risk_manager.avg_calculator.position_size = self._position_state['position_size']
                risk_manager.avg_calculator.position_value = self._position_state['position_value']
                risk_manager.avg_calculator.avg_price = self._position_state['avg_price']
                risk_manager.liquidation_price = self._position_state['liquidation_price']
                risk_manager.risk_level = self._position_state['risk_level']
                
                logger.info("포지션 상태 복구 완료")
                return True
            else:
                logger.info("복구할 포지션이 없습니다.")
                return False
        except Exception as e:
            logger.error(f"포지션 상태 복구 오류: {e}")
            return False
    
    def sync_with_account(self, force_sync=False):
        """
        실제 계정과 동기화
        
        Args:
            force_sync: 강제 동기화 여부
        
        Returns:
            Dict[str, any] - 동기화 결과
        """
        if not self.enable_account_sync or not self.account_manager:
            return {'synced': False, 'message': '계정 동기화가 비활성화되어 있습니다.'}
        
        try:
            current_time = time.time()
            
            # 동기화 간격 체크
            if not force_sync and (current_time - self._last_account_sync) < self._account_sync_interval:
                return {'synced': False, 'message': '동기화 간격이 아직 지나지 않았습니다.'}
            
            # 계정 정보 가져오기
            account_summary = self.account_manager.get_position_summary()
            
            # 리스크 매니저와 동기화
            sync_result = self.risk_manager.sync_with_account_data(account_summary)
            
            # 동기화 시간 업데이트
            self._last_account_sync = current_time
            
            if sync_result['synced']:
                logger.info(f"계정 동기화 완료 - {sync_result['action']}")
            else:
                logger.warning(f"계정 동기화 실패: {sync_result.get('error', 'Unknown error')}")
            
            return sync_result
            
        except Exception as e:
            logger.error(f"계정 동기화 오류: {e}")
            return {'synced': False, 'error': str(e)}
    
    def get_real_time_risk_status(self, current_price: float):
        """
        실시간 리스크 상태 확인 (계정 동기화 포함)
        
        Args:
            current_price: 현재 가격
        
        Returns:
            Dict[str, any] - 리스크 상태
        """
        try:
            # 계정 동기화 (필요시)
            if self.enable_account_sync:
                self.sync_with_account()
            
            # 리스크 상태 확인
            risk_status = self.risk_manager.get_real_time_risk_status(current_price)
            
            return risk_status
            
        except Exception as e:
            logger.error(f"실시간 리스크 상태 확인 오류: {e}")
            return {
                'timestamp': datetime.now(timezone.utc),
                'error': str(e),
                'is_position_active': False
            }
    
    def _check_and_notify_35min_candle(self):
        """35분봉 완성 확인 및 알림"""
        try:
            logger.info("[35분봉] 35분봉 완성 확인 시작")
            
            if self.minute_df.empty:
                logger.warning("[35분봉] 1분봉 데이터가 비어있음")
                return
            
            # 현재 시간 기준으로 마지막 1분봉 시간
            latest_minute = self.minute_df.index.max()
            logger.info(f"[35분봉] 최신 1분봉 시간: {latest_minute}")
            
            # 35분봉 종료 시간 계산 (다음 35분 단위)
            minutes_since_midnight = latest_minute.hour * 60 + latest_minute.minute
            next_35min_period = ((minutes_since_midnight // 35) + 1) * 35
            
            # UTC 00:00부터 시작하여 다음 35분봉 종료 시간 계산
            midnight_today = latest_minute.replace(hour=0, minute=0, second=0, microsecond=0)
            candle_end = midnight_today + timedelta(minutes=next_35min_period)
            
            # 35분봉 시작 시간
            candle_start = candle_end - timedelta(minutes=35)
            
            logger.info(f"[35분봉] 계산된 봉 시간: {candle_start} ~ {candle_end}")
            
            # 새로운 35분봉인지 확인
            if self.last_35min_candle and candle_end <= self.last_35min_candle:
                logger.info(f"[35분봉] 이미 처리된 봉: {candle_end} <= {self.last_35min_candle}")
                return
            
            # 35분봉 종료 시간 확인 (무한 루프 제거)
            current_time = datetime.now(timezone.utc)
            if current_time < candle_end:
                # 아직 35분봉이 완성되지 않음
                logger.info(f"[35분봉] 아직 완성되지 않음 - 현재: {current_time}, 완성: {candle_end}")
                return
            
            logger.info(f"[35분봉] 완성 확인됨 - 현재: {current_time}, 완성: {candle_end}")
            
            # 35분봉 데이터 생성 (시작 분 포함, 종료 분 제외)
            used_data = self.minute_df[(self.minute_df.index >= candle_start) & (self.minute_df.index <= latest_minute)]
            
            logger.info(f"[35분봉] 사용된 데이터: {len(used_data)}개")
            
            if len(used_data) < 30:  # 최소 30개 1분봉 필요
                logger.warning(f"[35분봉] 데이터 부족: {len(used_data)}개 (최소 30개 필요)")
                return
            
            # OHLC 계산
            open_price = used_data.iloc[0]['open']
            high_price = used_data['high'].max()
            low_price = used_data['low'].min()
            close_price = used_data.iloc[-1]['close']
            volume = used_data['volume'].sum()
            
            # 35분봉 데이터
            candle_data = {
                'timestamp': candle_start,
                'end_time': candle_end,
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': volume
            }
            
            # 로그 출력
            logger.info(f"[35분봉 완성] {candle_start.strftime('%H:%M')} ~ {candle_end.strftime('%H:%M')} | "
                       f"O:{open_price:.2f} H:{high_price:.2f} L:{low_price:.2f} C:{close_price:.2f} | "
                       f"사용 데이터: {len(used_data)}개")
            
            # 마지막 완성된 35분봉 시간 업데이트
            self.last_35min_candle = candle_end
            
            # 전략에 알림
            if self.strategy_callback:
                logger.info("[35분봉] 전략 콜백 호출 시작")
                self.strategy_callback(candle_data)
                logger.info("[35분봉] 전략 콜백 호출 완료")
            else:
                logger.warning("[35분봉] 전략 콜백이 설정되지 않음")
                
        except Exception as e:
            logger.error(f"35분봉 확인 오류: {e}")
            import traceback
            logger.error(f"상세 오류: {traceback.format_exc()}")
    
    def get_indicators_for_strategy(self, force_calculation: bool = False, force_log: bool = False) -> Optional[Dict[str, any]]:
        """
        전략용 지표 데이터 가져오기 (기존 바이낸스 로직과 동일)
        
        Args:
            force_calculation: 강제 계산 여부
            force_log: 강제 로그 출력 여부 (초기화 시 사용)
            
        Returns:
            Dict: 지표 데이터 또는 None
        """
        try:
            with self.data_lock:
                if self.minute_df.empty:
                    logger.warning("1분봉 데이터가 없습니다.")
                    return None
                
                # logger.info(f"지표 계산 시작 - 데이터 수: {len(self.minute_df)}")  # 1분봉 로그 주석처리
                
                # 완성된 1분봉 데이터로 지표 계산 (바이낸스와 동일)
                current_time = datetime.now(timezone.utc)
                current_minute = current_time.replace(second=0, microsecond=0)
                
                # 완성된 봉만 필터링 (현재 분보다 이전의 봉)
                completed_df = self.minute_df[self.minute_df.index < current_minute]
                
                if completed_df.empty:
                    logger.warning("완성된 1분봉 데이터가 없습니다.")
                    return None
                
                # logger.info(f"완성된 봉으로 지표 계산 - 데이터 수: {len(completed_df)}")  # 1분봉 로그 주석처리
                indicators = self.indicator_calculator.calculate_all_indicators(completed_df)
                
                if not indicators:
                    logger.warning("지표 계산 결과가 없습니다.")
                    return None
                
                # logger.info("전략 지표 추출 시작...")  # 1분봉 로그 주석처리
                
                # ATR 값 추출 (기존 로직과 동일)
                if not indicators['atr_14'].empty:
                    atr_14_val = indicators['atr_14'].iloc[-1]
                    # logger.info(f"ATR 14 값: {atr_14_val} (총 {len(indicators['atr_14'])}개 중 마지막)")  # 1분봉 로그 주석처리
                else:
                    atr_14_val = np.nan
                    # logger.warning("ATR 14 데이터가 비어있음")  # 1분봉 로그 주석처리
                
                # RSI 값 추출
                rsi_1m_val = np.nan
                if 'rsi_14' in indicators and not indicators['rsi_14'].empty:
                    valid_rsi = indicators['rsi_14'].dropna()
                    if not valid_rsi.empty:
                        rsi_1m_val = valid_rsi.iloc[-1]
                        # logger.info(f"RSI 14 값: {rsi_1m_val:.2f} (총 {len(valid_rsi)}개 유효값 중 마지막)")  # 1분봉 로그 주석처리
                    else:
                        pass
                        # logger.warning("RSI 유효값이 없음")  # 1분봉 로그 주석처리
                else:
                    pass
                    # logger.warning("RSI 데이터가 비어있음")  # 1분봉 로그 주석처리
                
                # Squeeze 값 추출 (디버깅 추가)
                # logger.info(f"Indicators 키들: {list(indicators.keys())}")  # 1분봉 로그 주석처리
                if 'squeeze' in indicators:
                    # logger.info(f"Squeeze 타입: {type(indicators['squeeze'])}")  # 1분봉 로그 주석처리
                    if isinstance(indicators['squeeze'], dict):
                        # logger.info(f"Squeeze 키들: {list(indicators['squeeze'].keys())}")  # 1분봉 로그 주석처리
                        if 'val' in indicators['squeeze']:
                            squeeze_series = indicators['squeeze']['val']
                            # logger.info(f"Squeeze val 타입: {type(squeeze_series)}")  # 1분봉 로그 주석처리
                            # logger.info(f"Squeeze val 길이: {len(squeeze_series) if hasattr(squeeze_series, '__len__') else 'N/A'}")  # 1분봉 로그 주석처리
                            # logger.info(f"Squeeze val 비어있음: {squeeze_series.empty if hasattr(squeeze_series, 'empty') else 'N/A'}")  # 1분봉 로그 주석처리
                            
                            if not squeeze_series.empty:
                                # NaN이 아닌 유효값 찾기
                                valid_squeeze = squeeze_series.dropna()
                                if not valid_squeeze.empty:
                                    squeeze_val = valid_squeeze.iloc[-1]
                                    # logger.info(f"Squeeze 값: {squeeze_val} (총 {len(valid_squeeze)}개 유효값 중 마지막)")  # 1분봉 로그 주석처리
                                else:
                                    squeeze_val = np.nan
                                    # logger.warning("Squeeze 유효값이 없음")  # 1분봉 로그 주석처리
                            else:
                                squeeze_val = np.nan
                                # logger.warning("Squeeze 데이터가 비어있음")  # 1분봉 로그 주석처리
                        else:
                            squeeze_val = np.nan
                            # logger.warning("Squeeze 'val' 키가 없음")  # 1분봉 로그 주석처리
                    else:
                        squeeze_val = np.nan
                        # logger.warning("Squeeze가 dict가 아님")  # 1분봉 로그 주석처리
                else:
                    squeeze_val = np.nan
                    # logger.warning("Squeeze 지표가 없음")  # 1분봉 로그 주석처리
                
                # Support/Resistance 값 추출 (마지막 유효값 사용, 0 방지)
                pivot_high_series = indicators['support_resistance']['pivot_high']
                if not pivot_high_series.empty:
                    # 마지막 유효값 찾기 (NaN이 아닌 값)
                    valid_highs = pivot_high_series.dropna()
                    if not valid_highs.empty:
                        resistance_val = valid_highs.iloc[-1]
                        # logger.info(f"Resistance 값: {resistance_val} (총 {len(valid_highs)}개 유효값 중 마지막)")  # 1분봉 로그 주석처리
                    else:
                        resistance_val = np.nan
                        # logger.warning("Resistance 유효값이 없음")  # 1분봉 로그 주석처리
                else:
                    resistance_val = np.nan
                    # logger.warning("Resistance 데이터가 비어있음")  # 1분봉 로그 주석처리
                
                pivot_low_series = indicators['support_resistance']['pivot_low']
                if not pivot_low_series.empty:
                    # 마지막 유효값 찾기 (NaN이 아닌 값)
                    valid_lows = pivot_low_series.dropna()
                    if not valid_lows.empty:
                        support_val = valid_lows.iloc[-1]
                        # logger.info(f"Support 값: {support_val} (총 {len(valid_lows)}개 유효값 중 마지막)")  # 1분봉 로그 주석처리
                    else:
                        support_val = np.nan
                        # logger.warning("Support 유효값이 없음")  # 1분봉 로그 주석처리
                else:
                    support_val = np.nan
                    # logger.warning("Support 데이터가 비어있음")  # 1분봉 로그 주석처리
                
                # 0 값 처리 (기존 로직과 동일)
                # pandas Series인 경우 마지막 값 추출
                if hasattr(resistance_val, 'iloc'):
                    resistance_val = resistance_val.iloc[-1] if not resistance_val.empty else np.nan
                if hasattr(support_val, 'iloc'):
                    support_val = support_val.iloc[-1] if not support_val.empty else np.nan
                
                # NaN 체크를 안전하게 수행
                resistance_is_nan = pd.isna(resistance_val) if hasattr(pd, 'isna') else (resistance_val != resistance_val)
                support_is_nan = pd.isna(support_val) if hasattr(pd, 'isna') else (support_val != support_val)
                
                if resistance_val == 0 or resistance_is_nan:
                    if self._last_valid_resistance is not None:
                        resistance_val = self._last_valid_resistance
                        logger.debug(f"저항선 0 값 처리 - 이전 값 사용: {self._last_valid_resistance}")
                    else:
                        resistance_val = np.nan
                else:
                    self._last_valid_resistance = resistance_val
                
                # 지지선 값 처리
                if support_val == 0 or support_is_nan:
                    if self._last_valid_support is not None:
                        support_val = self._last_valid_support
                        logger.debug(f"지지선 0 값 처리 - 이전 값 사용: {self._last_valid_support}")
                    else:
                        support_val = np.nan
                        logger.warning("지지선 0 값이고 이전 유효값이 없음")
                else:
                    # 유효한 값이면 저장
                    self._last_valid_support = support_val
                    # logger.debug(f"지지선 유효값 저장: {support_val:.2f}")
                
                # REST API로 계산한 마지막 종가 포함
                last_close = self._last_completed_candle_close
                last_time = self._last_completed_candle_time
                
                # ===== 30분봉 지표 (캐시된 값만 사용) =====
                # 스케줄러에서 계산된 캐시된 값 사용
                atr_30min = self._last_atr_30min if self._last_atr_30min is not None else np.nan
                squeeze_val_30min = self._last_squeeze_val_30min if self._last_squeeze_val_30min is not None else np.nan
                squeeze_prev_30min = self._last_squeeze_prev_30min if self._last_squeeze_prev_30min is not None else np.nan
                squeeze_state_30min = self._last_squeeze_state_30min if self._last_squeeze_state_30min is not None else {}
                last_candle_close_30min = self._last_close_30min
                
                # ===== 4시간봉 지표 (최신 데이터로 실시간 계산) =====
                # 진입 조건에 사용할 최신 EMA 값을 실시간으로 계산하여 _last_ema_4h에 저장
                ema_4h = None
                current_price_4h = None
                last_completed_candle_time = None
                
                try:
                    # TradingView처럼 정확한 EMA 계산을 위해 최대한 많은 과거 데이터 사용
                    # 여러 번 API 호출로 최대 3000개까지 가져오기 시도
                    all_dfs_current = []
                    end_time_current = None
                    max_batches_current = 3  # 최대 3번 호출
                    
                    for batch_num in range(max_batches_current):
                        try:
                            df_batch = self.realtime_client.fetch_historical_klines(
                                symbol=self.symbol,
                                interval='4H',
                                limit=1000,
                                end_time=end_time_current
                            )
                            
                            if df_batch is None or df_batch.empty:
                                break
                            
                            all_dfs_current.append(df_batch)
                            
                            if len(df_batch) > 0:
                                first_time = df_batch.index[0]
                                end_time_current = first_time - timedelta(seconds=1)
                            else:
                                break
                            
                            if sum(len(df) for df in all_dfs_current) >= 3000:
                                break
                                
                        except Exception as e:
                            break
                    
                    if all_dfs_current:
                        df_4h_current = pd.concat(all_dfs_current)
                        df_4h_current = df_4h_current[~df_4h_current.index.duplicated(keep='last')]
                        df_4h_current.sort_index(inplace=True)
                    else:
                        df_4h_current = None
                    
                    if df_4h_current is not None and not df_4h_current.empty and len(df_4h_current) >= 200:
                        # 마지막 봉 제외 (진행 중인 봉일 수 있음)
                        if len(df_4h_current) > 1:
                            completed_df_4h_current = df_4h_current.iloc[:-1].copy()
                        else:
                            completed_df_4h_current = df_4h_current
                        
                        if len(completed_df_4h_current) >= 200:
                            # TradingView 방식: 전체 과거 데이터를 사용해서 EMA 계산
                            ema_df_4h_current = completed_df_4h_current.copy()  # 전체 데이터 사용
                            
                            # EMA 계산 (95149.46을 계산한 것과 동일한 방식)
                            ema_4h_series_current = self.indicator_calculator.calculate_ema(ema_df_4h_current['close'], period=200)
                            if not ema_4h_series_current.empty:
                                ema_4h = float(ema_4h_series_current.iloc[-1])
                                current_price_4h = float(ema_df_4h_current['close'].iloc[-1])
                                last_completed_candle_time = ema_df_4h_current.index[-1]
                                
                                # 계산된 EMA 값을 _last_ema_4h에 저장 (진입 조건에 사용됨)
                                self._last_ema_4h = ema_4h
                                
                                # 데이터 저장 (다음 호출 시 재사용 가능)
                                self.df_4h = df_4h_current
                                
                                logger.info(f"[4시간봉 EMA 현재값] EMA(200): {ema_4h:.2f} | 최신 완성봉 종가: {current_price_4h:.2f} | 차이: {current_price_4h - ema_4h:.2f} ({((current_price_4h - ema_4h) / ema_4h * 100):.2f}%) | 최신 완성봉 시간: {last_completed_candle_time}")
                except Exception as e:
                    # 계산 실패 시 캐시된 값 사용
                    ema_4h = self._last_ema_4h if self._last_ema_4h is not None else np.nan
                    if ema_4h is not None and not np.isnan(ema_4h):
                        logger.warning(f"[4시간봉 EMA] 실시간 계산 실패, 캐시된 값 사용: {ema_4h:.2f} (오류: {e})")
                    else:
                        logger.error(f"[4시간봉 EMA] 계산 실패 및 캐시된 값 없음: {e}")
                
                # 계산 실패한 경우 NaN 처리
                if ema_4h is None:
                    ema_4h = np.nan
                
                # 결과 반환
                result = {
                    'atr': atr_14_val,
                    'rsi_1m': rsi_1m_val,
                    'squeeze_momentum': squeeze_val,
                    'resistance': resistance_val,
                    'support': support_val,
                    'last_candle_close': last_close,  # REST API 1분봉 종가
                    'last_candle_time': last_time,     # 해당 시간
                    'last_candle_open': getattr(self, '_last_candle_open', None),   # 마지막 봉 시가
                    'last_candle_high': getattr(self, '_last_candle_high', None),   # 마지막 봉 고가
                    'last_candle_low': getattr(self, '_last_candle_low', None),     # 마지막 봉 저가
                    # 30분봉 지표 (스퀴즈 모멘텀 전략용)
                    'atr_30min': atr_30min,
                    'squeeze_val': squeeze_val_30min,
                    'squeeze_prev': squeeze_prev_30min,
                    'squeeze': self._convert_squeeze_state_to_safe_dict(squeeze_state_30min),
                    # 4시간봉 지표
                    'ema_4h': ema_4h,
                }
                
                # 30분봉 종가가 있으면 last_candle_close를 30분봉 종가로 설정 (스퀴즈 모멘텀 전략용)
                # 단, 30분봉 종가가 없으면 기존 1분봉 종가 유지
                if last_candle_close_30min is not None:
                    result['last_candle_close'] = last_candle_close_30min
                    # 30분봉 시간 정보도 함께 제공 (같은 봉인지 확인용)
                    if self._last_completed_30min_candle_time is not None:
                        result['last_candle_time_30min'] = self._last_completed_30min_candle_time
                
                return result
                
        except Exception as e:
            logger.error(f"지표 계산 오류: {e}")
            import traceback
            logger.error(f"상세 오류: {traceback.format_exc()}")
            return None
    
    def _resample_to_35min_dataframe(self) -> pd.DataFrame:
        """
        1분봉 데이터를 35분봉으로 리샘플링 (바이낸스와 동일한 로직)
        
        Returns:
            pd.DataFrame: 35분봉 데이터
        """
        try:
            if self.minute_df.empty:
                return pd.DataFrame()
            
            # 35분봉 리샘플링 로직 (바이낸스와 동일)
            resampled_data = []
            current_35min_start = None
            current_35min_data = []
            
            for timestamp, row in self.minute_df.iterrows():
                # UTC 00:00 기준으로 35분봉 시작 시간 계산
                if current_35min_start is None:
                    current_35min_start = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
                
                # 35분봉 범위 확인
                next_35min = current_35min_start + timedelta(minutes=35)
                
                if timestamp < next_35min:
                    current_35min_data.append({
                        'timestamp': timestamp,
                        'open': row['open'],
                        'high': row['high'],
                        'low': row['low'],
                        'close': row['close'],
                        'volume': row['volume']
                    })
                else:
                    # 35분봉 완성
                    if current_35min_data:
                        resampled_data.append(self._create_35min_candle(current_35min_data))
                    
                    # 다음 35분봉 시작
                    current_35min_start = next_35min
                    current_35min_data = [{
                        'timestamp': timestamp,
                        'open': row['open'],
                        'high': row['high'],
                        'low': row['low'],
                        'close': row['close'],
                        'volume': row['volume']
                    }]
            
            # 마지막 35분봉 처리
            if current_35min_data:
                resampled_data.append(self._create_35min_candle(current_35min_data))
            
            if not resampled_data:
                return pd.DataFrame()
            
            # DataFrame 생성
            df = pd.DataFrame(resampled_data)
            df.set_index('timestamp', inplace=True)
            df.sort_index(inplace=True)
            
            logger.debug(f"35분봉 리샘플링 완료: {len(df)}개")
            return df
            
        except Exception as e:
            logger.error(f"35분봉 리샘플링 오류: {e}")
            return pd.DataFrame()
    
    def _resample_to_35min_dataframe(self) -> pd.DataFrame:
        """
        1분봉 데이터를 35분봉으로 리샘플링 (바이낸스와 동일한 로직)
        
        Returns:
            pd.DataFrame: 35분봉 데이터
        """
        try:
            if self.minute_df.empty:
                return pd.DataFrame()
            
            # 35분봉 리샘플링 로직 (바이낸스와 동일)
            resampled_data = []
            current_35min_start = None
            current_35min_data = []
            
            for timestamp, row in self.minute_df.iterrows():
                # UTC 00:00 기준으로 35분봉 시작 시간 계산
                if current_35min_start is None:
                    current_35min_start = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
                
                # 35분봉 범위 확인
                next_35min = current_35min_start + timedelta(minutes=35)
                
                if timestamp < next_35min:
                    current_35min_data.append({
                        'timestamp': timestamp,
                        'open': row['open'],
                        'high': row['high'],
                        'low': row['low'],
                        'close': row['close'],
                        'volume': row['volume']
                    })
                else:
                    # 35분봉 완성
                    if current_35min_data:
                        resampled_data.append(self._create_35min_candle(current_35min_data))
                    
                    # 다음 35분봉 시작
                    current_35min_start = next_35min
                    current_35min_data = [{
                        'timestamp': timestamp,
                        'open': row['open'],
                        'high': row['high'],
                        'low': row['low'],
                        'close': row['close'],
                        'volume': row['volume']
                    }]
            
            # 마지막 35분봉 처리
            if current_35min_data:
                resampled_data.append(self._create_35min_candle(current_35min_data))
            
            if not resampled_data:
                return pd.DataFrame()
            
            # DataFrame 생성
            df = pd.DataFrame(resampled_data)
            df.set_index('timestamp', inplace=True)
            df.sort_index(inplace=True)
            
            logger.debug(f"35분봉 리샘플링 완료: {len(df)}개")
            return df
            
        except Exception as e:
            logger.error(f"35분봉 리샘플링 오류: {e}")
            return pd.DataFrame()
    
    def _create_35min_candle(self, minute_data: List[Dict]) -> Dict:
        """
        35분봉 생성 (바이낸스와 동일한 로직)
        
        Args:
            minute_data: 1분봉 데이터 리스트
            
        Returns:
            Dict: 35분봉 데이터
        """
        if not minute_data:
            return {}
        
        # 마지막 1분봉 제외 (바이낸스와 동일)
        if len(minute_data) > 1:
            minute_data = minute_data[:-1]
        
        if not minute_data:
            return {}
        
        # OHLC 계산
        open_price = minute_data[0]['open']
        close_price = minute_data[-1]['close']
        high_price = max(row['high'] for row in minute_data)
        low_price = min(row['low'] for row in minute_data)
        volume = sum(row['volume'] for row in minute_data)
        
        return {
            'timestamp': minute_data[0]['timestamp'],
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price,
            'volume': volume
        }
    
    def start(self):
        """실시간 데이터 수신 시작"""
        try:
            logger.info("비트겟 실시간 데이터 수신 시작")
            self.realtime_client.start()
            logger.info("비트겟 실시간 데이터 수신 시작 완료")
            
        except Exception as e:
            logger.error(f"실시간 데이터 수신 시작 오류: {e}")
            raise
    
    def stop(self):
        """실시간 데이터 수신 중지"""
        try:
            logger.info("비트겟 실시간 데이터 수신 중지")
            self.realtime_client.stop()
            logger.info("비트겟 실시간 데이터 수신 중지 완료")
            
        except Exception as e:
            logger.error(f"실시간 데이터 수신 중지 오류: {e}")
    
    def is_running(self):
        """실행 상태 확인"""
        return self.realtime_client.is_running()
    
    def get_current_price(self) -> Optional[float]:
        """완성된 1분봉의 종가 가져오기 (바이낸스와 동일)"""
        try:
            with self.data_lock:
                if self.minute_df.empty:
                    return None
                
                # 현재 시간 기준으로 완성된 봉 확인
                current_time = datetime.now(timezone.utc)
                current_minute = current_time.replace(second=0, microsecond=0)
                
                # 완성된 봉만 필터링 (현재 분보다 이전의 봉)
                completed_candles = self.minute_df[self.minute_df.index < current_minute]
                
                if completed_candles.empty:
                    logger.warning("완성된 1분봉이 없습니다.")
                    return None
                
                # 가장 최근 완성된 봉의 종가
                latest_completed_price = completed_candles.iloc[-1]['close']
                latest_completed_time = completed_candles.index[-1]
                
                # logger.debug(f"완성된 봉 종가: {latest_completed_price:.2f} (시간: {latest_completed_time})")
                return latest_completed_price
                
        except Exception as e:
            logger.error(f"완성된 봉 가격 가져오기 오류: {e}")
            return None
    
    def _fetch_completed_candle_data_smart(self, completed_time: datetime) -> Optional[pd.DataFrame]:
        """
        스마트한 완성된 봉 데이터 가져오기 (완성된 봉까지만 사용)
        
        Args:
            completed_time: 완성된 봉 시간 (예: 12:09분)
            
        Returns:
            pd.DataFrame: 완성된 봉을 포함한 최근 데이터 (지표 계산용)
        """
        try:
            # 캐시된 데이터 확인 (같은 분이면 재사용) - 임시로 비활성화하여 항상 최신 데이터 가져오기
            # if hasattr(self, '_cached_data_time') and hasattr(self, '_cached_data'):
            #     if self._cached_data_time == completed_time and self._cached_data is not None:
            #         logger.info(f"[캐시] 기존 데이터 재사용: {completed_time}")
            #         return self._cached_data
            
            # 비트겟 클라이언트에서 REST API 사용
            if hasattr(self, 'realtime_client') and self.realtime_client:
                # 최근 1000개 1분봉 데이터 가져오기 (프로그램 시작 시 정확한 지지/저항 값 계산을 위해)
                historical_data = self.realtime_client.fetch_historical_klines(
                    symbol=self.symbol,
                    interval='1m',
                    limit=1000
                )
                
                if historical_data is not None and not historical_data.empty:
                    # logger.info(f"[REST API] 원본 데이터 가져오기 성공: {len(historical_data)}개")
                    
                    # 중복 데이터 확인
                    duplicate_count = len(historical_data) - len(historical_data.drop_duplicates(subset=['open', 'high', 'low', 'close']))
                    if duplicate_count > 0:
                        # logger.warning(f"[REST API] 중복 데이터 발견: {duplicate_count}개")
                        # 최근 10개 봉의 중복 확인
                        recent_10 = historical_data.tail(10)
                        # for i, (idx, row) in enumerate(recent_10.iterrows()):
                        #     logger.info(f"  {i+1}. {idx} - O:{row['open']:.2f} H:{row['high']:.2f} L:{row['low']:.2f} C:{row['close']:.2f}")
                    
                    # 완성된 봉까지만 포함 (completed_time 기준으로 필터링)
                    # completed_time이 06:02라면, 06:02까지만 포함 (06:03 진행 중이므로)
                    filter_time = completed_time
                    
                    # logger.info(f"[필터링] 완성된 봉 시간: {completed_time}")
                    # logger.info(f"[필터링] 원본 데이터 시간 범위: {historical_data.index.min()} ~ {historical_data.index.max()}")
                    # logger.info(f"[필터링] 원본 데이터 최신 3개 시간: {historical_data.index[-3:].tolist()}")
                    
                    # completed_time까지만 포함 (그 이후는 미완성 봉)
                    completed_data = historical_data[historical_data.index <= filter_time]
                    
                    # logger.info(f"[필터링] 필터링 후 최신 시간: {completed_data.index.max() if not completed_data.empty else 'N/A'}")
                    
                    if completed_data.empty:
                        logger.warning("[REST API] 완성된 봉 데이터가 없음")
                        return None
                    
                    # logger.info(f"[REST API] 완성된 봉 데이터 필터링: {len(historical_data)}개 → {len(completed_data)}개")
                    # logger.info(f"[REST API] 데이터 시간 범위: {completed_data.index.min()} ~ {completed_data.index.max()}")
                    
                    # 데이터 캐시
                    self._cached_data_time = completed_time
                    self._cached_data = completed_data
                    
                    return completed_data
                else:
                    logger.warning("[REST API] 완성된 봉 데이터가 비어있음")
                    return None
            else:
                logger.warning("[REST API] realtime_client가 없음")
                return None
                
        except Exception as e:
            logger.error(f"[REST API] 완성된 봉 데이터 가져오기 실패: {e}")
            return None
    
    def _fetch_completed_candle_data(self, completed_time: datetime) -> Optional[pd.DataFrame]:
        """
        REST API를 사용하여 완성된 봉의 정확한 데이터 가져오기 (레거시)
        
        Args:
            completed_time: 완성된 봉 시간 (예: 12:09분)
            
        Returns:
            pd.DataFrame: 완성된 봉을 포함한 최근 데이터 (지표 계산용)
        """
        return self._fetch_completed_candle_data_smart(completed_time)
 