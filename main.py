"""
비트겟 실시간 자동매매 시스템 메인 실행 파일

비트겟 비트코인 현물 실시간 자동매매 시스템을 실행합니다.
"""

import pandas as pd
import numpy as np
import logging
import time
import signal
import sys
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict
import os
import argparse

# 상위 디렉토리의 모듈 import를 위한 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_manager import BitgetDataManager
from squeeze_momentum_strategy_partial_close_real_trading import SqueezeMomentumStrategyPartialCloseRealTrading
from bitget_account import BitgetAccountManager

# 비트겟 데이터 페처 import (상대 경로 사용)
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))


# 로거 설정 (한국 시간으로 통일)
import logging
from datetime import datetime, timezone, timedelta

class KSTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        # UTC 시간을 한국 시간(KST)으로 변환
        utc_time = datetime.fromtimestamp(record.created, tz=timezone.utc)
        kst_time = utc_time + timedelta(hours=9)
        if datefmt:
            s = kst_time.strftime(datefmt)
        else:
            s = kst_time.strftime('%Y-%m-%d %H:%M:%S')
        return s

# 모든 기존 로거 핸들러 제거
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# 루트 로거 설정
logging.basicConfig(level=logging.DEBUG, force=True)

# 포맷터 생성 (한국 시간)
formatter = KSTFormatter('[%(asctime)s][%(levelname)s][%(name)s] %(message)s')

# 루트 로거에 핸들러 추가
root_logger = logging.getLogger()
root_logger.handlers.clear()

# 콘솔 핸들러
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
root_logger.addHandler(console_handler)

# 파일 핸들러
file_handler = logging.FileHandler('bitget_realtime_trading.log')
file_handler.setFormatter(formatter)
root_logger.addHandler(file_handler)

logger = logging.getLogger(__name__)

class BitgetRealtimeTradingSystem:
    """
    비트겟 실시간 데이터 수신 및 지표 계산 시스템
    
    실시간 데이터 수신과 지표 계산만 수행합니다.
    """
    
    def __init__(self, symbol: str = 'BTCUSDT', testnet: bool = True, strategy_type: str = 'squeeze_momentum'):
        """
        초기화
        
        Args:
            symbol: 거래 심볼
            testnet: 테스트넷 사용 여부
            strategy_type: 전략 타입 ('simple_test', 'squeeze_momentum', 'support_resistance' 또는 'martingale')
        """
        self.symbol = symbol
        self.testnet = testnet
        self.strategy_type = strategy_type
        self.data_manager = None
        self.is_running = False
        self.start_time = None
        self.historical_4h_data = None  # 마틴게일 전략용 4시간봉 데이터
        
        # 계정 매니저 초기화 (테스트넷 사용)
        self.account_manager = BitgetAccountManager(testnet=False)
        
        # 전략 초기화 (전략 타입에 따라)
        
        if strategy_type == 'squeeze_momentum':
            self.strategy = SqueezeMomentumStrategyPartialCloseRealTrading(
                # 기존 내용 삭제
            )
            logger.info("스퀴즈 모멘텀 전략 (부분 청산 포함, 실제 거래용)으로 초기화")
        
        
        # 시그널 핸들러 설정
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("비트겟 실시간 데이터 수신 시스템 초기화 완료")

    def _signal_handler(self, signum, frame):
        """시그널 핸들러 (Ctrl+C 등)"""
        logger.info(f"시그널 {signum} 수신, 시스템 종료 중...")
        self.stop()

    def initialize(self, enable_account_sync=False) -> bool:
        """
        시스템 초기화
        
        Args:
            enable_account_sync: 계정 동기화 활성화 여부
        
        Returns:
            bool - 초기화 성공 여부
        """
        try:
            logger.info("비트겟 시스템 초기화 시작...")
            
            # 과거 데이터 가져오기
            historical_data = self._fetch_historical_data()
            if historical_data is None or historical_data.empty:
                logger.error("과거 데이터를 가져올 수 없습니다.")
                return False
            
            # 마틴게일 전략인 경우 4시간봉 데이터는 별도 저장
            if self.strategy_type == 'martingale':
                self.historical_4h_data = historical_data.copy()
                # data_manager는 최소한의 1분봉 데이터가 필요하므로 별도로 가져옴
                from bitget_client import BitgetRealtimeClient
                client = BitgetRealtimeClient(symbol=self.symbol, testnet=self.testnet)
                minimal_1min_data = client.fetch_historical_klines(symbol=self.symbol, interval='1m', limit=1000)
                if minimal_1min_data is None or minimal_1min_data.empty:
                    logger.error("1분봉 데이터를 가져올 수 없습니다.")
                    return False
                historical_data = minimal_1min_data
            else:
                self.historical_4h_data = None
            
            # 데이터 매니저 생성 (계정 동기화 옵션 포함)
            self.data_manager = BitgetDataManager(
                symbol=self.symbol,
                testnet=self.testnet,
                strategy_callback=None,  # 35분봉 사용 안 함
                candle_1min_callback=self._on_candle_completed,  # 1분봉 완성 시 진입 조건 체크
                candle_30min_callback=self._on_30min_candle_completed,  # 30분봉 지표 계산 완료 시 진입 조건 체크
                enable_account_sync=enable_account_sync
            )
            
            # 실시간 trade 콜백 설정 (익절 체크용)
            if hasattr(self.data_manager, 'realtime_client') and self.data_manager.realtime_client:
                self.data_manager.realtime_client.set_trade_callback(self._on_trade_data)
            
            # 과거 데이터로 초기화
            self.data_manager.initialize_with_historical_data(historical_data)
            
            # 마틴게일 전략인 경우 4시간봉 RSI 계산 및 표시
            if self.strategy_type == 'martingale' and self.historical_4h_data is not None:
                from indicators import BitgetIndicatorCalculator
                indicator_calc = BitgetIndicatorCalculator()
                
                # 이미 4시간봉 데이터를 가져왔으므로 리샘플링 불필요
                if len(self.historical_4h_data) >= 14:
                    # 현재 시간 로그
                    current_time_utc = datetime.now(timezone.utc)
                    logger.info(f"[현재 시간 UTC] {current_time_utc}")
                    
                    # 마지막 4시간봉의 종료 시간 계산
                    last_candle_time = self.historical_4h_data.index[-1]
                    # 4시간봉의 예상 종료 시간 (시작 시간 + 4시간)
                    expected_end_time = last_candle_time + timedelta(hours=4)
                    logger.info(f"[마지막 4시간봉] 시작: {last_candle_time}, 예상 종료: {expected_end_time}")
                    
                    # 첫 번째와 두 번째 4시간봉 시간 확인 (비트겟 4시간봉 규칙 확인용)
                    first_candle_time = self.historical_4h_data.index[0]
                    second_candle_time = self.historical_4h_data.index[1] if len(self.historical_4h_data) > 1 else None
                    logger.info(f"[4시간봉 규칙 확인] 첫 번째: {first_candle_time}, 두 번째: {second_candle_time}")
                    
                    # 진행 중인 봉인지 확인
                    if current_time_utc < expected_end_time:
                        logger.warning(f"마지막 4시간봉이 아직 진행 중입니다! (남은 시간: {expected_end_time - current_time_utc})")
                        logger.warning("마지막 봉을 제외하고 RSI 계산하겠습니다.")
                        # 마지막 봉 제외
                        completed_candles = self.historical_4h_data.iloc[:-1]
                        logger.info(f"완성된 4시간봉 수: {len(completed_candles)}개 (원래 {len(self.historical_4h_data)}개)")
                    else:
                        logger.info("마지막 4시간봉은 이미 완성되었습니다.")
                        completed_candles = self.historical_4h_data
                    
                    # 최신 14개 데이터 로그 (표시용)
                    latest_14_closes = completed_candles['close'].tail(14) if len(completed_candles) >= 14 else completed_candles['close']
                    logger.info(f"[4시간봉 최신 14개 종가]")
                    for idx, close_val in latest_14_closes.items():
                        logger.info(f"  {idx}: {close_val:.2f}")
                    
                    # RSI 계산용 데이터: 전체 히스토리 사용 (RMA 누적 스무딩)
                    rsi_data = completed_candles['close']
                    
                    # RSI 계산
                    rsi_4h = indicator_calc.calculate_rsi(rsi_data, period=14)
                    if not rsi_4h.empty:
                        rsi_4hour = rsi_4h.iloc[-1]
                        logger.info(f"[초기화] 완성된 4시간봉 {len(completed_candles)}개, RSI 계산용 {len(rsi_data)}개, RSI(4H): {rsi_4hour:.2f}")
                    else:
                        logger.warning("RSI 4H 계산 결과가 비어있음")
                    
                    # 완성된 봉 데이터를 저장 (다음 계산에서 재사용)
                    self.historical_4h_data = completed_candles
                else:
                    logger.warning(f"4시간봉 데이터 부족 ({len(self.historical_4h_data)}개 < 14개)")
            
            # 계정 동기화 (활성화된 경우)
            if enable_account_sync:
                logger.info("계정 동기화 시작...")
                sync_result = self.data_manager.sync_with_account(force_sync=True)
                if sync_result['synced']:
                    logger.info(f"계정 동기화 완료 - {sync_result['action']}")
                else:
                    logger.warning(f"계정 동기화 실패: {sync_result.get('error', 'Unknown error')}")
            
            # 전략 계정 동기화 (포지션이 있으면 entry_count 복원)
            logger.info("전략 계정 동기화 시작...")
            strategy_sync_result = self.strategy.sync_with_account()
            if strategy_sync_result['synced']:
                logger.info(f"전략 계정 동기화 완료 - {strategy_sync_result['action']}")
                if strategy_sync_result.get('action') == 'restored':
                    logger.info(f"  복원된 상태:")
                    logger.info(f"    entry_count={strategy_sync_result.get('entry_count')}")
                    logger.info(f"    진입가={strategy_sync_result.get('entry_price', 0):.2f}")
                    logger.info(f"    전체 포지션={strategy_sync_result.get('position_size', 0):.6f} BTC")
                    logger.info(f"    초기 크기={strategy_sync_result.get('initial_position_size', 0):.6f} BTC")
            else:
                logger.warning(f"전략 계정 동기화 실패: {strategy_sync_result.get('error', 'Unknown error')}")
            
            # 스퀴즈 모멘텀 전략인 경우 초기화 시 지표 출력
            if self.strategy_type == 'squeeze_momentum':
                logger.info("=" * 80)
                logger.info("[초기화] 현재 지표 값 조회 중...")
                indicators = self.data_manager.get_indicators_for_strategy(force_calculation=True, force_log=True)
                if indicators:
                    logger.info("[초기화] 지표 조회 완료")
                else:
                    logger.warning("[초기화] 지표 조회 실패")
                logger.info("=" * 80)
            elif self.strategy_type == 'simple_test':
                logger.info("=" * 80)
                logger.info("[초기화] 간단한 테스트 전략 초기화 완료")
                logger.info("진입 조건: 가격 변화율 기반 (임계값: 0.2%)")
                logger.info("=" * 80)
            
            logger.info("비트겟 시스템 초기화 완료")
            return True
            
        except Exception as e:
            logger.error(f"비트겟 시스템 초기화 실패: {e}")
            return False

    def _fetch_historical_data(self) -> Optional[pd.DataFrame]:
        """
        과거 데이터 가져오기 (비트겟 API 사용)
        
        Returns:
            pd.DataFrame: 과거 1분봉 데이터
        """
        try:
            logger.info("비트겟 과거 데이터 가져오기 시작...")
            
            # 비트겟 클라이언트 생성
            from bitget_client import BitgetRealtimeClient
            client = BitgetRealtimeClient(symbol=self.symbol, testnet=self.testnet)
            
            # 마틴게일 전략: 4시간봉 1000개 가져오기 (RSI RMA 수렴용)
            # Support/Resistance 전략: 1분봉 1000개 (기존 방식)
            if self.strategy_type == 'martingale':
                # 4시간봉 1000개 가져오기 (RSI RMA 수렴에 충분한 히스토리 확보)
                historical_4h = client.fetch_historical_klines(
                    symbol=self.symbol,
                    interval='4H',  # 대문자
                    limit=1000
                )
                
                if historical_4h is None or historical_4h.empty:
                    logger.error("비트겟 4시간봉 과거 데이터가 비어있습니다.")
                    return None
                
                logger.info(f"비트겟 4시간봉 데이터 가져오기 완료 - 데이터 수: {len(historical_4h)}")
                return historical_4h
            else:
                # 최근 1000개 1분봉 데이터 가져오기 (비트겟 API 제한)
                historical_data = client.fetch_historical_klines(
                    symbol=self.symbol,
                    interval='1m',
                    limit=1000
                )
                
                if historical_data is not None and not historical_data.empty:
                    logger.info(f"비트겟 과거 데이터 가져오기 완료 - 데이터 수: {len(historical_data)}")
                    return historical_data
                else:
                    logger.error("비트겟 과거 데이터가 비어있습니다.")
                    return None
                
        except Exception as e:
            logger.error(f"비트겟 과거 데이터 가져오기 실패: {e}")
            return None

    def _on_candle_completed(self, candle_data: dict):
        """1분봉 완성 시 호출되는 콜백 (간헐적으로만 작동하므로 사용 안 함)"""
        # WebSocket 타이밍 문제로 간헐적으로만 호출됨
        # 대신 _check_entry_conditions_timer() 사용
        pass
    
    def _on_30min_candle_completed(self, completed_candle_time: datetime):
        """30분봉 지표 계산 완료 시 호출되는 콜백 (진입 조건 확인용)"""
        try:
            # data_manager가 초기화되지 않았으면 스킵
            if self.data_manager is None:
                logger.warning("[30분봉 콜백] data_manager가 아직 초기화되지 않음 - 스킵")
                return
            
            logger.info(f"[30분봉 콜백] 30분봉 지표 계산 완료 - 진입 조건 확인 시작 (봉 시간: {completed_candle_time})")
            
            # 지표 가져오기
            indicators = self.data_manager.get_indicators_for_strategy()
            if not indicators:
                logger.warning("[30분봉 콜백] 지표 없음")
                return
            
            # REST API로 계산한 마지막 1분봉 종가 사용
            current_price = indicators.get('last_candle_close')
            if not current_price or current_price <= 0:
                logger.warning("[30분봉 콜백] REST API 종가 없음")
                return
            
            # 스퀴즈 모멘텀 전략만 처리
            if self.strategy_type == 'squeeze_momentum':
                squeeze_val = indicators.get('squeeze_val', 0)
                squeeze_prev = indicators.get('squeeze_prev', 0)
                squeeze_state = indicators.get('squeeze', {})
                atr_30min = indicators.get('atr_30min', 0)
                ema_4h = indicators.get('ema_4h', current_price)
                last_candle_close_30min = indicators.get('last_candle_close', current_price)
                
                # 지표 유효성 검사
                import numpy as np
                indicators_valid = True
                if np.isnan(squeeze_val) or squeeze_val == 0:
                    logger.warning(f"[30분봉 콜백] squeeze_val이 유효하지 않음: {squeeze_val}")
                    indicators_valid = False
                if np.isnan(atr_30min) or atr_30min == 0:
                    logger.warning(f"[30분봉 콜백] atr_30min이 유효하지 않음: {atr_30min}")
                    indicators_valid = False
                if np.isnan(ema_4h) or ema_4h == 0:
                    logger.warning(f"[30분봉 콜백] ema_4h가 유효하지 않음: {ema_4h}")
                    indicators_valid = False
                
                if not indicators_valid:
                    logger.warning("[30분봉 콜백] 일부 지표가 유효하지 않아 진입 조건 체크를 건너뜁니다.")
                    return
                
                logger.info(
                    f"[30분봉 콜백] 진입 조건 확인: Price: {current_price:.2f}, "
                    f"Squeeze: {squeeze_val:.2f} (prev: {squeeze_prev:.2f}), "
                    f"ATR(30m): {atr_30min:.2f}, EMA(4h): {ema_4h:.2f}"
                )
                
                # 스퀴즈 모멘텀 전략 실행 (진입 조건 확인 포함)
                # 30분봉 완성 시점이므로 bar_index는 1 이상의 값 사용 가능 (하지만 실제로는 0 사용)
                # 진입 조건 확인은 30분봉 완성 시점에만 수행하므로 중복 진입 방지가 자연스럽게 해결됨
                signal_result = self.strategy.process_indicators(
                    indicators=indicators,
                    current_price=current_price,
                    bar_index=0,  # bar_index는 더 이상 중복 진입 방지에 사용되지 않음 (30분봉 완성 시점에만 진입 조건 확인)
                    skip_entry_check=False  # 진입 조건 확인 수행
                )
        except Exception as e:
            logger.error(f"[30분봉 콜백] 오류: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _update_4h_data(self):
        """4시간봉 데이터 갱신 (최신 1000개 가져오기)"""
        try:
            from bitget_client import BitgetRealtimeClient
            client = BitgetRealtimeClient(symbol=self.symbol, testnet=self.testnet)
            
            logger.info("[4시간봉] 최신 4시간봉 데이터 가져오기 시작...")
            historical_4h = client.fetch_historical_klines(
                symbol=self.symbol,
                interval='4H',
                limit=1000
            )
            
            if historical_4h is None or historical_4h.empty:
                logger.error("[4시간봉] 4시간봉 데이터를 가져올 수 없습니다.")
                return
            
            self.historical_4h_data = historical_4h
            logger.info(f"[4시간봉] 데이터 갱신 완료: {len(self.historical_4h_data)}개")
            
        except Exception as e:
            logger.error(f"[4시간봉] 데이터 갱신 오류: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _check_entry_conditions_timer(self):
        """타이머 기반 진입 조건 체크 (REST API 1분봉 종가 사용)"""
        try:
            logger.debug("[타이머 체크] 시작")
            # 지표 가져오기 (REST API 기반, 마지막 종가 포함)
            indicators = self.data_manager.get_indicators_for_strategy()
            if not indicators:
                logger.debug("[타이머 체크] 지표 없음")
                return
            
            # REST API로 계산한 마지막 1분봉 종가 사용
            current_price = indicators.get('last_candle_close')
            if not current_price or current_price <= 0:
                logger.warning("[타이머 체크] REST API 종가 없음")
                return
            
            # 전략 타입에 따라 다르게 처리
            if self.strategy_type == 'simple_test':
                # 간단한 테스트 전략: 가격만 사용
                signal_result = self.strategy.process_price(
                    current_price=current_price,
                    bar_index=0
                )
            elif self.strategy_type == 'squeeze_momentum':
                # 스퀴즈 모멘텀 전략: 30분봉 스퀴즈 모멘텀, ATR, 4시간봉 EMA 사용
                squeeze_val = indicators.get('squeeze_val', 0)
                squeeze_prev = indicators.get('squeeze_prev', 0)
                squeeze_state = indicators.get('squeeze', {})
                atr_30min = indicators.get('atr_30min', 0)
                ema_4h = indicators.get('ema_4h', current_price)
                last_candle_close_30min = indicators.get('last_candle_close', current_price)
                
                # 지표 유효성 검사
                import numpy as np
                indicators_valid = True
                if np.isnan(squeeze_val) or squeeze_val == 0:
                    logger.warning(f"[지표 검증] squeeze_val이 유효하지 않음: {squeeze_val}")
                    indicators_valid = False
                if np.isnan(atr_30min) or atr_30min == 0:
                    logger.warning(f"[지표 검증] atr_30min이 유효하지 않음: {atr_30min}")
                    indicators_valid = False
                if np.isnan(ema_4h) or ema_4h == 0:
                    logger.warning(f"[지표 검증] ema_4h가 유효하지 않음: {ema_4h}")
                    indicators_valid = False
                
                if not indicators_valid:
                    logger.warning("[지표 검증] 일부 지표가 유효하지 않아 진입 조건 체크를 건너뜁니다.")
                    return
                
                # 지표가 새로 계산되었을 때만 로그 출력
                calc_30min = indicators.get('_calc_30min', False)
                calc_4h = indicators.get('_calc_4h', False)
                
                if calc_30min or calc_4h:
                    logger.info(
                        f"[타이머 체크] Price: {current_price:.2f}, "
                        f"Squeeze: {squeeze_val:.2f} (prev: {squeeze_prev:.2f}), "
                        f"ATR(30m): {atr_30min:.2f}, EMA(4h): {ema_4h:.2f}"
                    )
                
                # 스퀴즈 모멘텀 전략 실행
                # 타이머 기반 호출: 진입 조건 확인 스킵, 포지션 관리만 수행 (브레이크이븐, 부분 청산 등)
                # 진입 조건 확인은 30분봉 지표 계산 시점에만 수행
                signal_result = self.strategy.process_indicators(
                    indicators=indicators,
                    current_price=current_price,
                    bar_index=0,
                    skip_entry_check=True  # 타이머에서는 진입 조건 확인 스킵
                )
            elif self.strategy_type == 'martingale':
                # 마틴게일 전략: RSI 4H 계산
                from indicators import BitgetIndicatorCalculator
                indicator_calc = BitgetIndicatorCalculator()
                
                # 4H 데이터 가져오기 (별도 저장된 데이터 사용)
                if self.historical_4h_data is not None and len(self.historical_4h_data) >= 14:
                    # RSI 계산용 데이터: 진행 중인 봉 제외
                    current_time_utc = datetime.now(timezone.utc)
                    last_candle_time = self.historical_4h_data.index[-1]
                    expected_end_time = last_candle_time + timedelta(hours=4)
                    
                    # 진행 중인 봉인지 확인 후 제외
                    rsi_calc_data = self.historical_4h_data
                    if current_time_utc < expected_end_time:
                        rsi_calc_data = self.historical_4h_data.iloc[:-1]
                    
                    # RSI 계산
                    if len(rsi_calc_data) >= 14:
                        # 전체 히스토리 사용 (RMA 누적 스무딩)
                        rsi_data = rsi_calc_data['close']
                        rsi_4h = indicator_calc.calculate_rsi(rsi_data, period=14)
                        if not rsi_4h.empty:
                            rsi_4hour = rsi_4h.iloc[-1]
                            logger.debug(f"[RSI 4H 계산] 완성된 4시간봉 {len(rsi_calc_data)}개, RSI 계산용 전체 히스토리, 최신 RSI: {rsi_4hour:.2f}")
                        else:
                            rsi_4hour = 50
                            logger.warning("RSI 4H 계산 결과가 비어있음, 기본값 50 사용")
                    else:
                        rsi_4hour = 50
                        logger.warning(f"완성된 4시간봉 데이터 부족 ({len(rsi_calc_data)}개 < 14개), 기본값 50 사용")
                else:
                    rsi_4hour = 50
                    logger.warning(f"4시간봉 데이터 부족, 기본값 50 사용")
                
                # RSI 4H를 indicators에 추가
                indicators['rsi_4hour'] = rsi_4hour
                
                logger.info(f"[타이머 체크] Price: {current_price:.2f}, RSI(4H): {rsi_4hour:.2f}")
                
                # 마틴게일 전략 실행
                signal_result = self.strategy.process_indicators(
                    indicators=indicators,
                    current_price=current_price
                )
            else:
                # Support/Resistance 전략: Squeeze 사용
                squeeze_val = indicators.get('squeeze_momentum', np.nan)
                if np.isnan(squeeze_val):
                    logger.debug("[타이머 체크] Squeeze 값 NaN")
                    return
                
                atr = indicators.get('atr', 0)
                
                logger.info(f"[타이머 체크] Price: {current_price:.2f}, Squeeze: {squeeze_val:.2f}, ATR: {atr:.2f}")
                
                # Support/Resistance 전략 실행
                signal_result = self.strategy.process_signal(
                    indicators=indicators,
                    current_price=current_price,
                    atr=atr
                )
            
            # 전략 결과 로그 (hold가 아닐 때만)
            if signal_result['action'] != 'hold':
                logger.info(f"[타이머 신호] {signal_result['action']}: {signal_result}")
                
                # 전략 상태 출력 (전략 타입에 따라 다르게)
                status = self.strategy.get_strategy_status()
                
                if self.strategy_type == 'simple_test' or self.strategy_type == 'squeeze_momentum':
                    # 간단한 테스트 전략 또는 스퀴즈 모멘텀 전략 상태
                    if status.get('has_position'):
                        logger.info(
                            f"[전략 상태] 포지션: {status['position_side'].upper()}, "
                            f"포지션크기: {status['position_size']:.6f}, "
                            f"평단가: {status['avg_price']:.2f}"
                        )
                        if status.get('tp_price'):
                            logger.info(
                                f"[익절/손절] 익절가: {status['tp_price']:.2f}, "
                                f"손절가: {status['sl_price']:.2f}"
                            )
                        logger.info(
                            f"[자산] 총자산: {status.get('equity', 0):.2f}, "
                            f"재투자자본: {status.get('reinvested_capital', 0):.2f}, "
                            f"보존수익: {status.get('saved_profit', 0):.2f}"
                        )
                    else:
                        logger.info(f"[전략 상태] 포지션 없음")
                        logger.info(
                            f"[자산] 총자산: {status.get('equity', 0):.2f}, "
                            f"재투자자본: {status.get('reinvested_capital', 0):.2f}, "
                            f"보존수익: {status.get('saved_profit', 0):.2f}"
                        )
                elif self.strategy_type == 'martingale':
                    # 마틴게일 전략 상태
                    if status.get('long', {}).get('has_position'):
                        long_info = status['long']
                        logger.info(
                            f"[롱 포지션] 진입횟수: {long_info['entry_count']}, "
                            f"평단가: {long_info['avg_price']:.2f}, "
                            f"포지션크기: {long_info['position_size']:.6f}, "
                            f"익절가: {long_info['profit_one']:.2f if long_info.get('profit_one') else 'N/A'}, "
                            f"청산가: {long_info['liquidation_price']:.2f if long_info.get('liquidation_price') else 'N/A'}"
                        )
                    elif status.get('short', {}).get('has_position'):
                        short_info = status['short']
                        logger.info(
                            f"[숏 포지션] 진입횟수: {short_info['entry_count']}, "
                            f"평단가: {short_info['avg_price']:.2f}, "
                            f"포지션크기: {short_info['position_size']:.6f}, "
                            f"익절가: {short_info['profit_one']:.2f if short_info.get('profit_one') else 'N/A'}"
                        )
                    else:
                        logger.info(f"[포지션] 없음")
                    
                    logger.info(
                        f"[자산] 총자산: {status.get('equity', 0):.2f}, "
                        f"현금보유량: {status.get('cash_reserve', 0):.2f}, "
                        f"총출금수익: {status.get('total_profit_withdrawn', 0):.2f}"
                    )
                else:
                    # Support/Resistance 전략 상태
                    if status['has_position']:
                        logger.info(f"[전략 상태] 진입횟수: {status['entry_count']}, "
                                   f"평단가: {status['avg_price']:.2f}, "
                                   f"포지션크기: {status['position_size']:.4f}, "
                                   f"미실현손익: {status['unrealized_pnl']:.2f}")
                        
                        # 복리 투자 정보 표시
                        logger.info(f"[복리 투자] 초기금액: {status['investment_amount']:.2f}, "
                                   f"현재잔고: {status['compound_balance']:.2f}, "
                                   f"총수익: {status['total_profit']:.2f}")
                        
                        # 익절 레벨 정보 표시
                        profit_levels = self.strategy.get_profit_levels()
                        logger.info(f"[익절 레벨] 부분익절: {profit_levels['partial_profit_price']:.2f}, "
                                   f"전체익절: {profit_levels['full_profit_price']:.2f}, "
                                   f"청산가: {profit_levels['liquidation_price']:.2f}")
                    else:
                        logger.info(f"[전략 상태] 포지션 없음")
                        logger.info(f"[복리 투자] 초기금액: {status['investment_amount']:.2f}, "
                                   f"현재잔고: {status['compound_balance']:.2f}, "
                                   f"총수익: {status['total_profit']:.2f}")
                
        except Exception as e:
            logger.error(f"타이머 진입 조건 체크 오류: {e}")
            import traceback
            logger.error(f"상세 오류: {traceback.format_exc()}")
    
    def _on_trade_data(self, trade_data: Dict):
        """실시간 trade 데이터 콜백 (익절 체크)"""
        try:
            # 포지션이 없으면 틱 체크 스킵
            status = self.strategy.get_strategy_status()
            if not status['has_position']:
                return
            
            current_price = trade_data['price']
            
            # 틱 데이터 기반 익절 체크 (실시간) - 메서드가 있는 경우에만
            if hasattr(self.strategy, 'check_tick_based_exit'):
                tick_exit_result = self.strategy.check_tick_based_exit(current_price)
                if tick_exit_result and tick_exit_result['action'] != 'hold':
                    logger.info(f"[실시간 trade 익절] {tick_exit_result['action']}: {tick_exit_result}")
                    
                    # 익절 후 전략 상태 출력
                    status = self.strategy.get_strategy_status()
                    if status.get('has_position'):
                        logger.info(f"[trade 익절 후 상태] 진입횟수: {status.get('entry_count', 'N/A')}, "
                                   f"평단가: {status.get('avg_price', 0):.2f}, "
                                   f"포지션크기: {status.get('position_size', 0):.4f}")
                    else:
                        logger.info(f"[trade 익절 후 상태] 포지션 없음")
                
        except Exception as e:
            logger.error(f"trade 데이터 처리 오류: {e}")
            import traceback
            logger.error(f"trade 처리 상세 오류: {traceback.format_exc()}")

    def _display_position_info(self):
        """현재 포지션 정보 표시"""
        try:
            # 계정 매니저에서 포지션 정보 가져오기
            if not self.account_manager:
                logger.debug("[포지션] 계정 매니저 없음")
                return
            
            # 일반적으로는 캐시 사용 (API 호출 최소화), 포지션 종료 감지 시에만 force_update=True 사용
            positions = self.account_manager.get_positions(force_update=False)
            
            # 포지션이 없으면 간단한 로그 출력
            if not positions:
                # 포지션 없음 상태를 간단히 로그로 표시
                logger.info("[포지션] 현재 포지션 없음")
                return
            
            # 첫 번째 포지션 정보 표시 (BTCUSDT)
            position = positions[0]
            
            # 포지션 크기 확인 (0.0001 미만이면 포지션 없음으로 간주)
            size = position.get('size') or position.get('contracts', 0)
            if abs(float(size)) < 0.0001:
                # 포지션 크기가 너무 작으면 포지션 없음으로 간주 (로그 출력 안 함)
                return
            
            # 포지션 기본 정보
            symbol = position.get('symbol', 'UNKNOWN')
            side = position.get('side', 'UNKNOWN')
            size = position.get('size') or position.get('contracts', 0)  # size 또는 contracts 사용
            entry_price = position.get('entry_price') or position.get('entryPrice', 0)
            liquidation_price = position.get('liquidation_price') or position.get('liquidationPrice', 0)
            mark_price = position.get('mark_price') or position.get('markPrice', 0)
            unrealized_pnl = position.get('unrealized_pnl') or position.get('unrealizedPnl', 0)
            leverage = position.get('leverage', 1)
            margin_mode = position.get('margin_mode') or position.get('marginMode', 'cross')
            
            # 포지션 방향 표시 (side는 'buy' 또는 'sell')
            side_display = "롱" if side.lower() == "buy" else "숏" if side.lower() == "sell" else side
            
            # 손익 표시 (색상 구분)
            pnl_display = f"+{unrealized_pnl:.2f}" if unrealized_pnl >= 0 else f"{unrealized_pnl:.2f}"
            pnl_status = "수익" if unrealized_pnl >= 0 else "손실"
            
            # 익절가 계산 (마틴게일 전략인 경우)
            take_profit_price = None
            if self.strategy_type == 'martingale' and self.strategy:
                try:
                    avg_price = entry_price  # 평균가는 entry_price와 동일
                    if avg_price > 0:
                        # 전략 상태에서 take_profit_pct 가져오기
                        if hasattr(self.strategy, 'take_profit_pct'):
                            take_profit_pct = self.strategy.take_profit_pct
                            # 롱 포지션 (buy): 평균가 * (1 + take_profit_pct%)
                            if side.lower() == "buy":
                                take_profit_price = avg_price * (1.0 + take_profit_pct / 100.0)
                            # 숏 포지션 (sell): 평균가 * (1 - take_profit_pct%)
                            elif side.lower() == "sell":
                                take_profit_price = avg_price * (1.0 - take_profit_pct / 100.0)
                except Exception as e:
                    logger.debug(f"익절가 계산 오류: {e}")
            
            # 포지션 정보 로그 출력 (INFO 레벨로 변경하여 더 자주 표시)
            profit_info = f" | 익절가: {take_profit_price:.2f}" if take_profit_price else ""
            logger.info(f"[포지션] {symbol} {side_display} {size} @ {entry_price:.2f} | "
                       f"청산가: {liquidation_price:.2f}{profit_info} | "
                       f"마크가: {mark_price:.2f} | "
                       f"손익: {pnl_display} ({pnl_status}) | "
                       f"레버리지: {leverage}x | "
                       f"마진: {margin_mode}")
            
            # 리스크 레벨 계산 및 표시
            if entry_price > 0 and mark_price > 0:
                price_diff_pct = ((mark_price - entry_price) / entry_price) * 100
                # 롱 포지션 (buy): 마크가가 청산가보다 높아야 함
                if side.lower() == "buy":
                    liquidation_diff_pct = ((mark_price - liquidation_price) / liquidation_price) * 100
                # 숏 포지션 (sell): 마크가가 청산가보다 낮아야 함
                else:
                    liquidation_diff_pct = ((liquidation_price - mark_price) / liquidation_price) * 100
                
                logger.info(f"[리스크] 가격변동: {price_diff_pct:+.2f}% | "
                           f"청산까지: {liquidation_diff_pct:.2f}%")
            
        except Exception as e:
            logger.error(f"포지션 정보 표시 오류: {e}")

    def start(self):
        """시스템 시작"""
        try:
            if self.is_running:
                logger.warning("이미 실행 중입니다.")
                return
            
            logger.info("비트겟 실시간 자동매매 시스템 시작...")
            
            # 데이터 매니저 시작
            self.data_manager.start()
            
            self.is_running = True
            self.start_time = datetime.now(timezone.utc)
            
            logger.info(f"비트겟 시스템 시작 완료 - 시작 시간: {self.start_time}")
            
            # 메인 루프
            self._main_loop()
            
        except Exception as e:
            logger.error(f"시스템 시작 오류: {e}")
            self.stop()

    def _main_loop(self):
        """메인 실행 루프"""
        try:
            loop_count = 0
            last_check_time = None  # 마지막 진입 조건 체크 시간
            last_4h_check = None  # 마지막 4시간봉 체크 시간
            last_websocket_check = None  # 마지막 WebSocket 연결 상태 확인 시간
            last_worker_check = None  # 마지막 워커 상태 확인 시간
            
            while self.is_running:
                try:
                    loop_count += 1
                    current_time = datetime.now()
                    current_time_utc = datetime.now(timezone.utc)
                    
                    # 10초마다 진입 조건 체크 (진입 주문 체결 확인, TP/SL 주문 빠른 설정, 포지션 종료 빠른 감지)
                    if last_check_time is None or (current_time_utc - last_check_time).total_seconds() >= 10:
                        last_check_time = current_time_utc
                        self._check_entry_conditions_timer()
                    
                    # 마틴게일 전략인 경우 4시간봉 데이터 업데이트 (4시간마다)
                    if self.strategy_type == 'martingale' and self.historical_4h_data is not None:
                        current_time_utc = datetime.now(timezone.utc)
                        # 4시간봉 경계 시간 체크 (00:00, 04:00, 08:00, 12:00, 16:00, 20:00)
                        current_hour = current_time_utc.hour
                        if (current_hour % 4 == 0 and current_time_utc.minute == 0 and 
                            current_time_utc.second == 1 and last_4h_check != current_time_utc):
                            last_4h_check = current_time_utc
                            logger.info(f"[4시간봉] 새 봉 시작 감지: {current_time_utc}, 4시간봉 데이터 갱신 중...")
                            self._update_4h_data()
                    
                    # 시스템 상태 모니터링
                    if self.data_manager:
                        current_price = self.data_manager.get_current_price()
                        # if current_price:
                            # logger.debug(f"[모니터링] 현재 가격: {current_price:.2f}")
                        
                        # 워커 상태 확인 (30초마다)
                        if last_worker_check is None or (current_time_utc - last_worker_check).total_seconds() >= 30:
                            last_worker_check = current_time_utc
                            health = self.data_manager.get_health()
                            logger.info(f"[모니터링] 워커 상태: {health}")
                            
                            # 워커가 죽었으면 재시작
                            if not health.get('worker_alive', False):
                                logger.warning("[모니터링] 워커가 중단됨 - 재시작 시도")
                                self.data_manager.ensure_worker()
                        
                        # 포지션 정보 표시 (10초마다, 워커 상태 확인과 동일한 주기)
                        if loop_count % 10 == 0 and self.account_manager:
                            self._display_position_info()
                        
                        # WebSocket 연결 상태 확인 (30초마다)
                        if last_websocket_check is None or (current_time_utc - last_websocket_check).total_seconds() >= 30:
                            last_websocket_check = current_time_utc
                            if hasattr(self.data_manager, 'realtime_client'):
                                client = self.data_manager.realtime_client
                                if hasattr(client, 'is_connected'):
                                    if not client.is_connected:
                                        logger.warning("[모니터링] WebSocket 연결 끊어짐 - 재연결 시도 중...")
                                    else:
                                        logger.debug("[모니터링] WebSocket 연결 정상")
                    
                    # 1초 대기 (타이머 체크를 위해)
                    time.sleep(1)
                    
                except KeyboardInterrupt:
                    logger.info("사용자에 의해 중단됨")
                    break
                except Exception as e:
                    logger.error(f"메인 루프 오류: {e}")
                    time.sleep(5)  # 오류 발생 시 5초 대기 후 재시도
                    
        except Exception as e:
            logger.error(f"메인 루프 중단: {e}")
        finally:
            self.stop()

    def stop(self):
        """시스템 중지"""
        try:
            if not self.is_running:
                logger.warning("이미 중지되었습니다.")
                return
            
            logger.info("비트겟 시스템 중지 중...")
            
            # 데이터 매니저 중지
            if self.data_manager:
                self.data_manager.stop()
            
            self.is_running = False
            
            if self.start_time:
                runtime = datetime.now(timezone.utc) - self.start_time
                logger.info(f"비트겟 시스템 중지 완료 - 실행 시간: {runtime}")
            else:
                logger.info("비트겟 시스템 중지 완료")
                
        except Exception as e:
            logger.error(f"시스템 중지 오류: {e}")

    def get_status(self) -> dict:
        """시스템 상태 가져오기"""
        try:
            status = {
                'is_running': self.is_running,
                'start_time': self.start_time,
                'data_manager_running': self.data_manager.is_running() if self.data_manager else False,
                'current_price': self.data_manager.get_current_price() if self.data_manager else None,
                'position_info': None
            }
            
            # 포지션 정보 추가
            if self.account_manager:
                try:
                    positions = self.account_manager.get_positions()
                    if positions:
                        position = positions[0]  # 첫 번째 포지션
                        status['position_info'] = {
                            'symbol': position.get('symbol', 'UNKNOWN'),
                            'side': position.get('side', 'UNKNOWN'),
                            'size': position.get('size') or position.get('contracts', 0),
                            'entry_price': position.get('entry_price') or position.get('entryPrice', 0),
                            'liquidation_price': position.get('liquidation_price') or position.get('liquidationPrice', 0),
                            'mark_price': position.get('mark_price') or position.get('markPrice', 0),
                            'unrealized_pnl': position.get('unrealized_pnl') or position.get('unrealizedPnl', 0),
                            'leverage': position.get('leverage', 1),
                            'margin_mode': position.get('margin_mode') or position.get('marginMode', 'cross')
                        }
                    else:
                        status['position_info'] = {'status': 'no_position'}
                except Exception as e:
                    status['position_info'] = {'error': str(e)}
            
            if self.start_time:
                status['runtime'] = datetime.now(timezone.utc) - self.start_time
            
            return status
            
        except Exception as e:
            logger.error(f"상태 가져오기 오류: {e}")
            return {'error': str(e)}


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='비트겟 실시간 데이터 수신 및 지표 계산 시스템')
    parser.add_argument('--symbol', default='BTCUSDT', help='거래 심볼 (기본값: BTCUSDT)')
    parser.add_argument('--testnet', action='store_true', help='테스트넷 사용')
    parser.add_argument('--account-sync', action='store_true', help='계정 동기화 활성화')
    parser.add_argument('--strategy', default='squeeze_momentum', 
                        choices=['simple_test', 'squeeze_momentum', 'support_resistance', 'martingale'],
                        help='전략 타입 (기본값: squeeze_momentum)')
    
    args = parser.parse_args()
    
    # 시스템 생성 및 실행
    system = BitgetRealtimeTradingSystem(symbol=args.symbol, testnet=args.testnet, strategy_type=args.strategy)
    
    try:
        # 초기화 (계정 동기화 옵션 포함)
        if not system.initialize(enable_account_sync=args.account_sync):
            logger.error("시스템 초기화 실패")
            return 1
        
        # 시작
        system.start()
        
    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단됨")
    except Exception as e:
        logger.error(f"시스템 실행 오류: {e}")
        return 1
    finally:
        system.stop()
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
