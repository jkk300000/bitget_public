"""
비트겟 계정 정보 관리 모듈

ccxt를 사용하여 비트겟 계정에서 실제 포지션 및 잔고 정보를 가져옵니다.
"""

import ccxt
import pandas as pd
import numpy as np
from typing import Dict, Optional, List, Any
import logging
from datetime import datetime, timezone
import time
import os
from dotenv import load_dotenv

# 로거 설정
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s][%(levelname)s][%(name)s] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

class BitgetAccountManager:
    """
    비트겟 계정 관리 클래스
    
    ccxt를 사용하여 실제 계정 정보를 가져오고 관리합니다.
    """
    
    def __init__(self, testnet: bool = False):
        """
        초기화
        
        Args:
            testnet: 테스트넷 사용 여부
        """
        # .env 파일 경로 설정 (bitget 디렉토리 기준)
        env_path = os.path.join(os.path.dirname(__file__), '.env')
        load_dotenv(env_path)
        
        self.testnet = testnet
        self.exchange = None
        self.symbol = 'BTCUSDT'
        
        # API 키 설정
        self.api_key = os.getenv('BITGET_API_KEY')
        self.secret_key = os.getenv('BITGET_SECRET_KEY')
        self.passphrase = os.getenv('BITGET_PASSPHRASE')
        
        # 계정 정보 캐시 (포지션과 잔고 분리)
        self._position_cache = {}
        self._balance_cache = {}
        self._last_position_update = 0  # 포지션 마지막 업데이트 시간
        self._last_balance_update = 0  # 잔고 마지막 업데이트 시간
        self._cache_duration = 15  # 15초 캐시 (기본)
        self._last_429_error_time = 0  # 마지막 429 에러 발생 시간
        self._last_position_state = None  # 마지막 포지션 상태 (변화 감지용)
        
        # 포지션 체크 상태 관리 (하이브리드 방식)
        self._position_check_enabled = True  # 포지션 체크 활성화 여부
        self._position_zero_confirmed = False  # 포지션 0인 상태 확인됨 플래그
        self._position_zero_confirmed_time = 0  # 포지션 0 확인 시간
        self._position_check_interval_no_position = 60  # 포지션 없을 때 안전장치 체크 주기 (60초)
        self._position_check_interval_with_position = 5  # 포지션이 있을 때 체크 주기 (5초)
        
        # 잔고 체크 상태 관리 (포지션과 연동)
        self._balance_check_enabled = True  # 잔고 체크 활성화 여부
        self._balance_check_interval = 30  # 잔고 체크 주기 (30초, 포지션보다 덜 자주)
        
        self._initialize_exchange()
        
        logger.info(f"비트겟 계정 매니저 초기화 완료 - 테스트넷: {testnet}")
    
    def _initialize_exchange(self):
        """ccxt exchange 초기화"""
        try:
            # API 키 검증
            if not self.api_key or not self.secret_key or not self.passphrase:
                raise Exception("API 키, 시크릿 키, 패스프레이즈가 모두 설정되어야 합니다.")
            
            # API 키 정보 로그 (마스킹 처리)
            masked_api_key = self.api_key[:8] + "..." + self.api_key[-4:] if self.api_key else "None"
            logger.info(f"비트겟 API 연결 시도 - 테스트넷: {self.testnet}")
            logger.info(f"API 키: {masked_api_key}")
            logger.info(f"시크릿 키: {'설정됨' if self.secret_key else 'None'}")
            logger.info(f"패스프레이즈: {'설정됨' if self.passphrase else 'None'}")
            
            # API 키 형식 검증
            if len(self.api_key) < 20:
                logger.warning("API 키 길이가 짧습니다. 올바른 API 키인지 확인하세요.")
            if len(self.secret_key) < 20:
                logger.warning("시크릿 키 길이가 짧습니다. 올바른 시크릿 키인지 확인하세요.")
            if len(self.passphrase) < 5:
                logger.warning("패스프레이즈가 너무 짧습니다. 올바른 패스프레이즈인지 확인하세요.")
            
            # 비트겟 API 설정 (환경에 관계없이 동일한 설정 사용)
            logger.info(f"{'테스트넷' if self.testnet else '메인넷'} 모드로 연결 중...")
            
            # 기본 설정
            config = {
                'apiKey': self.api_key,
                'secret': self.secret_key,
                'password': self.passphrase,
                'enableRateLimit': True,
                'options': {
                    'defaultType': 'future',  # 선물 거래
                    'recvWindow': 10000,  # 10초 타임아웃
                    'defaultMarginMode': 'cross',  # 크로스 마진 모드
                    'defaultSettle': 'USDT',  # USDT 정산
                }
            }
            
            # 테스트넷인 경우 sandbox 설정 추가
            if self.testnet:
                config['sandbox'] = True
                # ccxt가 자동으로 테스트넷 URL을 사용하도록 URL 설정 제거
                # sandbox: True일 때 ccxt가 자동으로 올바른 테스트넷 URL을 사용함
                logger.info("테스트넷 sandbox 모드 활성화 (ccxt가 자동으로 테스트넷 URL 사용)")
            else:
                logger.info("메인넷 모드 활성화")
            
            self.exchange = ccxt.bitget(config)
            
            # CCXT 디버그 로그 비활성화 (API 키 노출 방지)
            logging.getLogger('ccxt.base.exchange').setLevel(logging.WARNING)
            logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
            
            # 연결 테스트 (단계별)
            logger.info("1단계: 마켓 데이터 로딩 중...")
            markets = self.exchange.load_markets()
            logger.info(f"[OK] 마켓 데이터 로딩 완료 - {len(markets)}개 심볼")
            
            # 공개 API 테스트 (인증 없이)
            logger.info("2단계: 공개 API 테스트 중...")
            try:
                ticker = self.exchange.fetch_ticker(self.symbol)
                logger.info(f"[OK] 공개 API 성공 - {self.symbol} 현재가: {ticker['last']:.2f}")
            except Exception as e:
                logger.warning(f"⚠️ 공개 API 테스트 실패: {e}")
            
            # 인증 API 테스트
            logger.info("3단계: 인증 API 테스트 중...")
            try:
                # 잔고 조회 테스트 (USDT 마진 명시)
                logger.info("  - 잔고 조회 테스트...")
                balance = self.exchange.fetch_balance({'type': 'future', 'marginMode': 'cross','productType': 'USDT-FUTURES', 'marginCoin': 'USDT'})
                usdt_balance = balance.get('USDT', {}).get('total', 0)
                logger.info(f"  [OK] 잔고 조회 성공 - USDT: {usdt_balance:.2f}")
                
                # 포지션 조회 테스트 (USDT 선물)
                logger.info("  - 포지션 조회 테스트...")
                positions = self.exchange.fetch_positions([self.symbol], {'type': 'future', 'productType': 'USDT-FUTURES', 'marginMode': 'cross', 'marginCoin': 'USDT'})
                active_positions = [pos for pos in positions if float(pos.get('contracts', 0)) > 0 or float(pos.get('total', 0)) > 0]
                logger.info(f"  [OK] 포지션 조회 성공 - 활성 포지션: {len(active_positions)}개")
                
                logger.info("[OK] 비트겟 API 연결 및 인증 성공!")
                
            except Exception as e:
                logger.error(f"[FAIL] 인증 API 테스트 실패: {e}")
                
                # 서명 오류에 대한 상세 진단
                if "sign signature error" in str(e):
                    logger.error("🔍 서명 오류 진단:")
                    logger.error("1. API 키가 올바른지 확인하세요")
                    logger.error("2. 시크릿 키가 올바른지 확인하세요")
                    logger.error("3. 패스프레이즈가 올바른지 확인하세요")
                    logger.error("4. API 키에 선물 거래 권한이 있는지 확인하세요")
                    logger.error("5. 시계가 정확한지 확인하세요 (시간 동기화)")
                elif "40009" in str(e):
                    logger.error("🔍 API 키 인증 오류 (40009):")
                    logger.error("- API 키, 시크릿 키, 패스프레이즈를 다시 확인하세요")
                    logger.error("- 비트겟에서 새로 생성한 API 키인지 확인하세요")
                    logger.error("- API 키 권한에 '선물 거래'가 포함되어 있는지 확인하세요")
                
                # 인증 실패해도 공개 API는 사용 가능하므로 계속 진행
                logger.warning("⚠️ 인증 실패했지만 공개 API는 사용 가능합니다.")
                logger.warning("⚠️ 포지션 정보나 거래 기능은 사용할 수 없습니다.")
            
        except Exception as e:
            logger.error(f"[FAIL] 비트겟 API 연결 실패: {e}")
            logger.error(f"오류 타입: {type(e).__name__}")
            if hasattr(e, 'response'):
                logger.error(f"응답 상태: {e.response.status_code if hasattr(e.response, 'status_code') else 'Unknown'}")
            raise
    
    def get_account_balance(self, force_update: bool = False) -> Dict[str, Any]:
        """
        계정 잔고 정보 가져오기 (하이브리드 방식: 포지션 없을 때는 호출 최소화)
        
        Args:
            force_update: 강제 업데이트 여부
        
        Returns:
            Dict[str, Any] - 잔고 정보
        """
        try:
            current_time = time.time()
            
            # 429 에러 발생 후 60초 동안은 API 호출 자제
            if self._last_429_error_time > 0:
                time_since_429 = current_time - self._last_429_error_time
                if time_since_429 < 60:
                    if not force_update and self._balance_cache:
                        logger.debug(f"429 에러 발생 후 {time_since_429:.1f}초 경과. 잔고 캐시 사용.")
                        return self._balance_cache
            
            # 포지션 없을 때는 잔고 체크도 최소화
            if not force_update and not self._balance_check_enabled and self._position_zero_confirmed:
                time_since_zero_confirmed = current_time - self._position_zero_confirmed_time
                if time_since_zero_confirmed < self._position_check_interval_no_position:
                    # 포지션 없음 상태에서는 잔고도 캐시 사용
                    if self._balance_cache:
                        return self._balance_cache
                    # 캐시도 없으면 기본값 반환
                    return {
                        'total': 0.0,
                        'free': 0.0,
                        'used': 0.0,
                        'timestamp': datetime.now(timezone.utc)
                    }
            
            # 캐시 확인 (잔고는 포지션보다 덜 자주 체크)
            if not force_update and self._last_balance_update > 0:
                if (current_time - self._last_balance_update) < self._balance_check_interval:
                    return self._balance_cache
                # 캐시 유효 기간 내라면 캐시 사용
                if (current_time - self._last_balance_update) < self._cache_duration:
                    return self._balance_cache
            
            # 잔고 조회 (USDT 선물 마진 명시)
            balance = self.exchange.fetch_balance({'type': 'future', 'marginMode': 'cross', 'productType': 'USDT-FUTURES', 'marginCoin': 'USDT'})
            
            # USDT 잔고 추출
            usdt_balance = balance.get('USDT', {})
            
            result = {
                'total': usdt_balance.get('total', 0.0),
                'free': usdt_balance.get('free', 0.0),
                'used': usdt_balance.get('used', 0.0),
                'timestamp': datetime.now(timezone.utc)
            }
            
            # 캐시 업데이트 (잔고 전용 업데이트 시간 사용)
            self._balance_cache = result
            self._last_balance_update = current_time
            
            logger.debug(f"계정 잔고: {result}")
            return result
            
        except Exception as e:
            # 상세한 에러 정보 로깅
            error_msg = str(e)
            error_type = type(e).__name__
            logger.error(f"잔고 조회 오류 [{error_type}]: {error_msg}")
            
            # 429 에러 처리
            is_429_error = '429' in error_msg or 'Too Many Requests' in error_msg
            if is_429_error:
                self._last_429_error_time = current_time
                logger.warning("429 에러 발생 - 60초 동안 API 호출 자제, 캐시 사용")
            
            # ccxt 에러인 경우 추가 정보
            if hasattr(e, 'args') and e.args:
                logger.error(f"에러 상세: {e.args}")
            
            # 캐시된 데이터가 있으면 반환
            if self._balance_cache:
                if is_429_error:
                    logger.warning(f"429 에러 발생, 캐시된 잔고 데이터 반환: {self._balance_cache}")
                else:
                    logger.warning(f"에러 발생, 캐시된 잔고 데이터 반환: {self._balance_cache}")
                return self._balance_cache
            
            return {
                'total': 0.0,
                'free': 0.0,
                'used': 0.0,
                'timestamp': datetime.now(timezone.utc)
            }
    
    def get_positions(self, force_update: bool = False) -> List[Dict[str, Any]]:
        """
        포지션 정보 가져오기
        
        Args:
            force_update: 강제 업데이트 여부
        
        Returns:
            List[Dict[str, Any]] - 포지션 리스트
        """
        try:
            current_time = time.time()
            
            # 동적 체크 주기 확인 (포지션이 있을 때는 더 자주 체크)
            if not force_update:
                # 포지션이 있을 때는 더 짧은 간격으로 체크 (빠른 청산 감지)
                has_position = len(self._position_cache) > 0
                check_interval = self._position_check_interval_with_position if has_position else self._position_check_interval_no_position
                
                # 캐시가 유효하고 체크 주기가 지나지 않았으면 캐시 사용
                if self._last_position_update > 0 and (current_time - self._last_position_update) < check_interval:
                    # logger.debug("캐시된 포지션 데이터 사용")  # 반복 로그 주석처리
                    return list(self._position_cache.values())
                
                # 캐시가 있지만 체크 주기가 지났고, 캐시 유효 기간 내라면 캐시 사용
                # (API 호출 최소화)
                if self._last_position_update > 0 and (current_time - self._last_position_update) < self._cache_duration:
                    # logger.debug("캐시된 포지션 데이터 사용 (체크 주기 내)")  # 반복 로그 주석처리
                    return list(self._position_cache.values())
            
            # 포지션 조회 (USDT 선물) - 디버깅 정보 추가
            # logger.debug(f"포지션 조회 시도 - 심볼: {self.symbol}")  # 반복 로그 주석처리
            
            # 현재 포지션 조회
            self.exchange.options['fetchPositions']['method'] = 'privateMixGetV2MixPositionAllPosition'
            positions = self.exchange.fetch_positions()
            # logger.info(f"원본 포지션 데이터: {positions}")  # 반복 로그 주석처리 (포지션 있을 때만 출력)
            
            # # 모든 포지션 조회 (심볼 제한 없이)
            # all_positions = self.exchange.fetch_positions([], {'type': 'future', 'productType': 'USDT-FUTURES', 'marginMode': 'cross', 'marginCoin': 'USDT'})
            # logger.debug(f"모든 포지션 데이터 (크로스): {all_positions}")
            
            # # 격리 마진으로도 시도
            # try:
            #     isolated_positions = self.exchange.fetch_position([], {'type': 'future', 'productType': 'USDT-FUTURES', 'marginMode': 'isolated', 'marginCoin': 'USDT'})
            #     logger.debug(f"모든 포지션 데이터 (격리): {isolated_positions}")
            # except Exception as e:
            #     logger.debug(f"격리 마진 조회 실패: {e}")
            
            # # 다른 상품 타입도 시도
            # try:
            #     coin_positions = self.exchange.fetch_position([], {'type': 'future', 'productType': 'COIN-FUTURES'})
            #     logger.debug(f"모든 포지션 데이터 (COIN): {coin_positions}")
            # except Exception as e:
            #     logger.debug(f"COIN-FUTURES 조회 실패: {e}")
            
            result = []
            # 실제 API에서 가져온 포지션 심볼 집합 (캐시 정리용)
            active_symbols = set()
            
            for pos in positions:
                # logger.debug(f"포지션 분석: {pos}")  # 반복 로그 주석처리
                try:
                    # 포지션 크기 확인 (안전하게)
                    total_size = float(pos.get('total', 0)) if pos.get('total') else 0
                    contracts_size = float(pos.get('contracts', 0)) if pos.get('contracts') else 0
                    position_size = total_size or contracts_size
                    
                    if pos.get('contracts', 0) > 0:  # 포지션이 있는 경우만
                        # symbol 안전하게 처리
                        symbol = pos.get('symbol', 'UNKNOWN')
                        if symbol is None:
                            symbol = 'UNKNOWN'
                        
                        position_info = {
                            'symbol': symbol,
                            'side': pos.get('side'),  # 'long' or 'short'
                            'size': abs(pos.get('contracts', 0)),  # 포지션 크기
                            'entry_price': pos.get('entryPrice'),  # 평균 진입가
                            'liquidation_price': pos.get('liquidationPrice'),  # 청산가
                            'mark_price': pos.get('markPrice'),  # 현재 시장가
                            'unrealized_pnl': pos.get('unrealizedPnl'),  # 미실현 손익
                            'percentage': pos.get('percentage'),  # 수익률
                            'notional': pos.get('notional'),  # 포지션 가치
                            'leverage': pos.get('leverage'),  # 레버리지
                            'margin_mode': pos.get('marginMode'),  # 마진 모드
                            'timestamp': datetime.now(timezone.utc)
                        }
                        result.append(position_info)
                        
                        # 캐시 업데이트 (symbol이 유효한 경우만)
                        if symbol != 'UNKNOWN':
                            self._position_cache[symbol] = position_info
                            active_symbols.add(symbol)
                except Exception as e:
                    # I/O 에러 발생 가능하므로 로그 주석 처리
                    # logger.error(f"포지션 처리 오류: {e}")
                    # logger.error(f"포지션 심볼: {pos.get('symbol')}, contracts: {pos.get('contracts')}")
                    continue
            
            # 캐시에서 실제 API에 없는 포지션 제거 (포지션이 청산되었을 때 캐시 정리)
            if active_symbols:
                # 실제 포지션이 있는 경우, 캐시에서 없는 포지션만 제거
                cache_symbols_to_remove = [sym for sym in self._position_cache.keys() if sym not in active_symbols]
                for sym in cache_symbols_to_remove:
                    del self._position_cache[sym]
            else:
                # 실제 포지션이 하나도 없는 경우, 캐시 전체 정리
                self._position_cache.clear()
            
            # 캐시 업데이트 (포지션 전용 업데이트 시간 사용)
            self._last_position_update = current_time
            
            # 포지션 상태에 따른 체크 활성화/비활성화
            if len(result) == 0:
                # 포지션이 0인 경우: 포지션 0 확인 플래그 설정, 체크 비활성화
                if not self._position_zero_confirmed:
                    logger.info("[포지션 상태] 포지션 0 확인 - 체크 비활성화 (진입 전까지 API 호출 안 함)")
                self._position_zero_confirmed = True
                self._position_zero_confirmed_time = current_time
                self._position_check_enabled = False  # 진입 전까지 체크 비활성화
                self._balance_check_enabled = False  # 잔고 체크도 비활성화
            else:
                # 포지션이 있는 경우: 포지션 0 확인 플래그 해제, 체크 활성화
                if self._position_zero_confirmed:
                    logger.info(f"[포지션 상태] 포지션 진입 감지 ({len(result)}개) - 체크 활성화 (빠른 청산 감지)")
                self._position_zero_confirmed = False
                self._position_check_enabled = True  # 진입 후 빠른 체크 활성화
                self._balance_check_enabled = True  # 잔고 체크도 활성화
            
            # 포지션 변화 감지 (진입/청산 빠른 감지)
            current_state = self._get_position_state_hash(result)
            if self._last_position_state is not None and current_state != self._last_position_state:
                # 포지션 상태가 변경됨 (진입 또는 청산)
                logger.info(f"[포지션 변화 감지] 상태 변경됨")
            self._last_position_state = current_state
            
            # 포지션이 있을 때만 로그 출력
            if result:
                logger.info(f"활성 포지션 발견: {len(result)}개")
            # logger.debug(f"포지션 정보: {result}")  # 반복 로그 주석처리
            return result
            
        except Exception as e:
            # 상세한 에러 정보 로깅
            error_msg = str(e)
            error_type = type(e).__name__
            logger.error(f"포지션 조회 오류 [{error_type}]: {error_msg}")
            
            # ccxt 에러인 경우 추가 정보
            if hasattr(e, 'args') and e.args:
                logger.error(f"에러 상세: {e.args}")
            
            # 에러 발생 시: 캐시가 있으면 반환 (간단한 크기 체크만)
            if self._position_cache and self._last_position_update > 0:
                cache_age = current_time - self._last_position_update
                # 429 에러이거나 캐시가 최근(30초 이내)인 경우만 사용
                is_429_error = '429' in error_msg or 'Too Many Requests' in error_msg
                if is_429_error:
                    self._last_429_error_time = current_time
                
                if is_429_error or cache_age < 30:
                    valid_positions = []
                    for pos in list(self._position_cache.values()):
                        try:
                            size = pos.get('size') or pos.get('contracts', 0)
                            if size and abs(float(size)) >= 0.0001:
                                valid_positions.append(pos)
                        except (ValueError, TypeError):
                            # 잘못된 크기 값은 무시
                            continue
                    if is_429_error:
                        logger.warning(f"429 에러 발생, 캐시 데이터 반환 (유효 포지션: {len(valid_positions)}개)")
                    else:
                        logger.warning(f"에러 발생, 캐시 데이터 반환 (유효 포지션: {len(valid_positions)}개)")
                    return valid_positions
            
            return []
    
    def _get_position_state_hash(self, positions: List[Dict[str, Any]]) -> str:
        """
        포지션 상태 해시 생성 (변화 감지용)
        
        Args:
            positions: 포지션 리스트
        
        Returns:
            str - 포지션 상태 해시
        """
        if not positions:
            return "NO_POSITION"
        
        # 포지션 상태를 간단한 문자열로 변환 (심볼, 사이드, 크기)
        state_parts = []
        for pos in positions:
            symbol = pos.get('symbol', 'UNKNOWN')
            side = pos.get('side', 'UNKNOWN')
            size = pos.get('size', 0)
            state_parts.append(f"{symbol}:{side}:{size:.6f}")
        
        return "|".join(sorted(state_parts))
    
    def invalidate_position_cache(self):
        """
        포지션 캐시 무효화 (주문 체결 시 호출)
        
        주문이 체결되면 포지션이 변경될 수 있으므로 캐시를 무효화하여
        다음 호출 시 최신 데이터를 가져오도록 함
        """
        self._last_position_update = 0
        self._last_position_state = None
        logger.debug("[캐시 무효화] 포지션 캐시 무효화됨 (주문 체결 또는 수동 호출)")
    
    def enable_position_check(self):
        """
        포지션 체크 활성화 (진입 주문 체결 시 호출)
        
        진입 주문이 체결되면 포지션이 생겼을 수 있으므로
        포지션 체크를 활성화하여 빠른 청산 감지 시작
        """
        if not self._position_check_enabled:
            logger.info("[포지션 체크] 진입 감지 - 포지션 체크 활성화")
        self._position_check_enabled = True
        self._balance_check_enabled = True  # 잔고 체크도 활성화
        self._position_zero_confirmed = False  # 포지션 0 확인 플래그 해제
        self._last_position_update = 0  # 캐시 무효화하여 다음 호출 시 최신 데이터 조회
    
    def should_check_position(self) -> bool:
        """
        포지션 체크가 필요한지 확인 (동적 체크 주기)
        
        Returns:
            bool - 체크 필요 여부
        """
        current_time = time.time()
        
        # 캐시가 없으면 체크 필요
        if self._last_position_update == 0:
            return True
        
        # 포지션 상태에 따른 동적 체크 주기
        has_position = len(self._position_cache) > 0
        check_interval = self._position_check_interval_with_position if has_position else self._position_check_interval_no_position
        
        # 마지막 업데이트로부터 경과 시간
        elapsed = current_time - self._last_position_update
        
        return elapsed >= check_interval


    def get_btc_position(self, force_update: bool = False) -> Optional[Dict[str, Any]]:
        """
        BTC 포지션 정보 가져오기
        
        Args:
            force_update: 강제 업데이트 여부
        
        Returns:
            Optional[Dict[str, Any]] - BTC 포지션 정보 또는 None
        """
        positions = self.get_positions(force_update)
        
        for pos in positions:
            if pos['symbol'] == self.symbol:
                return pos
        
        return None
    
    def get_position_summary(self) -> Dict[str, Any]:
        """
        포지션 요약 정보 가져오기
        
        Returns:
            Dict[str, Any] - 포지션 요약
        """
        try:
            balance = self.get_account_balance()
            btc_position = self.get_btc_position()
            
            if btc_position is None:
                return {
                    'has_position': False,
                    'balance': balance,
                    'position': None,
                    'total_value': balance['total']
                }
            
            # 포지션 가치 계산
            position_value = btc_position['size'] * btc_position['mark_price']
            
            return {
                'has_position': True,
                'balance': balance,
                'position': btc_position,
                'position_value': position_value,
                'total_value': balance['total'] + btc_position['unrealized_pnl']
            }
            
        except Exception as e:
            logger.error(f"포지션 요약 조회 오류: {e}")
            return {
                'has_position': False,
                'balance': {'total': 0.0, 'free': 0.0, 'used': 0.0},
                'position': None,
                'total_value': 0.0
            }
    
    def sync_with_risk_manager(self, risk_manager) -> Dict[str, Any]:
        """
        리스크 매니저와 계정 정보 동기화
        
        Args:
            risk_manager: BitgetRiskManager 인스턴스
        
        Returns:
            Dict[str, Any] - 동기화 결과
        """
        try:
            summary = self.get_position_summary()
            
            if not summary['has_position']:
                # 포지션이 없는 경우 리스크 매니저 초기화
                risk_manager.close_all_positions()
                return {
                    'synced': True,
                    'action': 'cleared',
                    'message': '포지션이 없어 리스크 매니저를 초기화했습니다.'
                }
            
            position = summary['position']
            
            # 포지션 정보 추출
            side = position['side']
            size = position['size']
            entry_price = position['entry_price']
            
            # 리스크 매니저에 포지션 정보 설정
            # (실제 거래소의 포지션을 리스크 매니저에 반영)
            risk_manager.avg_calculator.close_all_positions()  # 기존 포지션 초기화
            
            # 포지션 추가 (비트겟 방식으로)
            if side == 'long':
                # 롱 포지션
                avg_price = risk_manager.avg_calculator.add_position(entry_price, size)
            else:
                # 숏 포지션 (음수로 처리)
                avg_price = risk_manager.avg_calculator.add_position(entry_price, -size)
            
            # 포지션 정보 업데이트
            risk_manager.position_size = abs(size)
            risk_manager.position_value = abs(size) * avg_price
            risk_manager.actual_leverage = risk_manager.position_value / risk_manager.initial_capital
            
            # 청산가 계산
            risk_manager.liquidation_price = risk_manager._calculate_liquidation_price(avg_price)
            
            # 리스크 레벨 계산
            risk_manager.risk_level = risk_manager._calculate_risk_level(position['mark_price'])
            
            logger.info(f"리스크 매니저 동기화 완료 - 포지션: {side} {size} @ {avg_price}")
            
            return {
                'synced': True,
                'action': 'updated',
                'position_info': {
                    'side': side,
                    'size': size,
                    'entry_price': entry_price,
                    'average_price': avg_price,
                    'mark_price': position['mark_price'],
                    'unrealized_pnl': position['unrealized_pnl']
                }
            }
            
        except Exception as e:
            logger.error(f"리스크 매니저 동기화 오류: {e}")
            return {
                'synced': False,
                'error': str(e)
            }
    
    def get_trading_fees(self) -> Dict[str, float]:
        """
        거래 수수료 정보 가져오기
        
        Returns:
            Dict[str, float] - 수수료 정보
        """
        try:
            markets = self.exchange.load_markets()
            market = markets.get(self.symbol)
            
            if market:
                return {
                    'maker': market.get('maker', 0.0),
                    'taker': market.get('taker', 0.0),
                    'symbol': self.symbol
                }
            else:
                return {
                    'maker': 0.001,  # 기본값
                    'taker': 0.001,
                    'symbol': self.symbol
                }
                
        except Exception as e:
            logger.error(f"수수료 정보 조회 오류: {e}")
            return {
                'maker': 0.001,
                'taker': 0.001,
                'symbol': self.symbol
            }
    
    def is_connected(self) -> bool:
        """API 연결 상태 확인"""
        try:
            logger.debug("API 연결 상태 확인 중...")
            balance = self.exchange.fetch_balance({'type': 'future', 'marginMode': 'cross', 'productType': 'USDT-FUTURES', 'marginCoin': 'USDT'})
            usdt_balance = balance.get('USDT', {}).get('total', 0)
            logger.debug(f"연결 상태: 정상 - USDT 잔고: {usdt_balance:.2f}")
            return True
        except Exception as e:
            logger.warning(f"API 연결 상태 확인 실패: {e}")
            return False


# 테스트용 메인 함수
if __name__ == "__main__":
    print("비트겟 계정 매니저 테스트")
    
    try:
        # 계정 매니저 생성
        account_manager = BitgetAccountManager(testnet=False)
        
        # 연결 상태 확인
        print(f"API 연결 상태: {account_manager.is_connected()}")
        
        # 잔고 조회
        print("\n=== 잔고 조회 ===")
        balance = account_manager.get_account_balance()
        print(f"잔고: {balance}")
        
        # 포지션 조회
        print("\n=== 포지션 조회 ===")
        positions = account_manager.get_positions(force_update=True)
        print(f"포지션: {positions}")
        
        # BTC 포지션 조회
        print("\n=== BTC 포지션 조회 ===")
        btc_position = account_manager.get_btc_position()
        print(f"BTC 포지션: {btc_position}")
        
        # 포지션 요약
        print("\n=== 포지션 요약 ===")
        summary = account_manager.get_position_summary()
        print(f"요약: {summary}")
        
        # 수수료 정보
        print("\n=== 수수료 정보 ===")
        fees = account_manager.get_trading_fees()
        print(f"수수료: {fees}")
        
    except Exception as e:
        print(f"테스트 오류: {e}")
