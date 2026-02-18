"""
비트겟 실시간 데이터 수신 클라이언트

WebSocket을 사용하여 실시간 데이터를 수신:
- 1분봉 kline 데이터 (완성된 봉만 처리)
- 거래 체결 데이터 (선택적)
"""
import json
import threading
import time
import logging
import websocket
import requests
import pandas as pd
import socket
from datetime import datetime, timezone, timedelta
from typing import Callable, Optional, Dict, Any
import queue
import os
from dotenv import load_dotenv

# 로거 설정
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class BitgetRealtimeClient:
    """
    비트겟 실시간 데이터 수신 클라이언트
    
    WebSocket을 사용하여 실시간 데이터를 수신하고 콜백 함수를 통해 처리합니다.
    """
    
    def __init__(self, symbol='BTCUSDT', testnet=False):
        """
        초기화
        
        Args:
            symbol: 거래 심볼 (기본값: BTCUSDT)
            testnet: 테스트넷 사용 여부 (비트겟은 테스트넷 지원)
        """
        # .env 파일 경로 설정 (bitget 디렉토리 기준)
        env_path = os.path.join(os.path.dirname(__file__), '.env')
        load_dotenv(env_path)
        
        self.symbol = symbol
        self.testnet = testnet
        
        # API 키 설정 (선택적 - 공개 데이터는 API 키 없이도 접근 가능)
        self.api_key = os.getenv('BITGET_API_KEY_SUB')
        self.secret_key = os.getenv('BITGET_SECRET_KEY_SUB')
        self.passphrase = os.getenv('BITGET_PASSPHRASE')
        
        # WebSocket URL 설정 (v2 퍼블릭 API 사용)
        if testnet:
            self.ws_url = "wss://ws-sandbox.bitget.com/v2/ws/public"
        else:
            self.ws_url = "wss://ws.bitget.com/v2/ws/public"
        
        # 데이터 큐
        self.kline_queue = queue.Queue()
        self.trade_queue = queue.Queue()
        
        # 중복 데이터 방지
        self.last_kline_time = None
        
        # 연결 상태
        self.is_connected = False
        self.ws = None
        self.last_pong_time = None  # pong 수신 시간 추적
        self.last_ping_time = None  # ping 전송 시간 추적
        
        # 콜백 함수들
        self.kline_callback = None
        self.trade_callback = None
        
        # Rate Limit 모니터링
        self.message_count = 0
        self.last_reset_time = time.time()
        self.rate_limit_window = 1.0  # 1초 윈도우
        self.max_messages_per_second = 8  # 안전 마진 (10개 제한 중 8개)
        
        # 스레드 관리
        self.stop_event = threading.Event()
        self.ws_thread = None
        self.heartbeat_thread = None
    
    def _check_rate_limit(self):
        """Rate Limit 체크 (초당 10개 메시지 제한)"""
        current_time = time.time()
        
        # 1초 윈도우 리셋
        if current_time - self.last_reset_time >= self.rate_limit_window:
            self.message_count = 0
            self.last_reset_time = current_time
        
        # Rate Limit 체크
        if self.message_count >= self.max_messages_per_second:
            wait_time = self.rate_limit_window - (current_time - self.last_reset_time)
            if wait_time > 0:
                logger.warning(f"Rate Limit 도달 - {wait_time:.2f}초 대기")
                time.sleep(wait_time)
                self.message_count = 0
                self.last_reset_time = time.time()
        
        self.message_count += 1
    
    def set_kline_callback(self, callback: Callable[[Dict], None]):
        """
        1분봉 데이터 콜백 함수 설정
        
        Args:
            callback: 1분봉 데이터를 처리할 콜백 함수
        """
        self.kline_callback = callback
        try:
            cb_name = getattr(callback, '__qualname__', getattr(callback, '__name__', str(callback)))
        except Exception:
            cb_name = str(callback)
        logger.info(f"1분봉 데이터 콜백 함수 설정 완료 -> {cb_name}")
    
    def set_trade_callback(self, callback: Callable[[Dict], None]):
        """
        거래 체결 데이터 콜백 함수 설정
        
        Args:
            callback: 거래 체결 데이터를 처리할 콜백 함수
        """
        self.trade_callback = callback
        logger.info("거래 체결 데이터 콜백 함수 설정 완료")
    
    def _on_message(self, ws, message):
        """WebSocket 메시지 처리 (비트겟 공식 문서 기반)"""
        try:
            # 메시지 수신 로그 (필요시만 활성화)
            # logger.info(f"[WS<-] 메시지 수신: {message[:200] if message else 'None'}")
            # 어떤 메시지이든 수신했으면 타임스탬프 갱신 (무응답 오탐 방지)
            self.last_message_time = time.time()
            # 빈 메시지 체크
            if not message or message.strip() == '':
                logger.debug("빈 메시지 수신 - 무시")
                return
            
            # Pong 응답 체크 (JSON이 아닌 경우)
            if message.strip() == 'pong':
                logger.info("[PONG] Pong 수신 - 연결 유지")
                self.last_pong_time = time.time()  # pong 수신 시간 기록
                return
            
            # JSON 파싱 시도
            try:
                data = json.loads(message)
                # logger.info(f"[WS] JSON 파싱 성공 - 키들: {list(data.keys()) if isinstance(data, dict) else 'Not dict'}")
            except json.JSONDecodeError as json_error:
                logger.warning(f"JSON 파싱 실패: {json_error} - 메시지: {message[:100]}")
                return
            
            # 공통 키 로깅 (구조 확인용)
            # try:
            #     top_keys = list(data.keys())[:6]
            #     # logger.info(f"[WS] message keys={top_keys}")
            # except Exception:
            #     pass
            
            # 구독 확인 (필요시만 활성화)
            if data.get('event') == 'subscribe':
                # logger.info(f"[WS] 구독 응답: {data}")
                return
            
            # Pong 응답 처리 (하트비트 확인)
            if data.get('event') == 'pong':
                logger.info("[PONG] Pong 수신 (JSON) - 연결 유지")
                self.last_pong_time = time.time()  # pong 수신 시간 기록
                return
            
            # 메시지 타입 확인 (필요시만 활성화)
            if 'arg' in data:
                arg = data['arg']
                channel = arg.get('channel', '')
                payload_len = -1
                if 'data' in data:
                    try:
                        payload_len = len(data.get('data') or [])
                    except Exception:
                        payload_len = -1
                # logger.info(f"[WS] 채널: {channel}, 데이터 길이: {payload_len}")
                
                if channel == 'candle1m' and 'data' in data:
                    # logger.info(f"[WS] 1분봉 데이터 처리 시작: {payload_len}개")
                    self._handle_kline_message(data)
                elif channel == 'trade' and 'data' in data:
                    # logger.info(f"[WS] 거래 데이터 처리 시작: {payload_len}개")
                    self._handle_trade_message(data)
                else:
                    if channel:
                        # logger.info(f"[WS] 알 수 없는 채널: {channel}, 데이터 길이: {payload_len}")
                        pass
            else:
                # logger.info(f"[WS] arg가 없는 메시지: {data}")
                pass
                    
        except Exception as e:
            logger.error(f"WebSocket 메시지 처리 오류: {e}")
            logger.error(f"메시지 내용: {message[:200] if message else 'None'}")
    
    def _handle_kline_message(self, data):
        """1분봉 데이터 메시지 처리"""
        try:
            arg = data['arg']
            kline_data_list = data['data']
            
            # 수신된 메시지 로그 (연결 상태 모니터링)
            current_time = datetime.now(timezone.utc)
            self.last_message_time = time.time()  # 마지막 메시지 시간 업데이트
            # logger.info(f"[DATA] 1분봉 메시지 수신: {len(kline_data_list)}개 봉 - {current_time.strftime('%H:%M:%S')}")
            
            # 각 kline 데이터 처리
            for kline in kline_data_list:
                # 비트겟 kline 데이터 구조
                # [timestamp, open, high, low, close, volume, volCcy]
                timestamp_ms = int(kline[0])
                open_price = float(kline[1])
                high_price = float(kline[2])
                low_price = float(kline[3])
                close_price = float(kline[4])
                volume = float(kline[5])
                
                # datetime 객체로 변환
                kline_time = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
                normalized_time = kline_time.replace(second=0, microsecond=0)  # 분 정각으로 정규화
                
                # 현재 시간과 비교하여 완성된 봉만 처리
                current_time = datetime.now(timezone.utc)
                current_minute = current_time.replace(second=0, microsecond=0)
                
                # logger.info(f"[필터링] 봉 시간: {normalized_time}, 현재 분: {current_minute}")
                
                # 실시간 데이터 처리: 현재 분과 이전 분의 봉 모두 처리
                # 하지만 완성된 봉의 지표 계산을 위해서는 완성된 봉만 처리
                one_minute_ago = current_minute - timedelta(minutes=1)
                
                # 완성된 봉만 처리 (현재 분보다 이전의 봉만)
                if normalized_time >= current_minute:
                    # 현재 분의 봉은 아직 완성되지 않았으므로 제외
                    continue
                
                # 너무 오래된 데이터는 제외 (1시간 이내만)
                one_hour_ago = current_minute - timedelta(hours=1)
                if normalized_time < one_hour_ago:
                    continue
                
                # logger.info(f"[필터링] 봉 처리 승인: {normalized_time}")
                
                # 중복 데이터 필터링 (같은 분의 데이터는 한 번만 처리)
                if self.last_kline_time == normalized_time:
                    # logger.info(f"[필터링] 중복 데이터 제외: {normalized_time} == {self.last_kline_time}")
                    continue
                
                self.last_kline_time = normalized_time
                
                # 1분봉 데이터 구성 (완성된 봉만 처리)
                kline_data = {
                    'timestamp': normalized_time,
                    'end_time': kline_time,
                    'open': open_price,
                    'high': high_price,
                    'low': low_price,
                    'close': close_price,
                    'volume': volume,
                    'is_closed': True  # 비트겟은 완성된 봉만 전송
                }
                
                # 큐에 추가
                self.kline_queue.put(kline_data)
                
                # 콜백 함수 호출
                if self.kline_callback:
                    try:
                        # logger.info(f"[CALLBACK] 콜백 함수 호출 시작: {kline_data['timestamp']}")
                        self.kline_callback(kline_data)
                        # logger.info(f"[CALLBACK] candle1m -> {kline_data['timestamp']} close={close_price:.2f}")
                    except Exception as cb_err:
                        logger.error(f"kline 콜백 실행 오류: {cb_err}")
                        logger.error(f"콜백 데이터: {kline_data}")
                        import traceback
                        logger.error(f"콜백 오류 상세: {traceback.format_exc()}")
                else:
                    logger.warning("[CALLBACK] kline_callback이 설정되지 않음")
                
        except Exception as e:
            logger.error(f"1분봉 데이터 처리 오류: {e}")
    
    def _handle_trade_message(self, data):
        """거래 체결 데이터 메시지 처리"""
        try:
            # 디버깅: 수신된 데이터 구조 확인
            logger.debug(f"거래 데이터 수신: {data}")
            
            arg = data['arg']
            trade_data_list = data['data']
            
            # 각 거래 데이터 처리
            for trade in trade_data_list:
                # 비트겟 v2 API 필드명 확인 및 처리
                price_field = trade.get('px') or trade.get('price') or trade.get('p')
                quantity_field = trade.get('sz') or trade.get('size') or trade.get('q')
                time_field = trade.get('ts') or trade.get('time') or trade.get('t')
                
                trade_data = {
                    'symbol': arg.get('instId', self.symbol),
                    'price': float(price_field) if price_field else 0.0,
                    'quantity': float(quantity_field) if quantity_field else 0.0,
                    'trade_time': int(time_field) if time_field else 0,
                    'timestamp': datetime.fromtimestamp(int(time_field) / 1000, tz=timezone.utc) if time_field else datetime.now(timezone.utc),
                    'side': trade.get('side', trade.get('S', 'unknown'))
                }
                
                # 큐에 추가
                self.trade_queue.put(trade_data)
                
                # 콜백 함수 호출
                if self.trade_callback:
                    self.trade_callback(trade_data)
                
                logger.debug(f"거래 체결: {trade_data['timestamp']} - 가격:{trade_data['price']:.2f} 수량:{trade_data['quantity']:.6f}")
                
        except Exception as e:
            logger.error(f"거래 체결 데이터 처리 오류: {e}")
    
    def _on_error(self, ws, error):
        """WebSocket 오류 처리"""
        logger.error(f"WebSocket 오류: {error}")
    
    def _on_close(self, ws, close_status_code, close_msg):
        """WebSocket 연결 종료 처리 (상세 로깅)"""
        logger.info(f"WebSocket 연결 종료: {close_status_code} - {close_msg}")
        
        # 종료 코드별 상세 로깅
        if close_status_code == 1006:
            logger.warning("비정상 종료 (1006) - 네트워크 문제 가능성")
        elif close_status_code == 1000:
            logger.info("정상 종료 (1000)")
        elif close_status_code == 1001:
            logger.info("서버 종료 (1001)")
        elif close_status_code == 1002:
            logger.error("프로토콜 오류 (1002)")
        elif close_status_code == 1003:
            logger.error("지원하지 않는 데이터 타입 (1003)")
        elif close_status_code == 1004:
            logger.error("예약됨 (1004)")
        elif close_status_code == 1005:
            logger.warning("상태 코드 없음 (1005)")
        elif close_status_code == 1007:
            logger.error("데이터 타입 오류 (1007)")
        elif close_status_code == 1008:
            logger.error("정책 위반 (1008)")
        elif close_status_code == 1009:
            logger.error("메시지가 너무 큼 (1009)")
        elif close_status_code == 1010:
            logger.error("확장 협상 실패 (1010)")
        elif close_status_code == 1011:
            logger.error("서버 오류 (1011)")
        elif close_status_code == 1012:
            logger.error("서버 재시작 (1012)")
        elif close_status_code == 1013:
            logger.error("서버 과부하 (1013)")
        elif close_status_code == 1014:
            logger.error("게이트웨이 오류 (1014)")
        elif close_status_code == 1015:
            logger.error("TLS 핸드셰이크 실패 (1015)")
        
        self.is_connected = False
        
        # 하트비트 스레드 정리
        if hasattr(self, 'heartbeat_thread') and self.heartbeat_thread is not None and self.heartbeat_thread.is_alive():
            logger.info("하트비트 스레드 정리")
            # 하트비트 스레드는 daemon=True이므로 자동 종료됨
    
    def _on_open(self, ws):
        """WebSocket 연결 시작 처리 (비트겟 공식 문서 기반)"""
        logger.info("[OK] WebSocket 연결 성공")
        self.is_connected = True
        self.last_message_time = time.time()  # 마지막 메시지 시간 초기화
        self.last_pong_time = time.time()  # pong 응답 시간 초기화
        
        # 하트비트 스레드 시작 (연결 성공 시)
        if not hasattr(self, 'heartbeat_thread') or self.heartbeat_thread is None or not self.heartbeat_thread.is_alive():
            self.heartbeat_thread = threading.Thread(target=self._heartbeat_thread, daemon=True)
            self.heartbeat_thread.start()
            logger.debug("하트비트 스레드 시작")
        
        # 1분봉 캔들 구독 (v2 API 형식) - 완성된 봉만
        kline_subscribe = {
            "op": "subscribe",
            "args": [{
                "instType": "USDT-FUTURES",
                "channel": "candle1m",
                "instId": self.symbol
            }]
        }
        ws.send(json.dumps(kline_subscribe))
        logger.info(f"1분봉 캔들 구독: {self.symbol}")
        
        # Rate Limit 관리를 위한 100ms 지연
        time.sleep(0.1)
        
        # 거래 체결 데이터 구독 (선택적)
        trade_subscribe = {
            "op": "subscribe",
            "args": [{
                "instType": "USDT-FUTURES",
                "channel": "trade",
                "instId": self.symbol
            }]
        }
        ws.send(json.dumps(trade_subscribe))
        logger.info(f"거래 체결 데이터 구독: {self.symbol}")
    
    def _websocket_thread(self):
        """WebSocket 스레드 (개선)"""
        reconnect_count = 0
        max_reconnect = 10
        heartbeat_thread = None
        
        while not self.stop_event.is_set():
            try:
                logger.info(f"WebSocket 연결 시도... (시도 {reconnect_count + 1}/{max_reconnect})")
                
                # 기존 연결 정리
                if self.ws:
                    try:
                        self.ws.close()
                    except:
                        pass
                    self.ws = None
                
                self.ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                    on_open=self._on_open
                )
                
                self.is_connected = True
                reconnect_count = 0  # 연결 성공 시 카운트 리셋
                
                self.ws.run_forever(ping_interval=0, ping_timeout=None)  # 라이브러리 내장 ping 비활성화
                
            except Exception as e:
                logger.error(f"WebSocket 연결 오류: {e}")
                self.is_connected = False
                
                # 하트비트 스레드 정리
                if heartbeat_thread and heartbeat_thread.is_alive():
                    heartbeat_thread.join(timeout=1)
                
                if not self.stop_event.is_set():
                    reconnect_count += 1
                    if reconnect_count >= max_reconnect:
                        logger.error(f"최대 재연결 시도 횟수({max_reconnect}) 초과. 연결 중단.")
                        break
                    
                    # 지수 백오프: 5초, 10초, 20초, 40초, 60초... (최대 60초)
                    wait_time = min(5 * (2 ** (reconnect_count - 1)), 60)
                    logger.info(f"{wait_time}초 후 재연결 시도... (시도 {reconnect_count}/{max_reconnect})")
                    time.sleep(wait_time)
    
    def _heartbeat_thread(self):
        """하트비트 스레드 (강화된 연결 모니터링)"""
        while not self.stop_event.is_set():
            try:
                # 연결 상태 및 WebSocket 객체 확인
                if not self.is_connected or not self.ws:
                    logger.debug("WebSocket 연결이 끊어져 하트비트 중단")
                    break
                
                # WebSocket 소켓 상태 확인
                if not hasattr(self.ws, 'sock') or not self.ws.sock:
                    logger.debug("WebSocket 소켓이 없어 하트비트 중단")
                    break
                
                # 마지막 메시지 수신 시간 체크 (2분 이상 메시지가 없으면 재연결)
                if self.last_message_time is not None:
                    time_since_last_message = time.time() - self.last_message_time
                    if time_since_last_message > 120:  # 2분
                        logger.warning(f"메시지 수신 타임아웃 ({time_since_last_message:.1f}초) - 재연결 필요")
                        self.is_connected = False
                        break
                
                # pong 응답 타임아웃 체크 (35초 이상 pong이 없으면 재연결)
                if self.last_pong_time is not None:
                    time_since_last_pong = time.time() - self.last_pong_time
                    if time_since_last_pong > 35:
                        logger.warning(f"pong 응답 타임아웃 ({time_since_last_pong:.1f}초) - 재연결 필요")
                        self.is_connected = False
                        break
                
                try:
                    # Rate Limit 체크
                    self._check_rate_limit()
                    
                    # 비트겟 공식: 단순 "ping" 문자열 전송 (JSON 아님)
                    self.ws.send("ping")
                    self.last_ping_time = time.time()  # ping 전송 시간 기록
                    logger.info("[PING] WebSocket ping 전송")
                except Exception as send_error:
                    logger.warning(f"하트비트 전송 실패: {send_error}")
                    self.is_connected = False
                    break
                
                # 30초 대기 (비트겟 권장 간격)
                time.sleep(30)
                
            except Exception as e:
                logger.error(f"하트비트 스레드 오류: {e}")
                self.is_connected = False
                break
    
    def start(self):
        """실시간 데이터 수신 시작"""
        if self.is_connected:
            logger.warning("이미 연결되어 있습니다.")
            return
        
        try:
            # WebSocket 스레드 시작
            self.ws_thread = threading.Thread(target=self._websocket_thread, daemon=True)
            self.ws_thread.start()
            
            # 연결 대기
            timeout = 10
            start_time = time.time()
            while not self.is_connected and (time.time() - start_time) < timeout:
                time.sleep(0.1)
            
            if not self.is_connected:
                raise Exception("WebSocket 연결 시간 초과")
            
            logger.info("실시간 데이터 수신 시작 완료")
            
        except Exception as e:
            logger.error(f"실시간 데이터 수신 시작 오류: {e}")
            raise
    
    def get_connection_status(self):
        """WebSocket 연결 상태 반환"""
        return {
            'is_connected': self.is_connected,
            'last_message_time': self.last_message_time,
            'last_pong_time': self.last_pong_time,
            'last_ping_time': getattr(self, 'last_ping_time', None)
        }
    
    def stop(self):
        """실시간 데이터 수신 중지"""
        if not self.is_connected:
            logger.warning("연결되어 있지 않습니다.")
            return
        
        try:
            # 스레드 중지
            self.stop_event.set()
            
            # WebSocket 연결 종료
            if self.ws:
                self.ws.close()
            
            # 스레드 종료 대기
            if self.ws_thread and self.ws_thread.is_alive():
                self.ws_thread.join(timeout=5)
            
            self.is_connected = False
            logger.info("실시간 데이터 수신 중지 완료")
            
        except Exception as e:
            logger.error(f"실시간 데이터 수신 중지 오류: {e}")
    
    def get_kline_data(self, timeout=1.0):
        """
        1분봉 데이터 가져오기
        
        Args:
            timeout: 타임아웃 시간 (초)
            
        Returns:
            Dict: 1분봉 데이터 또는 None
        """
        try:
            return self.kline_queue.get(timeout=timeout)
        except queue.Empty:
            return None
    
    def get_trade_data(self, timeout=1.0):
        """
        거래 체결 데이터 가져오기
        
        Args:
            timeout: 타임아웃 시간 (초)
            
        Returns:
            Dict: 거래 체결 데이터 또는 None
        """
        try:
            return self.trade_queue.get(timeout=timeout)
        except queue.Empty:
            return None
    
    def is_running(self):
        """실행 상태 확인"""
        return self.is_connected
    
    def fetch_historical_klines(self, symbol: str, interval: str = '1m', limit: int = 1000, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None) -> Optional[pd.DataFrame]:
        """
        과거 kline 데이터 가져오기 (REST API)
        
        Args:
            symbol: 거래 심볼
            interval: 시간 간격 (1m, 5m, 15m, 30m, 1H, 4H, 1D)
            limit: 가져올 데이터 수 (최대 1000)
            start_time: 시작 시간 (선택사항, 지정 시 해당 시간 이전 데이터 가져옴)
            end_time: 종료 시간 (선택사항, 지정 시 해당 시간 이전 데이터 가져옴)
            
        Returns:
            pd.DataFrame: 과거 kline 데이터 또는 None
        """
        try:
            import pandas as pd
            
            # 비트겟 선물 API 엔드포인트 (v2)
            url = "https://api.bitget.com/api/v2/mix/market/candles"
            
            params = {
                'symbol': symbol,
                'granularity': interval,
                'limit': min(limit, 1000),
                'productType': 'usdt-futures'
            }
            
            # startTime과 endTime 파라미터 추가 (밀리초 단위)
            if end_time:
                params['endTime'] = int(end_time.timestamp() * 1000)
            if start_time:
                params['startTime'] = int(start_time.timestamp() * 1000)
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code != 200:
                logger.error(f"비트겟 API HTTP 오류: {response.status_code} - {response.text}")
                response.raise_for_status()
            
            data = response.json()
            
            if data.get('code') != '00000':
                logger.error(f"비트겟 API 오류: {data.get('msg', 'Unknown error')}")
                return None
            
            klines = data.get('data', [])
            
            if not klines:
                logger.warning("가져온 kline 데이터가 없습니다.")
                return None
            
            # DataFrame 생성
            df_data = []
            for kline in klines:
                # 비트겟 kline 데이터 구조: [timestamp, open, high, low, close, volume, volCcy]
                df_data.append({
                    'timestamp': datetime.fromtimestamp(int(kline[0]) / 1000, tz=timezone.utc),
                    'open': float(kline[1]),
                    'high': float(kline[2]),
                    'low': float(kline[3]),
                    'close': float(kline[4]),
                    'volume': float(kline[5])
                })
            
            df = pd.DataFrame(df_data)
            df.set_index('timestamp', inplace=True)
            
            # Bitget API는 최신 데이터를 먼저 반환할 수 있으므로, 첫 번째와 마지막 타임스탬프 확인
            if not df.empty:
                first_timestamp = df.index[0]
                last_timestamp = df.index[-1]
                # 첫 번째가 마지막보다 크면 역순으로 정렬되어 있음
                # if first_timestamp > last_timestamp:
                #     logger.debug(f"[fetch_historical_klines] 데이터가 역순으로 정렬되어 있음. 첫 번째={first_timestamp}, 마지막={last_timestamp}. 정렬 중...")
            df.sort_index(inplace=True)
            
            # 최신 봉과 가장 오래된 봉 시간 로깅
            if not df.empty:
                latest_time = df.index[-1]
                oldest_time = df.index[0]
                current_utc = datetime.now(timezone.utc)
                time_diff_minutes = (current_utc - latest_time).total_seconds() / 60
                logger.info(f"[fetch_historical_klines] {symbol}@{interval}: 최신 봉={latest_time}, 가장 오래된 봉={oldest_time}, 총 {len(df)}개, 현재 시간과 차이={time_diff_minutes:.1f}분")
            
            return df
            
        except Exception as e:
            logger.error(f"과거 데이터 가져오기 오류: {e}")
            return None


# 테스트용 메인 함수
if __name__ == "__main__":
    # 콘솔 로깅 활성화 (standalone 테스트 시 INFO 로그 출력)
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s][%(levelname)s][%(name)s] %(message)s'
    )
    # def kline_callback(data):
        # print(f"1분봉 데이터: {data['timestamp']} - Close: {data['close']:.2f} - Closed: {data['is_closed']}")
    
    # def trade_callback(data):
    #     print(f"거래 체결: {data['timestamp']} - 가격: {data['price']:.2f}")
    
    # 클라이언트 생성 및 시작
    client = BitgetRealtimeClient(symbol='BTCUSDT', testnet=False)
    # client.set_kline_callback(kline_callback)
    # client.set_trade_callback(trade_callback)

    df = client.fetch_historical_klines(symbol='BTCUSDT', interval='1m', limit=1000)
    print(df)
    
    try:
        client.start()
        
        # 60초간 실행
        time.sleep(360)
        
    except KeyboardInterrupt:
        print("사용자에 의해 중단됨")
    finally:
        client.stop()
