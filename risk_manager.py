"""
비트겟 리스크 관리 모듈

비트겟 평균가 및 청산가 계산
마틴게일 전략의 리스크 관리
실시간 포지션 모니터링
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple, List
import logging
from datetime import datetime, timezone
import threading

# 로거 설정
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

class BitgetAveragePriceCalculator:
    """
    비트겟 평균가 계산기
    
    비트겟의 실제 평균가 계산 방식을 정확히 구현합니다.
    """
    
    def __init__(self):
        """초기화"""
        self.total_cost = 0.0  # 총 매수 금액
        self.total_quantity = 0.0  # 총 매수 수량
        self.entry_count = 0  # 진입 횟수
        self.emergency_position_size = 0.0  # 긴급 물타기 수량
        
        # 스레드 안전성을 위한 락
        self.lock = threading.Lock()
        
        logger.info("비트겟 평균가 계산기 초기화 완료")

    def add_position(self, price: float, quantity: float) -> float:
        """
        포지션 추가 (비트겟 방식)
        
        Args:
            price: 진입 가격
            quantity: 진입 수량
        
        Returns:
            float - 새로운 평균가
        """
        with self.lock:
            if quantity <= 0:
                logger.warning(f"잘못된 수량: {quantity}")
                return self.get_average_price()
            
            # 비트겟 평균가 계산
            new_cost = price * quantity
            self.total_cost += new_cost
            self.total_quantity += quantity
            self.entry_count += 1
            
            new_avg_price = self.total_cost / self.total_quantity
            
            logger.debug(f"포지션 추가 - 가격: {price:.2f}, 수량: {quantity:.6f}, 평균가: {new_avg_price:.2f}")
            
            return new_avg_price

    def remove_position(self, quantity: float) -> float:
        """
        포지션 제거 (비트겟 방식)
        
        Args:
            quantity: 제거할 수량
        
        Returns:
            float - 새로운 평균가
        """
        with self.lock:
            if quantity <= 0 or quantity > self.total_quantity:
                logger.warning(f"잘못된 제거 수량: {quantity}, 현재 수량: {self.total_quantity}")
                return self.get_average_price()
            
            # 비트겟 방식: 평균가 유지, 수량만 감소
            current_avg = self.get_average_price()
            removed_cost = current_avg * quantity
            
            self.total_cost -= removed_cost
            self.total_quantity -= quantity
            
            new_avg_price = self.total_cost / self.total_quantity if self.total_quantity > 0 else 0.0
            
            logger.debug(f"포지션 제거 - 수량: {quantity:.6f}, 평균가: {new_avg_price:.2f}")
            
            return new_avg_price

    def get_average_price(self) -> float:
        """현재 평균가 반환"""
        with self.lock:
            if self.total_quantity <= 0:
                return 0.0
            return self.total_cost / self.total_quantity

    def get_total_quantity(self) -> float:
        """총 수량 반환"""
        with self.lock:
            return self.total_quantity

    def get_total_value(self) -> float:
        """총 포지션 가치 반환"""
        with self.lock:
            return self.total_cost

    def get_entry_count(self) -> int:
        """진입 횟수 반환"""
        with self.lock:
            return self.entry_count

    def set_emergency_position_size(self, size: float):
        """긴급 물타기 수량 설정"""
        with self.lock:
            self.emergency_position_size = size
            logger.debug(f"긴급 물타기 수량 설정: {size:.6f}")

    def get_emergency_position_size(self) -> float:
        """긴급 물타기 수량 반환"""
        with self.lock:
            return self.emergency_position_size

    def clear_emergency_position_size(self):
        """긴급 물타기 수량 초기화"""
        with self.lock:
            self.emergency_position_size = 0.0

    def close_all_positions(self):
        """모든 포지션 청산"""
        with self.lock:
            self.total_cost = 0.0
            self.total_quantity = 0.0
            self.entry_count = 0
            self.emergency_position_size = 0.0
            logger.info("모든 포지션 청산 완료")

    def calculate_pnl(self, current_price: float) -> Dict[str, float]:
        """
        손익 계산
        
        Args:
            current_price: 현재 가격
        
        Returns:
            Dict[str, float] - 손익 정보
        """
        with self.lock:
            if self.total_quantity <= 0:
                return {
                    'unrealized_pnl': 0.0,
                    'unrealized_pnl_percent': 0.0,
                    'total_value': 0.0,
                    'current_value': 0.0
                }
            
            current_value = self.total_quantity * current_price
            unrealized_pnl = current_value - self.total_cost
            unrealized_pnl_percent = (unrealized_pnl / self.total_cost) * 100 if self.total_cost > 0 else 0.0
            
            return {
                'unrealized_pnl': unrealized_pnl,
                'unrealized_pnl_percent': unrealized_pnl_percent,
                'total_value': self.total_cost,
                'current_value': current_value
            }

    def get_status(self) -> Dict[str, any]:
        """현재 상태 반환"""
        with self.lock:
            return {
                'total_cost': self.total_cost,
                'total_quantity': self.total_quantity,
                'average_price': self.get_average_price(),
                'entry_count': self.entry_count,
                'emergency_position_size': self.emergency_position_size
            }

class BitgetRiskManager:
    """
    비트겟 리스크 관리 클래스
    
    비트겟 청산가 계산 및 리스크 모니터링
    """
    
    def __init__(self, initial_capital: float = 5000.0, leverage: float = 5.0):
        """
        초기화
        
        Args:
            initial_capital: 초기 자본
            leverage: 레버리지
        """
        self.initial_capital = initial_capital
        self.leverage = leverage
        self.maintenance_margin = 0.005  # 비트겟 유지 마진 0.5%
        
        # 평균가 계산기
        self.avg_calculator = BitgetAveragePriceCalculator()
        
        # 포지션 추적
        self.position_size = 0.0
        self.position_value = 0.0
        
        # 리스크 모니터링
        self.liquidation_price = None
        self.actual_leverage = 0.0
        self.risk_level = 'LOW'  # LOW, MEDIUM, HIGH, CRITICAL
        
        # 스레드 안전성
        self.lock = threading.Lock()
        
        logger.info(f"비트겟 리스크 매니저 초기화 완료 - 초기자본: {initial_capital}, 레버리지: {leverage}")

    def add_position(self, price: float, quantity: float) -> Dict[str, any]:
        """
        포지션 추가 및 리스크 계산
        
        Args:
            price: 진입 가격
            quantity: 진입 수량
        
        Returns:
            Dict[str, any] - 포지션 정보
        """
        with self.lock:
            # 평균가 계산
            avg_price = self.avg_calculator.add_position(price, quantity)
            
            # 포지션 정보 업데이트
            self.position_size = self.avg_calculator.get_total_quantity()
            self.position_value = self.position_size * avg_price
            
            # 실제 레버리지 계산
            self.actual_leverage = self.position_value / self.initial_capital
            
            # 청산가 계산
            self.liquidation_price = self._calculate_liquidation_price(avg_price)
            
            # 리스크 레벨 계산
            self.risk_level = self._calculate_risk_level(price)
            
            result = {
                'average_price': avg_price,
                'position_size': self.position_size,
                'position_value': self.position_value,
                'actual_leverage': self.actual_leverage,
                'liquidation_price': self.liquidation_price,
                'risk_level': self.risk_level,
                'entry_count': self.avg_calculator.get_entry_count()
            }
            
            logger.info(f"포지션 추가 완료 - {result}")
            return result

    def remove_position(self, quantity: float) -> Dict[str, any]:
        """
        포지션 제거 및 리스크 재계산
        
        Args:
            quantity: 제거할 수량
        
        Returns:
            Dict[str, any] - 포지션 정보
        """
        with self.lock:
            # 평균가 계산
            avg_price = self.avg_calculator.remove_position(quantity)
            
            # 포지션 정보 업데이트
            self.position_size = self.avg_calculator.get_total_quantity()
            self.position_value = self.position_size * avg_price
            
            # 실제 레버리지 계산
            self.actual_leverage = self.position_value / self.initial_capital if self.initial_capital > 0 else 0.0
            
            # 청산가 계산
            self.liquidation_price = self._calculate_liquidation_price(avg_price) if self.position_size > 0 else None
            
            # 리스크 레벨 계산
            self.risk_level = self._calculate_risk_level(avg_price) if self.position_size > 0 else 'LOW'
            
            result = {
                'average_price': avg_price,
                'position_size': self.position_size,
                'position_value': self.position_value,
                'actual_leverage': self.actual_leverage,
                'liquidation_price': self.liquidation_price,
                'risk_level': self.risk_level,
                'entry_count': self.avg_calculator.get_entry_count()
            }
            
            logger.info(f"포지션 제거 완료 - {result}")
            return result

    def _calculate_liquidation_price(self, avg_price: float) -> Optional[float]:
        """
        비트겟 청산가 계산 (더 이상 사용하지 않음)
        
        Args:
            avg_price: 평균가
        
        Returns:
            Optional[float] - 청산가 (실제로는 API에서 제공)
        """
        # 실제 청산가는 비트겟 API에서 제공하므로 계산 불필요
        # 이 함수는 호환성을 위해 유지하지만 실제로는 사용하지 않음
        return None

    def update_from_api_data(self, position_data: Dict[str, any]) -> None:
        """
        API에서 받은 실제 포지션 데이터로 업데이트
        
        Args:
            position_data: 비트겟 API 포지션 데이터
        """
        try:
            # 실제 API 값들 사용
            self.position_size = abs(position_data.get('contracts', 0))
            self.entry_price = position_data.get('entryPrice', 0)
            self.liquidation_price = position_data.get('liquidationPrice', 0)
            self.mark_price = position_data.get('markPrice', 0)
            self.unrealized_pnl = position_data.get('unrealizedPnl', 0)
            self.leverage = position_data.get('leverage', 1.0)
            self.margin_mode = position_data.get('marginMode', 'cross')
            
            # 포지션 방향 설정
            side = position_data.get('side', 'long')
            if side == 'short':
                self.position_size = -abs(self.position_size)
            
            # 포지션 가치 계산
            self.position_value = abs(self.position_size) * self.mark_price
            
            # 리스크 레벨 계산
            if self.position_size > 0:
                self.risk_level = self._calculate_risk_level(self.mark_price)
            else:
                self.risk_level = 'LOW'
            
            logger.info(f"API 데이터로 업데이트 완료 - 포지션: {side} {abs(self.position_size)} @ {self.entry_price}")
            
        except Exception as e:
            logger.error(f"API 데이터 업데이트 오류: {e}")

    def _calculate_risk_level(self, current_price: float) -> str:
        """
        리스크 레벨 계산
        
        Args:
            current_price: 현재 가격
        
        Returns:
            str - 리스크 레벨
        """
        if self.liquidation_price is None:
            return 'LOW'
        
        # 청산가 대비 현재가 거리 계산
        distance = (current_price - self.liquidation_price) / self.liquidation_price
        
        if distance <= 0.02:  # 2% 이내
            return 'CRITICAL'
        elif distance <= 0.05:  # 5% 이내
            return 'HIGH'
        elif distance <= 0.10:  # 10% 이내
            return 'MEDIUM'
        else:
            return 'LOW'

    def check_liquidation_risk(self, current_price: float) -> Dict[str, any]:
        """
        청산 위험도 확인
        
        Args:
            current_price: 현재 가격
        
        Returns:
            Dict[str, any] - 청산 위험 정보
        """
        with self.lock:
            if self.liquidation_price is None:
                return {
                    'is_at_risk': False,
                    'distance_percentage': 0.0,
                    'risk_level': 'LOW',
                    'liquidation_price': None
                }
            
            distance = (current_price - self.liquidation_price) / self.liquidation_price
            is_at_risk = current_price <= self.liquidation_price
            
            return {
                'is_at_risk': is_at_risk,
                'distance_percentage': distance * 100,
                'risk_level': self.risk_level,
                'liquidation_price': self.liquidation_price,
                'current_price': current_price,
                'actual_leverage': self.actual_leverage
            }

    def get_position_info(self) -> Dict[str, any]:
        """포지션 정보 반환"""
        with self.lock:
            return {
                'average_price': self.avg_calculator.get_average_price(),
                'position_size': self.position_size,
                'position_value': self.position_value,
                'actual_leverage': self.actual_leverage,
                'liquidation_price': self.liquidation_price,
                'risk_level': self.risk_level,
                'entry_count': self.avg_calculator.get_entry_count(),
                'initial_capital': self.initial_capital
            }

    def calculate_pnl(self, current_price: float) -> Dict[str, float]:
        """손익 계산"""
        return self.avg_calculator.calculate_pnl(current_price)

    def close_all_positions(self):
        """모든 포지션 청산"""
        with self.lock:
            self.avg_calculator.close_all_positions()
            self.position_size = 0.0
            self.position_value = 0.0
            self.actual_leverage = 0.0
            self.liquidation_price = None
            self.risk_level = 'LOW'
            logger.info("모든 포지션 청산 완료")

    def update_initial_capital(self, new_capital: float):
        """초기 자본 업데이트"""
        with self.lock:
            self.initial_capital = new_capital
            # 실제 레버리지 재계산
            if self.position_value > 0:
                self.actual_leverage = self.position_value / self.initial_capital
                # 청산가 재계산
                avg_price = self.avg_calculator.get_average_price()
                self.liquidation_price = self._calculate_liquidation_price(avg_price)
            logger.info(f"초기 자본 업데이트: {new_capital}")

    def get_risk_summary(self) -> Dict[str, any]:
        """리스크 요약 정보 반환"""
        with self.lock:
            return {
                'position_info': self.get_position_info(),
                'risk_level': self.risk_level,
                'liquidation_price': self.liquidation_price,
                'actual_leverage': self.actual_leverage,
                'maintenance_margin': self.maintenance_margin,
                'initial_capital': self.initial_capital
            }
    
    def sync_with_account_data(self, account_data: Dict[str, any]) -> Dict[str, any]:
        """
        실제 계정 데이터와 동기화
        
        Args:
            account_data: BitgetAccountManager에서 가져온 계정 데이터
        
        Returns:
            Dict[str, any] - 동기화 결과
        """
        with self.lock:
            try:
                if not account_data.get('has_position', False):
                    # 포지션이 없는 경우 초기화
                    self.close_all_positions()
                    return {
                        'synced': True,
                        'action': 'cleared',
                        'message': '실제 계정에 포지션이 없어 리스크 매니저를 초기화했습니다.'
                    }
                
                position = account_data['position']
                balance = account_data['balance']
                
                # 실제 계정 정보로 업데이트
                self.initial_capital = balance['total']
                
                # 포지션 정보 추출
                side = position['side']
                size = position['size']
                entry_price = position['entry_price']
                mark_price = position['mark_price']
                
                # 기존 포지션 초기화
                self.avg_calculator.close_all_positions()
                
                # 실제 포지션을 리스크 매니저에 반영
                if side == 'long':
                    # 롱 포지션
                    avg_price = self.avg_calculator.add_position(entry_price, size)
                else:
                    # 숏 포지션 (음수로 처리)
                    avg_price = self.avg_calculator.add_position(entry_price, -size)
                
                # 포지션 정보 업데이트
                self.position_size = abs(size)
                self.position_value = abs(size) * avg_price
                self.actual_leverage = self.position_value / self.initial_capital if self.initial_capital > 0 else 0.0
                
                # 청산가 재계산
                self.liquidation_price = self._calculate_liquidation_price(avg_price)
                
                # 리스크 레벨 재계산
                self.risk_level = self._calculate_risk_level(mark_price)
                
                logger.info(f"실제 계정 데이터 동기화 완료 - {side} {size} @ {avg_price}")
                
                return {
                    'synced': True,
                    'action': 'updated',
                    'position_info': {
                        'side': side,
                        'size': size,
                        'entry_price': entry_price,
                        'average_price': avg_price,
                        'mark_price': mark_price,
                        'unrealized_pnl': position['unrealized_pnl'],
                        'actual_leverage': self.actual_leverage,
                        'liquidation_price': self.liquidation_price,
                        'risk_level': self.risk_level
                    }
                }
                
            except Exception as e:
                logger.error(f"계정 데이터 동기화 오류: {e}")
                return {
                    'synced': False,
                    'error': str(e)
                }
    
    def get_real_time_risk_status(self, current_price: float) -> Dict[str, any]:
        """
        실시간 리스크 상태 확인 (계정 데이터 기반)
        
        Args:
            current_price: 현재 가격
        
        Returns:
            Dict[str, any] - 실시간 리스크 상태
        """
        with self.lock:
            try:
                # 기본 포지션 정보
                position_info = self.get_position_info()
                
                # 손익 계산
                pnl_info = self.calculate_pnl(current_price)
                
                # 청산 위험도 확인
                liquidation_risk = self.check_liquidation_risk(current_price)
                
                # 리스크 상태 종합
                risk_status = {
                    'timestamp': datetime.now(timezone.utc),
                    'current_price': current_price,
                    'position_info': position_info,
                    'pnl_info': pnl_info,
                    'liquidation_risk': liquidation_risk,
                    'risk_level': self.risk_level,
                    'is_position_active': self.position_size > 0
                }
                
                # 경고 메시지 생성
                warnings = []
                if liquidation_risk['is_at_risk']:
                    warnings.append("⚠️ 청산 위험! 즉시 조치 필요")
                elif liquidation_risk['distance_percentage'] < 5:
                    warnings.append("⚠️ 청산가 근접 (5% 이내)")
                elif pnl_info['unrealized_pnl_percent'] < -10:
                    warnings.append("⚠️ 큰 손실 발생 (-10% 이상)")
                elif self.actual_leverage > 10:
                    warnings.append("⚠️ 높은 레버리지 (10배 이상)")
                
                risk_status['warnings'] = warnings
                
                return risk_status
                
            except Exception as e:
                logger.error(f"실시간 리스크 상태 확인 오류: {e}")
                return {
                    'timestamp': datetime.now(timezone.utc),
                    'error': str(e),
                    'is_position_active': False
                }


if __name__ == "__main__":
    # 테스트 코드
    print("비트겟 리스크 매니저 테스트")
    
    # 리스크 매니저 생성
    risk_manager = BitgetRiskManager(initial_capital=5000.0, leverage=5.0)
    
    # 포지션 추가 테스트
    print("\n=== 포지션 추가 테스트 ===")
    result1 = risk_manager.add_position(50000.0, 0.1)
    print(f"첫 번째 진입: {result1}")
    
    result2 = risk_manager.add_position(49000.0, 0.1)
    print(f"두 번째 진입: {result2}")
    
    result3 = risk_manager.add_position(48000.0, 0.1)
    print(f"세 번째 진입: {result3}")
    
    # 청산 위험도 확인
    print("\n=== 청산 위험도 확인 ===")
    risk_info = risk_manager.check_liquidation_risk(47000.0)
    print(f"현재가 47000일 때: {risk_info}")
    
    risk_info = risk_manager.check_liquidation_risk(46000.0)
    print(f"현재가 46000일 때: {risk_info}")
    
    # 손익 계산
    print("\n=== 손익 계산 ===")
    pnl = risk_manager.calculate_pnl(47000.0)
    print(f"현재가 47000일 때 손익: {pnl}")
    
    # 포지션 정보
    print("\n=== 포지션 정보 ===")
    position_info = risk_manager.get_position_info()
    print(f"포지션 정보: {position_info}")
    
    # 리스크 요약
    print("\n=== 리스크 요약 ===")
    risk_summary = risk_manager.get_risk_summary()
    print(f"리스크 요약: {risk_summary}")
