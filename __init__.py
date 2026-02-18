"""
비트겟 실시간 자동매매 모듈

비트겟 거래소를 사용한 실시간 자동매매 시스템입니다.
"""

from .bitget_client import BitgetRealtimeClient
from .data_manager import BitgetDataManager
from .main import BitgetRealtimeTradingSystem

__all__ = [
    'BitgetRealtimeClient',
    'BitgetDataManager', 
    'BitgetRealtimeTradingSystem'
]
