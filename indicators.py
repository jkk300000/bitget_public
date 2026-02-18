"""
비트겟 실시간 지표 계산 모듈

지원하는 지표:
- ATR 14 RMA (Pine Script ta.atr(14)와 동일)
- RSI 14 (Pine Script ta.rsi(14)와 동일)
- Squeeze Momentum Indicator [LazyBear]
- Support and Resistance Levels with Breaks [LuxAlgo]
"""

import pandas as pd
import numpy as np
import talib as ta
from typing import Dict, Tuple, Optional, List
import logging
from datetime import datetime, timezone, timedelta

# 로거 설정
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

class BitgetIndicatorCalculator:
    """
    비트겟 실시간 지표 계산 클래스
    
    Pine Script와 동일한 계산 결과를 보장합니다.
    """
    
    def __init__(self):
        """초기화"""
        self.cache = {}  # 계산 결과 캐시
        self.cache_size = 1000  # 캐시 크기 제한
        
        logger.info("비트겟 지표 계산기 초기화 완료")

    def calculate_rma(self, data: pd.Series, period: int) -> pd.Series:
        """
        RMA (Relative Moving Average) 계산 - Pine Script ta.rma()와 정확히 일치
        
        Args:
            data: pandas Series - 계산할 데이터
            period: int - 기간
        
        Returns:
            pandas Series - RMA 값
        """
        if len(data) < period:
            return pd.Series([np.nan] * len(data), index=data.index)
        
        # 결과를 저장할 Series 생성
        rma = pd.Series(index=data.index, dtype=float)
        
        # Pine Script와 동일한 방식으로 계산
        alpha = 1.0 / period
        sum_val = 0.0
        
        for i in range(len(data)):
            if pd.isna(data.iloc[i]):
                rma.iloc[i] = np.nan
                continue
                
            if pd.isna(sum_val) or i < period:
                # 첫 period개 값의 SMA로 초기화 (Pine Script 방식)
                if i == period - 1:
                    sum_val = data.iloc[:period].mean()
                    rma.iloc[i] = sum_val
                else:
                    rma.iloc[i] = np.nan
            else:
                # RMA 공식: alpha * current_value + (1 - alpha) * previous_rma
                sum_val = alpha * data.iloc[i] + (1 - alpha) * sum_val
                rma.iloc[i] = sum_val
        
        return rma

    def calculate_atr_rma(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        """
        ATR 14 RMA 계산 - Pine Script ta.atr(14)와 정확히 일치
        
        Args:
            df: pandas DataFrame - OHLC 데이터
            period: int - ATR 기간 (기본값: 14)
        
        Returns:
            pandas Series - ATR 값
        """
        high = df['high']
        low = df['low']
        close = df['close']
        
        # True Range 계산
        tr = self.calculate_true_range(high, low, close)
        
        # RMA 기반 ATR (Pine Script와 정확히 동일)
        atr = self.calculate_rma(tr, period)
        
        return atr

    def calculate_true_range(self, high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
        """
        True Range 계산 - Pine Script ta.tr()와 정확히 일치
        
        Args:
            high: pandas Series - 고가
            low: pandas Series - 저가  
            close: pandas Series - 종가
        
        Returns:
            pandas Series - True Range 값
        """
        tr = pd.Series(index=high.index, dtype=float)
        
        for i in range(len(high)):
            if i == 0:
                # 첫 번째 봉은 high - low만 사용
                tr.iloc[i] = high.iloc[i] - low.iloc[i]
            else:
                # 이전 종가 확인
                prev_close = close.iloc[i-1]
                
                if pd.isna(prev_close):
                    # high - low 값 사용
                    tr.iloc[i] = high.iloc[i] - low.iloc[i]
                else:
                    # Pine Script 공식: math.max(high - low, math.abs(high - close[1]), math.abs(low - close[1]))
                    tr.iloc[i] = max(
                        high.iloc[i] - low.iloc[i],
                        abs(high.iloc[i] - prev_close),
                        abs(low.iloc[i] - prev_close)
                    )
        
        return tr

    def calculate_squeeze_momentum(self, df: pd.DataFrame, 
                                 length: int = 20, 
                                 mult: float = 2.0,
                                 lengthKC: int = 35, 
                                 multKC: float = 1.0,
                                 useTrueRange: bool = True) -> Dict[str, pd.Series]:
        """
        Squeeze Momentum Indicator 계산 - LazyBear 버전
        
        Args:
            df: pandas DataFrame - OHLC 데이터
            length: int - Bollinger Bands 기간
            mult: float - Bollinger Bands 승수
            lengthKC: int - Keltner Channel 기간
            multKC: float - Keltner Channel 승수
            useTrueRange: bool - True Range 사용 여부
        
        Returns:
            Dict[str, pd.Series] - 지표 결과들
        """
        close = df['close']
        high = df['high']
        low = df['low']
        
        # Bollinger Bands 계산 (Pine: basis = sma(close, length), dev = mult * stdev(close, length))
        basis = close.rolling(window=length).mean()
        dev = mult * close.rolling(window=length).std()
        upperBB = basis + dev
        lowerBB = basis - dev
        
        # Keltner Channel 계산 (Pine: ma = sma(close, lengthKC))
        ma = close.rolling(window=lengthKC).mean()
        
        if useTrueRange:
            tr = self.calculate_true_range(high, low, close)
        else:
            tr = high - low
        
        # Pine: rangema = sma(range, lengthKC)
        rangema = tr.rolling(window=lengthKC).mean()
        upperKC = ma + rangema * multKC
        lowerKC = ma - rangema * multKC
        
        # Squeeze 상태 계산
        sqzOn = (lowerBB > lowerKC) & (upperBB < upperKC)
        sqzOff = (lowerBB < lowerKC) & (upperBB > upperKC)
        noSqz = ~(sqzOn | sqzOff)
        
        # Squeeze Momentum 값 계산
        highest = high.rolling(window=lengthKC).max()
        lowest = low.rolling(window=lengthKC).min()
        sma_lenKC = close.rolling(window=lengthKC).mean()
        middle_hl = (highest + lowest) / 2
        # Pine: val = linreg(close - avg(avg(highest(high, lengthKC), lowest(low, lengthKC)), sma(close,lengthKC)), lengthKC, 0)
        # avg(a,b) = (a+b)/2 → close - (( (H+L)/2 + sma_lenKC ) / 2)
        diff = close - ((middle_hl + sma_lenKC) / 2)
        
        # Linear Regression 계산 (Pine ta.linreg 동일, offset=0)
        val = self.calculate_linear_regression(diff, lengthKC, offset=0)
        
        # 색상 계산
        bcolor = pd.Series(index=val.index, dtype=object)
        scolor = pd.Series(index=val.index, dtype=object)
        
        for i in range(len(val)):
            if i == 0:
                bcolor.iloc[i] = 'gray'
                scolor.iloc[i] = 'gray'
            else:
                # bcolor 계산
                if val.iloc[i] > 0:
                    if val.iloc[i] > val.iloc[i-1]:
                        bcolor.iloc[i] = 'lime'
                    else:
                        bcolor.iloc[i] = 'green'
                else:
                    if val.iloc[i] < val.iloc[i-1]:
                        bcolor.iloc[i] = 'red'
                    else:
                        bcolor.iloc[i] = 'maroon'
                
                # scolor 계산
                if noSqz.iloc[i]:
                    scolor.iloc[i] = 'blue'
                elif sqzOn.iloc[i]:
                    scolor.iloc[i] = 'black'
                else:
                    scolor.iloc[i] = 'gray'
        
        return {
            'val': val,
            'bcolor': bcolor,
            'scolor': scolor,
            'sqzOn': sqzOn,
            'sqzOff': sqzOff,
            'noSqz': noSqz,
            'upperBB': upperBB,
            'lowerBB': lowerBB,
            'upperKC': upperKC,
            'lowerKC': lowerKC
        }

    def calculate_linear_regression(self, data: pd.Series, period: int, offset: int = 0) -> pd.Series:
        """
        Linear Regression 계산 - Pine Script ta.linreg와 동일
        
        Args:
            data: pandas Series - 계산할 데이터
            period: int - 기간
            offset: int - 오프셋 (기본값 0)
        
        Returns:
            pandas Series - Linear Regression 값
        """
        result = pd.Series(index=data.index, dtype=float)
        
        for i in range(len(data)):
            if i < period - 1:
                result.iloc[i] = np.nan
            else:
                # 최근 period개 데이터로 선형 회귀 계산
                y = data.iloc[i-period+1:i+1].values
                x = np.arange(period)
                
                # NaN 값이 있으면 건너뛰기
                if np.any(np.isnan(y)):
                    result.iloc[i] = np.nan
                    continue
                
                # 선형 회귀 계산 (Pine 공식: linreg = intercept + slope * (length - 1 - offset))
                if len(y) == period:
                    # numpy의 polyfit 사용
                    slope, intercept = np.polyfit(x, y, 1)
                    eval_x = (period - 1 - offset)
                    result.iloc[i] = intercept + slope * eval_x
                else:
                    result.iloc[i] = np.nan
        
        return result

    def calculate_support_resistance(self, df: pd.DataFrame, 
                                   leftBars: int = 40, 
                                   rightBars: int = 40,
                                   volumeThresh: int = 20) -> Dict[str, pd.Series]:
        """
        Support and Resistance Levels with Breaks 계산 - LuxAlgo 버전
        
        Args:
            df: pandas DataFrame - OHLC 데이터
            leftBars: int - 왼쪽 바 수
            rightBars: int - 오른쪽 바 수
            volumeThresh: int - 볼륨 임계값
        
        Returns:
            Dict[str, pd.Series] - 지표 결과들
        """
        high = df['high']
        low = df['low']
        close = df['close']
        volume = df['volume']
        
        # Pivot High/Low 계산 (Pine Script와 동일: [1] 오프셋 적용)
        pivot_high_raw = self.calculate_pivot_high(high, leftBars, rightBars)
        pivot_low_raw = self.calculate_pivot_low(low, leftBars, rightBars)
        
        # Pine Script의 [1] 오프셋과 fixnan() 적용
        pivot_high = pivot_high_raw.shift(1).ffill()
        pivot_low = pivot_low_raw.shift(1).ffill()
        
        # 볼륨 계산
        short_vol = volume.ewm(span=5).mean()
        long_vol = volume.ewm(span=10).mean()
        osc = 100 * (short_vol - long_vol) / long_vol
        
        # Breakout 신호 계산
        crossover_high = self.calculate_crossover(close.shift(3), pivot_high)
        crossunder_low = self.calculate_crossunder(close, pivot_low)
        
        # Breakout 조건 (볼륨 확인)
        break_high = crossover_high & (osc > volumeThresh) & ~(close.shift(1) - close < high - close.shift(1))
        break_low = crossunder_low & (osc > volumeThresh) & ~(close.shift(1) - close < high - close.shift(1))
        
        return {
            'pivot_high': pivot_high,
            'pivot_low': pivot_low,
            'crossover_high': crossover_high,
            'crossunder_low': crossunder_low,
            'break_high': break_high,
            'break_low': break_low,
            'volume_osc': osc
        }

    def calculate_pivot_high(self, high: pd.Series, leftBars: int, rightBars: int) -> pd.Series:
        """Pivot High 계산"""
        result = pd.Series(index=high.index, dtype=float)
        
        for i in range(len(high)):
            if i < leftBars or i >= len(high) - rightBars:
                result.iloc[i] = np.nan
            else:
                # 왼쪽과 오른쪽 바들과 비교
                left_highs = high.iloc[i-leftBars:i]
                right_highs = high.iloc[i+1:i+rightBars+1]
                current_high = high.iloc[i]
                
                # 현재 고가가 양쪽의 모든 고가보다 높으면 Pivot High
                # Pivot Low와 대칭: 오른쪽은 <= (같거나 작아야 함)
                if (left_highs < current_high).all() and (right_highs <= current_high).all():
                    result.iloc[i] = current_high
                else:
                    result.iloc[i] = np.nan
        
        return result

    def calculate_pivot_low(self, low: pd.Series, leftBars: int, rightBars: int) -> pd.Series:
        """Pivot Low 계산"""
        result = pd.Series(index=low.index, dtype=float)
        
        for i in range(len(low)):
            if i < leftBars or i >= len(low) - rightBars:
                result.iloc[i] = np.nan
            else:
                # 왼쪽과 오른쪽 바들과 비교
                left_lows = low.iloc[i-leftBars:i]
                right_lows = low.iloc[i+1:i+rightBars+1]
                current_low = low.iloc[i]
                
                # 현재 저가가 양쪽의 모든 저가보다 낮으면 Pivot Low
                if (left_lows > current_low).all() and (right_lows >= current_low).all():
                    result.iloc[i] = current_low
                else:
                    result.iloc[i] = np.nan
        
        return result

    def calculate_crossover(self, series1: pd.Series, series2: pd.Series) -> pd.Series:
        """Crossover 계산"""
        return (series1 > series2) & (series1.shift(1) <= series2.shift(1))

    def calculate_crossunder(self, series1: pd.Series, series2: pd.Series) -> pd.Series:
        """Crossunder 계산"""
        return (series1 < series2) & (series1.shift(1) >= series2.shift(1))

    def calculate_rsi(self, close: pd.Series, period: int = 14) -> pd.Series:
        """
        RSI (Relative Strength Index) 계산 - Pine Script ta.rsi()와 정확히 일치
        
        Pine Script 공식 (ta.rma() 사용):
        pine_rsi(x, y) =>
            u = math.max(x - x[1], 0)  // upward change
            d = math.max(x[1] - x, 0)  // downward change
            rs = ta.rma(u, y) / ta.rma(d, y)
            res = 100 - 100 / (1 + rs)
            res
        
        Args:
            close: pandas Series - 종가 데이터
            period: int - RSI 기간 (기본값: 14)
        
        Returns:
            pandas Series - RSI 값
        """
        if len(close) < period:
            return pd.Series([np.nan] * len(close), index=close.index, dtype=float)
        
        # Pine Script 공식대로 계산
        delta = close.diff()
        
        # u = math.max(x - x[1], 0)  // upward change
        up = delta.where(delta > 0, 0)
        
        # d = math.max(x[1] - x, 0)  // downward change  
        down = -delta.where(delta < 0, 0)
        
        # rs = ta.rma(u, y) / ta.rma(d, y)
        rma_up = self.calculate_rma(up, period)
        rma_down = self.calculate_rma(down, period)
        
        # res = 100 - 100 / (1 + rs)
        rsi = pd.Series(index=close.index)
        
        for i in range(len(rma_up)):
            if pd.isna(rma_up.iloc[i]) or pd.isna(rma_down.iloc[i]):
                rsi.iloc[i] = np.nan
            elif rma_down.iloc[i] == 0:
                rsi.iloc[i] = 100.0
            elif rma_up.iloc[i] == 0:
                rsi.iloc[i] = 0.0
            else:
                rs = rma_up.iloc[i] / rma_down.iloc[i]
                rsi.iloc[i] = 100.0 - (100.0 / (1.0 + rs))
        
        return rsi

    def calculate_ema(self, data: pd.Series, period: int) -> pd.Series:
        """
        EMA (Exponential Moving Average) 계산 - TradingView ta.ema() 방식과 100% 동일
        
        TradingView ta.ema() 계산 방식:
        - α = 2 / (length + 1)
        - 첫 번째 캔들: EMA[0] = Price[0] (종가 그대로)
        - 두 번째 캔들부터: EMA[t] = Price[t] × α + EMA[t-1] × (1 - α)
        - 초기값을 SMA로 강제하지 않음 (adjust=False 방식)
        
        Args:
            data: pandas Series - 계산할 데이터 (예: close 가격)
            period: int - EMA 기간 (예: 200)
        
        Returns:
            pandas Series - EMA 값
        """
        if len(data) == 0:
            return pd.Series([], index=data.index, dtype=float)
        
        # 스무딩 계수 계산: α = 2 / (period + 1)
        alpha = 2.0 / (period + 1.0)
        
        # 결과를 저장할 Series 생성
        result = pd.Series(index=data.index, dtype=float)
        
        prev_ema = None
        
        for i in range(len(data)):
            if pd.isna(data.iloc[i]):
                # NaN 값은 이전 EMA 유지 (없으면 NaN)
                result.iloc[i] = prev_ema if prev_ema is not None else np.nan
                continue
            
            if prev_ema is None:
                # 첫 번째 유효한 값: 종가를 그대로 첫 EMA 값으로 사용 (TradingView 방식)
                prev_ema = data.iloc[i]
                result.iloc[i] = prev_ema
            else:
                # 두 번째 값부터: 재귀식 사용
                # EMA[t] = Price[t] × α + EMA[t-1] × (1 - α)
                prev_ema = data.iloc[i] * alpha + prev_ema * (1.0 - alpha)
                result.iloc[i] = prev_ema
        
        return result

    def calculate_all_indicators(self, df: pd.DataFrame) -> Dict[str, any]:
        """
        모든 지표를 한 번에 계산
        
        Args:
            df: pandas DataFrame - OHLC 데이터
        
        Returns:
            Dict[str, any] - 모든 지표 결과들
        """
        # ATR 14 RMA 계산
        atr_14 = self.calculate_atr_rma(df, period=14)
        
        # RSI 14 계산
        rsi_14 = self.calculate_rsi(df['close'], period=14)
        
        # Squeeze Momentum 계산
        squeeze_result = self.calculate_squeeze_momentum(df)
        
        # Support/Resistance 계산
        sr_result = self.calculate_support_resistance(df)
        
        # 결과 통합
        result = {
            'atr_14': atr_14,
            'rsi_14': rsi_14,
            'squeeze': squeeze_result,
            'support_resistance': sr_result
        }
        return result

    def calculate_atr_and_squeeze(self, df: pd.DataFrame) -> Dict[str, any]:
        """
        ATR과 Squeeze Momentum만 계산 (1분봉용)
        
        Args:
            df: pandas DataFrame - OHLC 데이터
        
        Returns:
            Dict[str, any] - ATR과 Squeeze 결과들
        """
        # ATR 14 RMA 계산
        atr_14 = self.calculate_atr_rma(df, period=14)
        
        # Squeeze Momentum 계산
        squeeze_result = self.calculate_squeeze_momentum(df)
        
        return {
            'atr_14': atr_14,
            'squeeze': squeeze_result
        }

    def resample_35min_exclude_last(self, df_segment: pd.DataFrame, start_time: datetime, end_time: datetime) -> Optional[Dict]:
        """
        35분봉 계산 (마지막 1분봉 제외 - 트레이딩뷰 방식)
        
        예: 01:10 ~ 01:45 봉
        - 사용: 01:10 ~ 01:44 (34개 1분봉)
        - Open: 01:10 시가
        - High: 01:10~01:44 최고가
        - Low: 01:10~01:44 최저가
        - Close: 01:44 종가
        
        Args:
            df_segment: 전체 1분봉 데이터
            start_time: 봉 시작 시각
            end_time: 봉 종료 시각
        
        Returns:
            dict or None - 35분봉 데이터
        """
        # 마지막 1분봉 제외 (종료 -1분까지만)
        actual_end = end_time - timedelta(minutes=1)
        data = df_segment[(df_segment.index >= start_time) & (df_segment.index <= actual_end)]
        
        if data.empty:
            return None
        
        candle = {
            'timestamp': end_time,  # 종료 시각
            'open': data.iloc[0]['open'],
            'high': data['high'].max(),
            'low': data['low'].min(),
            'close': data.iloc[-1]['close'],
            'volume': data['volume'].sum()
        }
        
        return candle

    def resample_with_utc_reset(self, df: pd.DataFrame, timeframe: str = '35min') -> pd.DataFrame:
        """
        UTC 00:00 일일 리셋을 적용한 리샘플링
        
        로직:
        1. 00:00 이전 23:55까지: 일반 35분 리샘플링 (... → 23:20 → 23:55)
        2. 23:55 ~ 00:00: 수동으로 5분 봉 생성 (5개 전체 사용)
        3. 00:00 이후: 00:00 origin으로 35분 리샘플링 (00:00 → 00:35 → 01:10)
        
        모든 35분봉은 마지막 1분 제외 (34개 사용), 5분 봉만 5개 전체 사용
        
        Args:
            df: pandas DataFrame - 1분봉 데이터
            timeframe: str - 리샘플링 간격
        
        Returns:
            pandas DataFrame - 리샘플링된 데이터
        """
        if df.empty:
            return df
        
        interval_minutes = int(timeframe.replace('min', ''))
        
        # 데이터 내 UTC 00:00 찾기
        midnights = []
        for ts in df.index:
            midnight = ts.replace(hour=0, minute=0, second=0, microsecond=0)
            if midnight not in midnights and df.index[0] < midnight <= df.index[-1]:
                midnights.append(midnight)
        
        # UTC 00:00이 없으면 일반 리샘플링
        if not midnights:
            resampled = df.resample(timeframe, label='right', closed='right').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna()
            
            # 마지막 1분 제외 로직 적용
            result_list = []
            for ts in resampled.index:
                start_time = ts - timedelta(minutes=interval_minutes)
                candle = self.resample_35min_exclude_last(df, start_time, ts)
                if candle:
                    result_list.append(candle)
            
            if not result_list:
                return pd.DataFrame()
            
            result_df = pd.DataFrame(result_list)
            result_df.set_index('timestamp', inplace=True)
            result_df.sort_index(inplace=True)
            return result_df
        
        logger.info(f"  UTC 00:00 발견: {len(midnights)}개")
        
        result_list = []
        midnight = midnights[0]
        
        # 1. 00:00 이전 23:54까지: 일반 리샘플링 후 마지막 1분 제외
        before_23_55 = df[df.index < midnight - timedelta(minutes=5)]
        
        if not before_23_55.empty:
            before_resampled = before_23_55.resample(timeframe, label='right', closed='right').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna()
            
            logger.info(f"  00:00 이전 (~23:55): {len(before_23_55)}개 1분봉 → {len(before_resampled)}개 35분 봉")
            
            # 각 봉에서 마지막 1분 제외하고 다시 계산
            for ts in before_resampled.index:
                start_time = ts - timedelta(minutes=interval_minutes)
                candle = self.resample_35min_exclude_last(df, start_time, ts)
                if candle:
                    result_list.append(candle)
        
        # 2. 23:55 ~ 23:59: 5개 1분봉으로 5분 봉 (전체 사용)
        start_5min = midnight - timedelta(minutes=5)
        data_5min = df[(df.index >= start_5min) & (df.index < midnight)]
        
        if not data_5min.empty:
            # 5분 봉은 마지막 1분 제외 안함 (5개 전체)
            candle_5min = {
                'timestamp': midnight,
                'open': data_5min.iloc[0]['open'],
                'high': data_5min['high'].max(),
                'low': data_5min['low'].min(),
                'close': data_5min.iloc[-1]['close'],  # 23:59 종가
                'volume': data_5min['volume'].sum()
            }
            logger.info(f"  23:55 ~ 23:59: 5개 1분봉으로 5분 봉 생성 (전체 사용) [OK]")
            result_list.append(candle_5min)
                
        # 3. 00:00 이후: 00:00 origin으로 리샘플링 후 마지막 1분 제외
        after = df[df.index >= midnight]
        
        if not after.empty:
            after_resampled = after.resample(timeframe, label='right', closed='right', origin=midnight).agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna()
            
            # 00:00 봉 제외
            after_resampled = after_resampled[after_resampled.index > midnight]
            
            logger.info(f"  00:00 이후: {len(after)}개 1분봉 → {len(after_resampled)}개 35분 봉")
            
            # 각 봉에서 마지막 1분 제외하고 다시 계산
            for ts in after_resampled.index:
                start_time = ts - timedelta(minutes=interval_minutes)
                candle = self.resample_35min_exclude_last(df, start_time, ts)
                if candle:
                    result_list.append(candle)
        
        if not result_list:
            return pd.DataFrame()
        
        result_df = pd.DataFrame(result_list)
        result_df.set_index('timestamp', inplace=True)
        result_df.sort_index(inplace=True)
        
        return result_df

    def resample_data(self, df: pd.DataFrame, timeframe: str = '35min') -> pd.DataFrame:
        """
        데이터 리샘플링 (1분봉 → 35분봉)
        
        UTC 00:00 일일 리셋 및 마지막 1분 제외 로직 적용
        
        Args:
            df: pandas DataFrame - 1분봉 데이터
            timeframe: str - 리샘플링 시간프레임
        
        Returns:
            pandas DataFrame - 리샘플링된 데이터
        """
        if df.empty:
            return df
        
        # 35분봉인 경우 UTC 00:00 리셋 로직 사용
        if timeframe == '35min':
            resampled = self.resample_with_utc_reset(df, timeframe)
            logger.debug(f"데이터 리샘플링 완료 (UTC 리셋): {len(df)} → {len(resampled)} (1분봉 → {timeframe})")
            return resampled
        
        # 다른 타임프레임은 일반 리샘플링
        resampled = df.resample(timeframe, label='right', closed='right').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        logger.debug(f"데이터 리샘플링 완료: {len(df)} → {len(resampled)} (1분봉 → {timeframe})")
        return resampled

    def get_latest_values(self, df: pd.DataFrame) -> Dict[str, float]:
        """
        최신 지표 값들 반환
        
        Args:
            df: pandas DataFrame - OHLC 데이터
        
        Returns:
            Dict[str, float] - 최신 지표 값들
        """
        if df.empty:
            return {}
        
        # 모든 지표 계산
        indicators = self.calculate_all_indicators(df)
        
        # 최신 값들 추출
        latest_values = {}
        
        # ATR
        if not indicators['atr_14'].empty:
            latest_values['atr_14'] = indicators['atr_14'].iloc[-1]
        
        # Squeeze Momentum
        if not indicators['squeeze']['val'].empty:
            latest_values['squeeze_val'] = indicators['squeeze']['val'].iloc[-1]
        
        # Support/Resistance
        if not indicators['support_resistance']['pivot_high'].empty:
            latest_values['resistance'] = indicators['support_resistance']['pivot_high'].iloc[-1]
        if not indicators['support_resistance']['pivot_low'].empty:
            latest_values['support'] = indicators['support_resistance']['pivot_low'].iloc[-1]
        
        return latest_values

    def clear_cache(self):
        """캐시 초기화"""
        self.cache.clear()
        logger.info("비트겟 지표 계산 캐시 초기화 완료")


if __name__ == "__main__":
    # 테스트 코드
    print("비트겟 지표 계산 모듈 테스트")
    
    # 샘플 데이터 생성
    dates = pd.date_range(start='2024-01-01', periods=100, freq='1min')
    np.random.seed(42)
    
    sample_data = pd.DataFrame({
        'open': 50000 + np.random.randn(100) * 100,
        'high': 50000 + np.random.randn(100) * 100 + 50,
        'low': 50000 + np.random.randn(100) * 100 - 50,
        'close': 50000 + np.random.randn(100) * 100,
        'volume': np.random.randint(100, 1000, 100)
    }, index=dates)
    
    # 고가/저가 조정
    sample_data['high'] = np.maximum(sample_data[['open', 'close']].max(axis=1), sample_data['high'])
    sample_data['low'] = np.minimum(sample_data[['open', 'close']].min(axis=1), sample_data['low'])
    
    # 지표 계산기 생성
    calculator = BitgetIndicatorCalculator()
    
    # 지표 계산
    indicators = calculator.calculate_all_indicators(sample_data)
    
    print(f"ATR 14 마지막 값: {indicators['atr_14'].iloc[-1]:.2f}")
    print(f"Squeeze Momentum 마지막 값: {indicators['squeeze']['val'].iloc[-1]:.2f}")
    
    # 최신 값들
    latest = calculator.get_latest_values(sample_data)
    print(f"최신 지표 값들: {latest}")
