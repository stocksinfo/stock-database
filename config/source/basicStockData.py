import numpy as np
from math import isnan
import os, sys

from numpy.ma.extras import average

sys.path.append(os.path.dirname(__file__))
from get_yfinance_data import CollectYFinanceData


class BasicStockData(CollectYFinanceData):
    def __init__(self):
        super().__init__()
        self.pre_signal = None
        self.pre_ema12 = None
        self.pre_ema26 = None
        self.fib_levels = [0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0]
        self.ts_indicators = {
            "RelativeStrengthIndex": { "period": 16, "func": self.get_rsi, "def": -1.0}, # 16 items for 15 days RSI
            "BollingerBands": { "period": 15, "func": self.get_bb, "def": [-1.0]*3},
            "YZVolatilityEstimator": { "period": 15, "func": self.get_yz_volatility, "def": -1.0},
            "AverageTrueRange": { "period": 15, "func": self.get_at_range, "def": -1.0},
            "MovingAverageConvergenceDivergence": { "period": 1, "func": self.get_mcad, "def": [-1.0]*3},
            "DollarVolume": { "period": 1, "func": self.get_dv, "def": -1.0},
            "StochasticOscillator": { "period": 17, "func": self.get_sto_osc, "def": [-1.0]*2},
        }

    def update_ts_indicators(self, timeseries: dict) -> dict:
        nts = timeseries
        key_list = sorted(timeseries.keys())
        value_list = list(dict(sorted(timeseries.items())).values())
        for index in range(len(key_list)):
            dt = key_list[index]
            # print(index, dt)
            for item in self.ts_indicators.keys():
                if index >= (self.ts_indicators[item]["period"]-1):
                    start_i = index - self.ts_indicators[item]["period"] + 1
                    end_i = index + 1
                    passing_items = value_list[start_i:end_i]
                    if self.ts_indicators[item]["period"] == 1:
                        passing_items = value_list[index]
                    val = self.ts_indicators[item]["func"](passing_items)
                    nts[dt][item] = val
                else:
                    nts[dt][item] = self.ts_indicators[item]["def"]

        return nts

    @staticmethod
    def get_rsi(items: list) -> float:
        pre_close = items[0]["Close"]
        gains = []
        losses = []
        for data in items[1:]:
            close_val = data["Close"]
            diff = close_val - pre_close
            if diff < 0:
                losses.append(-1.0*diff)
                gains.append(0.0)
            else:
                losses.append(0.0)
                gains.append(diff)
            pre_close = close_val
        # av_gain = average(gains)
        # av_loss = average(losses)
        av_gain = None
        for gain in gains:
            alpha = 1.0/15.0
            if av_gain is None:
                av_gain = gain
            else:
                av_gain = alpha*gain + ((1-alpha)*av_gain)
            # print("gain = ", gain, av_gain)
        av_loss = None
        for loss in losses:
            alpha = 1.0/15.0
            if av_loss is None:
                av_loss = loss
            else:
                av_loss = alpha*loss + ((1-alpha)*av_loss)
            # print("loss = ", loss, av_loss)
        # print(len(gains), gains)
        # print(len(losses), losses)
        # print(av_gain, av_loss)
        rs = 999.0
        if av_loss > 0.0:
            rs = av_gain / av_loss
        rsi_index = 100.0 - (100/(1 + rs))
        return round(rsi_index,4)

    @staticmethod
    def get_bb(items: list) -> list:
        closes = []
        for data in items:
            close = data["Close"]
            if isnan(close):
                close = 0.0
            closes.append(close)
        mean = float(np.mean(closes))
        sd = float(np.std(closes, ddof=0))
        bb_band = [round(mean-(2.0*sd),2), round(mean,2), round(mean+(2.0*sd),2)]
        return bb_band

    @staticmethod
    def rogers_satchell_var(o, h, l, c):
        # single-day Rogers-Satchell term (variance)
        # avoid div-by-zero or invalid logs by small epsilon
        eps = 1e-12
        o, h, l, c = np.maximum(o, eps), np.maximum(h, eps), np.maximum(l, eps), np.maximum(c, eps)
        return (np.log(h / c) * np.log(h / o) +
                np.log(l / c) * np.log(l / o))

    def get_yz_volatility(self, items: list, debug = False) -> float:
        hi = []
        lo = []
        op = []
        cl = []
        for data in items:
            hi.append(data["High"])
            lo.append(data["Low"])
            op.append(data["Open"])
            cl.append(data["Close"])
        # 1
        n = len(cl)
        hi = np.array(hi)
        lo = np.array(lo)
        op = np.array(op)
        cl = np.array(cl)
        rs_terms = self.rogers_satchell_var(op[1:], hi[1:], lo[1:], cl[1:])
        # print("rs_terms = ", rs_terms) if debug else None
        # overnight returns: log(Open_t / Close_{t-1}) for t = 1..n-1
        r_oc = np.log(op[1:]/cl[:-1])
        # print("r_oc = ", r_oc) if debug else None
        # open-to-close returns: log(Close_t / Open_t) for t = 1..n-1
        r_co = np.log(cl[1:]/op[1:])
        # print("r_co = ", r_co) if debug else None
        # sample variances (unbiased) of r_o and r_c: use (n-1) denominator
        sig2_oc = float(np.var(r_oc, ddof=1))  # overnight variance
        # print("sig2_oc = ", sig2_oc) if debug else None
        sig2_co = float(np.var(r_co, ddof=1))  # open-to-close variance
        # print("sig2_co = ", sig2_co) if debug else None
        # average RS term
        sig2_rs = rs_terms.mean()
        # print("sig2_rs = ", sig2_rs) if debug else None
        # k factor (Yang & Zhang, 2000)
        k = 0.34 / (1.34 + (n / (n - 2)))
        # print("k = ", k) if debug else None
        # YZ Volatility squared formula (combining open/close jump and drift-independent intraday)
        yz_volatility = np.sqrt(sig2_oc + k * sig2_co + (1.0 - k) * sig2_rs)
        # print("yz_volatility = ", yz_volatility) if debug else None
        return round(yz_volatility*100, 4)

    @staticmethod
    def get_at_range(items: list) -> float:
        hi = []
        lo = []
        cl = []
        for data in items:
            hi.append(data["High"])
            lo.append(data["Low"])
            cl.append(data["Close"])
        hi = np.array(hi[:-1])
        lo = np.array(lo[:-1])
        cl = np.array(cl[1:])
        tr1 = hi-lo
        tr2 = np.abs(hi-cl)
        tr3 = np.abs(lo-cl)
        tr_max = np.maximum(tr1, tr2, tr3)
        atr = tr_max.mean()
        return round(atr, 2)

    def get_mcad(self, item) -> list:
        ema12 = 0.0
        ema26 = 0.0
        macd = 0.0
        # 1. Calculate EMAs
        if self.pre_ema12 is not None:
            alpha = 0.15384615
            ema12 = alpha*item["Close"] + (1-alpha)*self.pre_ema12
            self.pre_ema12 = ema12
        else:
            self.pre_ema12 = float(item["Close"])
        if self.pre_ema26 is not None:
            alpha = 0.07407407
            ema26 =  alpha*item["Close"] + (1-alpha)*self.pre_ema26
            self.pre_ema26 = ema26
        else:
            self.pre_ema26 = float(item["Close"])
        # 2. MACD line
        signal = 0.0
        if self.pre_signal is not None:
            macd = ema12 - ema26
            alpha = 0.2
            signal = alpha * macd + (1 - alpha) * self.pre_signal
            self.pre_signal = signal
        else:
            self.pre_signal = 0.0
        histogram = macd - signal
        # print(item["Close"], macd, signal, histogram)
        return [round(macd,4), round(signal,4), round(histogram,4)]

    @staticmethod
    def get_dv(item) -> float:
        return round(item["Close"]*item["Volume"], 2)

    @staticmethod
    def get_sto_osc(items: list) -> list:
        # print(len(items))
        hi = []
        lo = []
        cl = []
        so = []
        for data in items:
            hi.append(data["High"])
            lo.append(data["Low"])
            cl.append(data["Close"])
        # print(len(hi), hi)
        # print(len(lo), lo)
        # print(len(cl), cl)
        for i in range(14, len(hi)):
            low_min = np.min(lo[i-14:i+1])
            high_max = np.max(hi[i-14:i+1])
            # print(i, cl[i], low_min, high_max)
            if high_max != low_min:
                so.append(100.0*(cl[i] - low_min) / (high_max - low_min))
            else:
                so.append(100.0)
        # print(so)
        pd = round(float(so[-1]),3)
        pk = round(float(np.mean(so)),3)
        return [pd, pk]


