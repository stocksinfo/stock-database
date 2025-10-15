import numpy as np
from math import isnan
import os, sys
import datetime

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

    @staticmethod
    def get_timeseries_entry(timeseries: dict, date_key: str, tag: str) -> float:
        value = 0.0
        if date_key in timeseries:
            value = timeseries[date_key][tag]
        else:
            last_date = sorted(timeseries.keys())[0]
            last_date = datetime.datetime.strptime(last_date, "%Y-%m-%d")
            ask_date = datetime.datetime.strptime(date_key, "%Y-%m-%d")
            if ask_date < last_date:
                value = timeseries[last_date.strftime("%Y-%m-%d")][tag]
            else:
                k = "-".join(date_key.split("-")[:-1])
                red_ts = {}
                for key in timeseries.keys():
                    if k in key:
                        red_ts[key] = timeseries[key]
                for key in sorted(red_ts.keys(), reverse=True):
                    value = red_ts[key][tag]
                    if not isnan(value):
                        break
        value = round(value, 2)
        return value

    '''
    Revenue Growth is calculated based on current and previous value,
    be it yearwise or quarter wise, this will give a comparison about how the company did in 
    last year or last quarter.
    '''

    @staticmethod
    def get_rev_growth(infos: dict, financials: dict, timeseries: dict) -> dict:
        value = {}
        try:
            dt = datetime.datetime.today().strftime("%Y-%m-%d")
            data = infos["revenueGrowth"]
            if not data:
                value[dt] = 0.0
            else:
                value[dt] = round(data, 2)
            pre_val = 0.0
            for dt in sorted(financials["TotalRevenue"].keys()):
                cur_val = float(financials["TotalRevenue"][dt])
                if isnan(cur_val):
                    continue
                if pre_val != 0.0:
                    value[dt] = round((cur_val - pre_val) / pre_val, 2)
                pre_val = cur_val
        except Exception as e:
            print(f"RevG. {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
        value = dict(sorted(value.items(), reverse=True))
        return value

    '''
    Profit Growth is calculated based on current and previous value,
    be it yearwise or quarter wise, this will give a comparison about how the company did in 
    last year or last quarter.
    As there is no indicator in the Info it is calculated based on Financial Results.
    When there is no Financial Results it is equal to Revenue Growth.
    '''

    def get_profit_growth(self, infos, financials, timeseries):
        value = {}
        try:
            dt = datetime.datetime.today().strftime("%Y-%m-%d")
            pre_val = 0.0
            data = infos["grossProfits"]
            if not data:
                data = 0.0
            else:
                data = round(data, 2)
            for date_key in sorted(financials["GrossProfit"].keys()):
                cur_val = float(financials["GrossProfit"][date_key])
                if isnan(cur_val):
                    continue
                if pre_val != 0.0:
                    value[date_key] = round((cur_val - pre_val) / pre_val, 2)
                pre_val = cur_val
            if pre_val != 0.0:
                # print(data,pre_val)
                value[dt] = round((data - pre_val) / pre_val, 2)
        except Exception as e:
            print(f"ProftG. {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
        if not value:
            value = self.get_rev_growth(infos, financials, timeseries)
        value = dict(sorted(value.items(), reverse=True))
        return value

    '''
    Earning per Share is calculated :
    1. via trailingEps information of the Info -> Based on yearly performance
    2. based on Diluted EPS of the Financial part 
        This will give last statement performance
    These two value can give you information about how was the company in last 12 months and in last 3 months
    '''

    @staticmethod
    def get_eps(infos, financials, timeseries):
        value = {}
        try:
            dt = datetime.datetime.today().strftime("%Y-%m-%d")
            data = infos["trailingEps"]
            if not data:
                value[dt] = 0.0
            else:
                value[dt] = round(data, 2)
            for dt in sorted(financials["DilutedEPS"].keys()):
                cur_val = float(financials["DilutedEPS"][dt])
                if isnan(cur_val):
                    continue
                value[dt] = round(cur_val, 2)
        except Exception as e:
            print(f"EPS. {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
        value = dict(sorted(value.items(), reverse=True))
        return value

    '''
    Assets vs. Liabilities :
    This is just a ratio of Total Assets and Total Liabilities Net Minority Interest
    Two values gives you a trend from last two statements
    '''

    @staticmethod
    def get_ass_v_lia(infos, financials, timeseries):
        value = {}
        try:
            for dt in sorted(financials["TotalAssets"].keys()):
                if dt in financials["TotalLiabilitiesNetMinorityInterest"]:
                    aval = float(financials["TotalAssets"][dt])
                    lval = float(financials["TotalLiabilitiesNetMinorityInterest"][dt])
                    if isnan(aval) or isnan(lval) or lval == 0.0:
                        continue
                    value[dt] = round(aval / lval, 2)
        except Exception as e:
            print(f"A/L. {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
        value = dict(sorted(value.items(), reverse=True))
        return value

    '''
    Debt-to-Equity Ratio (D/E) :
    This measures a company's leverage. A low ratio (often 1 or lower) suggests less 
    reliance on debt. Note that "good" ratios can vary by industry. 
    To calculate this ratio, you need the company's total debt (or total liabilities) 
    and total shareholder equity. The formula is: D/E Ratio=Total Debt/Total Equity
    1. via debtToEquity information of the Info -> Based on yearly performance
    2. based on Total Debt and Stockholders Equity of the Financial part
    '''

    @staticmethod
    def get_de_ratio(infos, financials, timeseries):
        value = {}
        try:
            dt = datetime.datetime.today().strftime("%Y-%m-%d")
            data = infos["debtToEquity"]
            if not data:
                value[dt] = 0.0
            else:
                value[dt] = round(data, 2)
            for dt in sorted(financials["TotalDebt"].keys()):
                if dt in financials["StockholdersEquity"]:
                    debt = float(financials["TotalDebt"][dt])
                    equity = float(financials["StockholdersEquity"][dt])
                    if isnan(debt) or isnan(equity) or equity == 0.0:
                        continue
                    value[dt] = round(100 * debt / equity, 2)
        except Exception as e:
            print(f"D/E. {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
        value = dict(sorted(value.items(), reverse=True))
        return value

    '''
    Free Cash Flow (FCF) :
    1. via freeCashflow information of the Info -> Based on yearly performance
    2. based on "Free Cash Flow" or "Operating Cash Flow"-"Capital Expenditure" of the Financial part,
        Second value will give you a YTD information of the last reported year in financial 
    '''

    @staticmethod
    def get_fcf(infos, financials, timeseries):
        value = {}
        try:
            dt = datetime.datetime.today().strftime("%Y-%m-%d")
            data = infos["freeCashflow"]
            if not data:
                value[dt] = 0.0
            else:
                value[dt] = round(data, 2)
            for dt in sorted(financials["FreeCashFlow"].keys()):
                fcf = float(financials["FreeCashFlow"][dt])
                if fcf == 0.0 or isnan(fcf):
                    if dt in financials["OperatingCashFlow"] and dt in financials["CapitalExpenditure"]:
                        fcf = float(financials["OperatingCashFlow"][dt] + financials["CapitalExpenditure"][dt])
                        if isnan(fcf):
                            value[dt] = 0.0
                        else:
                            value[dt] = round(fcf, 2)
                else:
                    value[dt] = round(fcf, 2)
        except Exception as e:
            print(f"FCF. {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
        value = dict(sorted(value.items(), reverse=True))
        return value

    '''
    Price-to-Earnings (P/E) Ratio :
    This Compares the stock's value to its earnings : tells you health of a company good > 15-20
    1. This value is a ratio of currentPrice/trailingEps from Info 
    2. Calculated by:
        from financial "Diluted EPS"
        from time seris price on the day of earning
        pe = price/eps (average for the year of statement)
        This will give an average of YTD P/E based on last statment year
    '''

    def get_pe_ratio(self, infos, financials, timeseries):
        value = {}
        try:
            dt = datetime.datetime.today().strftime("%Y-%m-%d")
            eps = infos["trailingEps"]
            if not eps:
                eps = 0.0
            else:
                eps = float(eps)
            price = infos["currentPrice"]
            if not price:
                price = 0.0
            else:
                price = float(price)
            if not isnan(price) and not isnan(eps) and eps != 0.0:
                value[dt] = round(price / eps, 2)
            for dt in sorted(financials["DilutedEPS"].keys()):
                date_price = self.get_timeseries_entry(timeseries, dt, "High")
                if date_price == 0.0:
                    date_price = float(price)
                date_eps = financials["DilutedEPS"][dt]
                if date_eps != 0.0 and not isnan(date_eps):
                    value[dt] = round(date_price / date_eps, 2)
        except Exception as e:
            print(f"P/E. {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
        value = dict(sorted(value.items(), reverse=True))
        return value

    '''
    Price-to-Sales (P/S) Ratio :
    This gives Market capitalization divided by revenue 
    1. This value is taken from priceToSalesTrailing12Months in Info 
    2. Calculated by 
        from financial Market capitalization = "Basic Average Shares" * share price on the day
        from financial "Total Revenue" in each statement time
        ps = Market Cap/Total Revenue
        second value will provide an average of YTD P/E based on last statment year
    '''

    def get_ps_ratio(self, infos, financials, timeseries):
        value = {}
        try:
            dt = datetime.datetime.today().strftime("%Y-%m-%d")
            data = infos["priceToSalesTrailing12Months"]
            if not data:
                value[dt] = 0.0
            else:
                value[dt] = round(data, 2)
            for dt in sorted(financials["TotalRevenue"].keys()):
                rev = financials["TotalRevenue"][dt]
                date_price = self.get_timeseries_entry(timeseries, dt, "High")
                if date_price == 0.0 or isnan(date_price):
                    date_price = float(infos["currentPrice"])
                vol = 0.0
                if dt in financials["BasicAverageShares"]:
                    vol = float(financials["BasicAverageShares"][dt])
                if vol == 0.0 or isnan(vol):
                    vol = float(infos["sharesOutstanding"])
                marcap = date_price * vol
                if rev != 0.0 and not isnan(rev):
                    value[dt] = round(marcap / rev, 2)
        except Exception as e:
            print(f"P/S. {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
        value = dict(sorted(value.items(), reverse=True))
        return value

    '''
    Price-to-Book (P/B) Ratio :
    Stock price divided by book value per share. A ratio between 1 and 3 often indicates fair valuation.
    1. This value is taken from priceToBook in Info
    2. Calculate by
        "Total Equity Gross Minority Interest" divided by "Total Revenue" in each statement time, 
        Market capitalization is calculated as follows:
        from financials get Book value per share using "Basic Average Shares"/"Basic Average Shares" 
        from timeseries price on that day
        pb = price/Book value per share
    '''

    def get_pb_ratio(self, infos, financials, timeseries):
        value = {}
        try:
            dt = datetime.datetime.today().strftime("%Y-%m-%d")
            data = infos["priceToBook"]
            if not data:
                value[dt] = 0.0
            else:
                value[dt] = round(data, 2)
            for dt in sorted(financials["TotalEquityGrossMinorityInterest"].keys()):
                tot_eqs = financials["TotalEquityGrossMinorityInterest"][dt]
                date_price = self.get_timeseries_entry(timeseries, dt, "High")
                if date_price == 0.0 or isnan(date_price):
                    date_price = float(infos["currentPrice"])
                vol = 0.0
                if dt in financials["BasicAverageShares"]:
                    vol = float(financials["BasicAverageShares"][dt])
                if vol == 0.0 or isnan(vol):
                    vol = float(infos["sharesOutstanding"])
                if tot_eqs != 0.0 and not isnan(tot_eqs):
                    value[dt] = round(date_price / (tot_eqs / vol), 2)
        except Exception as e:
            print(f"P/B. {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
        value = dict(sorted(value.items(), reverse=True))
        return value

    '''
    Return on Equity (ROE) :
    Measures how efficiently the company generates profits from shareholder equity. 
    A good ROE is often 10-20%.
    1. This value is taken from returnOnEquity in Info
    2. Calculate by
        "Net Income" divided by yearly average "Stockholders Equity" from each statement time, 
        Market capitalization is calculated as follows:
        from financials get "Net Income" and yearly average "Stockholders Equity" 
        roe = net income/stockholders equity
    '''

    @staticmethod
    def get_roe(infos, financials, timeseries):
        value = {}
        try:
            dt = datetime.datetime.today().strftime("%Y-%m-%d")
            data = infos["returnOnEquity"]
            if not data:
                value[dt] = 0.0
            else:
                value[dt] = round(data, 2)
            for dt in sorted(financials["NetIncome"].keys()):
                net_inc = financials["NetIncome"][dt]
                tot_eq = 0.0
                if dt in financials["StockholdersEquity"]:
                    tot_eq = float(financials["StockholdersEquity"][dt])
                if tot_eq != 0.0 and not isnan(tot_eq) and not isnan(net_inc):
                    value[dt] = round(net_inc / tot_eq, 2)
        except Exception as e:
            print(f"ROE. {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
        value = dict(sorted(value.items(), reverse=True))
        return value

    '''
    Dividend Payout Ratio :
    This assess the dividend yield and whether the company can sustain its dividends 
    (payout ratio comfortably below 60-70%)
    1. This value is taken from dividendRate/trailingEps in Info
    2. Calculate by
        from financials get total yearly "Cash Dividends Paid" and total yearly "Net Income" 
        div payout ratio = Cash Dividends Paid/Net Income
    '''

    @staticmethod
    def get_div_payout(infos, financials, timeseries):
        value = {}
        try:
            dt = datetime.datetime.today().strftime("%Y-%m-%d")
            divr = infos["dividendRate"]
            if not divr:
                divr = 0.0
            else:
                divr = float(divr)
            eps = infos["trailingEps"]
            if not eps:
                eps = 0.0
            else:
                eps = float(eps)
            if not isnan(eps) and not isnan(divr) and divr != 0.0:
                value[dt] = round(eps / divr, 2)
            for dt in sorted(financials["CashDividendsPaid"].keys()):
                divp = -1.0 * financials["CashDividendsPaid"][dt]
                if dt in financials["NetIncome"]:
                    net_inc = float(financials["NetIncome"][dt])
                    if net_inc != 0.0 and not isnan(net_inc) and not isnan(divp):
                        value[dt] = round(divp / net_inc, 2)
        except Exception as e:
            print(f"DIV PAY. {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
        value = dict(sorted(value.items(), reverse=True))
        return value

    '''
    Current Ratio :
    This Measure the ability to meet short-term obligations 
    (a current ratio above 1.5 is often good)

    1. This value is taken from currentRatio in Info
    2. Calculate by
        from financials get  yearly "Total Assets" and yearly "Total Liabilities Net Minority Interest" 
        cur_ratio = Total Assets/Total Liabilities Net Minority Interest
    '''

    @staticmethod
    def get_cur_ratio(infos, financials, timeseries):
        value = {}
        try:
            dt = datetime.datetime.today().strftime("%Y-%m-%d")
            data = infos["currentRatio"]
            if not data:
                value[dt] = 0.0
            else:
                value[dt] = round(data, 2)
            for dt in sorted(financials["TotalAssets"].keys()):
                tot_ass = financials["TotalAssets"][dt]
                if dt in financials["TotalLiabilitiesNetMinorityInterest"]:
                    tot_lia = float(financials["TotalLiabilitiesNetMinorityInterest"][dt])
                    if tot_lia != 0.0 and not isnan(tot_lia) and not isnan(tot_ass):
                        value[dt] = round(tot_ass / tot_lia, 2)
        except Exception as e:
            print(f"CUR RAT. {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
        value = dict(sorted(value.items(), reverse=True))
        return value

    '''
    Paid Dividend per share:
    1. This value is taken from dividendRate in Info
    2. Calculate by
        from financials get  yearly "Cash Dividends Paid" and  "Basic Average Shares" 
        cur_ratio = Cash Dividends Paid/Basic Average Shares
    '''

    @staticmethod
    def get_div(infos, financials, timeseries):
        value = {}
        try:
            dt = datetime.datetime.today().strftime("%Y-%m-%d")
            data = infos["dividendRate"]
            if not data:
                value[dt] = 0.0
            else:
                value[dt] = round(data, 2)
            for dt in sorted(financials["CashDividendsPaid"].keys()):
                tot_div = float(financials["CashDividendsPaid"][dt])
                if dt in financials["BasicAverageShares"]:
                    vol = float(financials["BasicAverageShares"][dt])
                    if vol != 0.0 and not isnan(vol) and not isnan(tot_div):
                        value[dt] = round(-1.0 * tot_div / vol, 2)
        except Exception as e:
            print(f"DIVIDEND. {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
        value = dict(sorted(value.items(), reverse=True))
        return value

    '''
    EBITDA :
    1. This value is taken from ebitda in Info
    2. Calculate by
        from financials get average yearly "EBITDA" 
    '''

    @staticmethod
    def get_ebitda(infos, financials, timeseries):
        value = {}
        try:
            dt = datetime.datetime.today().strftime("%Y-%m-%d")
            data = infos["ebitda"]
            if not data:
                value[dt] = 0.0
            else:
                value[dt] = round(data, 2)
            for dt in sorted(financials["EBITDA"].keys()):
                ebitxx = float(financials["EBITDA"][dt])
                if ebitxx == 0.0 or isnan(ebitxx):
                    if dt in financials["EBIT"]:
                        ebitxx = float(financials["EBIT"][dt])
                if ebitxx != 0.0 and not isnan(ebitxx):
                    value[dt] = round(ebitxx, 2)
        except Exception as e:
            print(f"EBITDA. {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
        value = dict(sorted(value.items(), reverse=True))
        return value

    def get_market_cap(self, infos, financials, timeseries):
        value = {}
        try:
            dt = datetime.datetime.today().strftime("%Y-%m-%d")
            data = infos["marketCap"]
            if not data:
                value[dt] = 0.0
            else:
                value[dt] = round(data, 2)
            for dt in sorted(financials["BasicAverageShares"].keys()):
                vol = float(financials["BasicAverageShares"][dt])
                if vol == 0.0 or isnan(vol):
                    vol = float(infos["sharesOutstanding"])
                date_price = self.get_timeseries_entry(timeseries, dt, "High")
                if date_price == 0.0 or isnan(date_price):
                    date_price = float(infos["currentPrice"])
                marcap = date_price * vol
                if marcap != 0.0 and not isnan(marcap):
                    value[dt] = round(marcap, 2)
        except Exception as e:
            print(f"MAR CAP. {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
        value = dict(sorted(value.items(), reverse=True))
        return value

    @staticmethod
    def get_cash(infos, financials, timeseries):
        value = {}
        try:
            dt = datetime.datetime.today().strftime("%Y-%m-%d")
            data = infos["totalCash"]
            if not data:
                value[dt] = 0.0
            else:
                value[dt] = round(data, 2)

            for dt in sorted(financials["CashAndCashEquivalents"].keys()):
                cace = float(financials["CashAndCashEquivalents"][dt])
                if cace != 0.0 and not isnan(cace):
                    value[dt] = round(cace, 2)
        except Exception as e:
            print(f"TOT CASH. {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
        value = dict(sorted(value.items(), reverse=True))
        return value

    def update_rating_items(self, infos: dict, financials: dict, timeseries: dict):
        calc_values = {}
        cal_functions = {
            "RevenueGrowth": self.get_rev_growth,
            "ProfitGrowth": self.get_profit_growth,
            "EarningsPerShareEPS": self.get_eps,
            "AssetsVsLiabilities": self.get_ass_v_lia,
            "DebtToEquityRatioDE": self.get_de_ratio,
            "FreeCashFlowFCF": self.get_fcf,
            "PriceToEarningsPERatio": self.get_pe_ratio,
            "PriceToSalesPSRatio": self.get_ps_ratio,
            "PriceToBookPBRatio": self.get_pb_ratio,
            "ReturnonEquityROE": self.get_roe,
            "DividendPayoutRatio": self.get_div_payout,
            "CurrentRatio": self.get_cur_ratio,
            "MarketCap": self.get_market_cap,
            "PaidDividend": self.get_div,
            "TotalCash": self.get_cash,
            "EBITxx": self.get_ebitda,
        }
        for key in cal_functions.keys():
            calc_values[key] = cal_functions[key](infos, financials, timeseries)
        return calc_values

