import datetime
import json
import sys
from typing import Any
from math import isnan
import os

sys.path.append(os.path.dirname(__file__))
from source.basicStockData import BasicStockData


class CompanyDataCollector:
    def __init__(self, company_info_file, ticker_info_file):
        """
        Enhanced company data collector that gathers comprehensive information
        including ISIN, WKN, name, exchange, ticker, sector, and industry.
        """
        self.ticker_info_file = ticker_info_file  # JSON file with all ticker to process
        self.all_ticker_symbols = {}
        self.company_info_file = company_info_file  # JSON file to write out the company information
        self.company_info = {}
        # Load existing information
        self.load_all_ticker_symbols()
        if not self.all_ticker_symbols:
            print(f'No Tickers found to be processed')

    def load_current_company_information(self):
        self.company_info = {}
        try:
            with open(self.company_info_file, 'r', encoding='utf-8') as f:
                self.company_info = json.load(f)
                f.close()
        except Exception as e:
            print(f'No company_info.json found {e}')

    def load_all_ticker_symbols(self):
        self.all_ticker_symbols = []
        with open(self.ticker_info_file, 'r', encoding='utf-8') as f:
            self.all_ticker_symbols = json.load(f)
            f.close()
        if not self.all_ticker_symbols:
            print(f'No tickers found to process in file {f.name}')
            return

    @staticmethod
    def get_timeseries(timeseries: dict, date_key: str, tag: str) -> float:
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
    def get_rev_growth(infos: dict, financials: dict, timeseries: dict) -> dict[Any, Any]:
        value = {}
        try:
            dt = datetime.datetime.today().strftime("%Y-%m-%d")
            data = infos["revenueGrowth"]
            if not data:
                value[dt] = 0.0
            else:
                value[dt] = round(data, 2)
            pre_val = 0.0
            for dt in sorted(financials["Total Revenue"].keys()):
                cur_val = float(financials["Total Revenue"][dt])
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
            for date_key in sorted(financials["Gross Profit"].keys()):
                cur_val = float(financials["Gross Profit"][date_key])
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
            for dt in sorted(financials["Diluted EPS"].keys()):
                cur_val = float(financials["Diluted EPS"][dt])
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
            for dt in sorted(financials["Total Assets"].keys()):
                if dt in financials["Total Liabilities Net Minority Interest"]:
                    aval = float(financials["Total Assets"][dt])
                    lval = float(financials["Total Liabilities Net Minority Interest"][dt])
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
            for dt in sorted(financials["Total Debt"].keys()):
                if dt in financials["Stockholders Equity"]:
                    debt = float(financials["Total Debt"][dt])
                    equity = float(financials["Stockholders Equity"][dt])
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
            for dt in sorted(financials["Free Cash Flow"].keys()):
                fcf = float(financials["Free Cash Flow"][dt])
                if fcf == 0.0 or isnan(fcf):
                    if dt in financials["Operating Cash Flow"] and dt in financials["Capital Expenditure"]:
                        fcf = float(financials["Operating Cash Flow"][dt] + financials["Capital Expenditure"][dt])
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
            for dt in sorted(financials["Diluted EPS"].keys()):
                date_price = self.get_timeseries(timeseries, dt, "High")
                if date_price == 0.0:
                    date_price = float(price)
                date_eps = financials["Diluted EPS"][dt]
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
            for dt in sorted(financials["Total Revenue"].keys()):
                rev = financials["Total Revenue"][dt]
                date_price = self.get_timeseries(timeseries, dt, "High")
                if date_price == 0.0 or isnan(date_price):
                    date_price = float(infos["currentPrice"])
                vol = 0.0
                if dt in financials["Basic Average Shares"]:
                    vol = float(financials["Basic Average Shares"][dt])
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
            for dt in sorted(financials["Total Equity Gross Minority Interest"].keys()):
                tot_eqs = financials["Total Equity Gross Minority Interest"][dt]
                date_price = self.get_timeseries(timeseries, dt, "High")
                if date_price == 0.0 or isnan(date_price):
                    date_price = float(infos["currentPrice"])
                vol = 0.0
                if dt in financials["Basic Average Shares"]:
                    vol = float(financials["Basic Average Shares"][dt])
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
            for dt in sorted(financials["Net Income"].keys()):
                net_inc = financials["Net Income"][dt]
                tot_eq = 0.0
                if dt in financials["Stockholders Equity"]:
                    tot_eq = float(financials["Stockholders Equity"][dt])
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
            for dt in sorted(financials["Cash Dividends Paid"].keys()):
                divp = -1.0 * financials["Cash Dividends Paid"][dt]
                if dt in financials["Net Income"]:
                    net_inc = float(financials["Net Income"][dt])
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
            for dt in sorted(financials["Total Assets"].keys()):
                tot_ass = financials["Total Assets"][dt]
                if dt in financials["Total Liabilities Net Minority Interest"]:
                    tot_lia = float(financials["Total Liabilities Net Minority Interest"][dt])
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
            for dt in sorted(financials["Cash Dividends Paid"].keys()):
                tot_div = float(financials["Cash Dividends Paid"][dt])
                if dt in financials["Basic Average Shares"]:
                    vol = float(financials["Basic Average Shares"][dt])
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
            for dt in sorted(financials["Basic Average Shares"].keys()):
                vol = float(financials["Basic Average Shares"][dt])
                if vol == 0.0 or isnan(vol):
                    vol = float(infos["sharesOutstanding"])
                date_price = self.get_timeseries(timeseries, dt, "High")
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

            for dt in sorted(financials["Cash And Cash Equivalents"].keys()):
                cace = float(financials["Cash And Cash Equivalents"][dt])
                if cace != 0.0 and not isnan(cace):
                    value[dt] = round(cace, 2)
        except Exception as e:
            print(f"TOT CASH. {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
        value = dict(sorted(value.items(), reverse=True))
        return value

    def update_rating_items(self, infos: dict, financials: dict, timeseries: dict):
        calc_values = {}
        cal_functions = {
            "Revenue Growth": self.get_rev_growth,
            "Profit Growth": self.get_profit_growth,
            "Earnings Per Share (EPS)": self.get_eps,
            "Assets vs. Liabilities": self.get_ass_v_lia,
            "Debt-to-Equity Ratio (D/E)": self.get_de_ratio,
            "Free Cash Flow (FCF)": self.get_fcf,
            "Price-to-Earnings (P/E) Ratio": self.get_pe_ratio,
            "Price-to-Sales (P/S) Ratio": self.get_ps_ratio,
            "Price-to-Book (P/B) Ratio": self.get_pb_ratio,
            "Return on Equity (ROE)": self.get_roe,
            "Dividend Payout Ratio": self.get_div_payout,
            "Current Ratio": self.get_cur_ratio,
            "Market Cap": self.get_market_cap,
            "Paid Dividend": self.get_div,
            "Total Cash": self.get_cash,
            "EBITxx": self.get_ebitda,
        }
        for key in cal_functions.keys():
            calc_values[key] = cal_functions[key](infos, financials, timeseries)
        return calc_values

    def collect_company_information(self, ticker: str):
        companies_data = {}
        bsd = BasicStockData()
        status, data = bsd.get_company_info(ticker)
        if status:
            companies_data[ticker] = {}
            if "google_ticker" not in data:
                data["google_ticker"] = self.all_ticker_symbols[ticker]

            companies_data[ticker]["info"] = data
            status, data = bsd.get_company_timeseries(ticker)
            if status:
                data = bsd.update_ts_indicators(data)
                companies_data[ticker]["timeseries"] = data
            status, data = bsd.get_company_financials(ticker)
            if status:
                companies_data[ticker]["financials"] = data
                companies_data[ticker]["ratings"] = self.update_rating_items(companies_data[ticker]["info"],
                                                                             companies_data[ticker]["financials"],
                                                                             companies_data[ticker]["timeseries"])
        return [status, companies_data]

    def start_processing(self):
        missing_tickers = []
        counter = 1
        for ticker in self.all_ticker_symbols.keys():
            self.load_current_company_information()
            to_update = False
            if ticker in self.company_info:
                if "info" not in self.company_info[ticker]:
                    to_update = True
                elif "financials" not in self.company_info[ticker]:
                    to_update = True
                elif "timeseries" not in self.company_info[ticker]:
                    to_update = True
                else:
                    print(f'Processing ticker {ticker}... already processed')
            else:
                to_update = True
            if to_update:
                print(f'Processing ticker {counter} {ticker}...')
                status, data = self.collect_company_information(ticker)
                self.company_info.update(data)
                if not status:
                    missing_tickers.append(ticker)
                # self.ticker_obj = None
                counter += 1
                with open(self.company_info_file, 'w', encoding='utf-8') as f:
                    f.write(json.dumps(self.company_info, indent=4))
                    f.close()
            # if counter > 2:
            #     break
        print(f'Ticker with no information {missing_tickers}')

