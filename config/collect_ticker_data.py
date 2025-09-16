import datetime
import json

import math
import yfinance
from math import isnan
from numpy.ma.extras import average


class CompanyDataCollector:
    def __init__(self, company_info_file, ticker_info_file):
        """
        Enhanced company data collector that gathers comprehensive information
        including ISIN, WKN, name, exchange, ticker, sector, and industry.
        """
        self.ticker_obj = None
        self.ticker_info_file = ticker_info_file # JSON file with all ticker to process
        self.all_ticker_symbols = {}
        self.company_info_file = company_info_file # JSON file to write out the company information
        self.company_info = {}
        self.info_keys = [
            "symbol",
            "google_ticker",
            "longName",
            "shortName",
            "country",
            "industry",
            "sector",
            "language",
            "currency",
            "exchangeTimezoneShortName",
            "fullExchangeName",
            "market",
            "currentPrice",
            "sharesOutstanding",
            "marketCap",
            "debtToEquity",
            "revenueGrowth",
            "trailingEps",
            "freeCashflow",
            "priceToBook",
            "ebitda",
            "priceToSalesTrailing12Months",
            "returnOnEquity",
            "dividendRate",
            "currentRatio",
            "totalCash",
            "volume"
            ]
        self.financials_keys = [
            "Basic Average Shares",
            "EBIT",
            "EBITDA",
            "Gross Profit",
            "Net Income",
            "Diluted EPS",
            "Cash And Cash Equivalents",
            "Total Revenue",
            "Stockholders Equity",
            "Total Assets",
            "Total Debt",
            "Total Equity Gross Minority Interest",
            "Total Liabilities Net Minority Interest",
            "Capital Expenditure",
            "Cash Dividends Paid",
            "Free Cash Flow",
            "Operating Cash Flow"
        ]
        # Load existing information
        self.load_all_ticker_symbols()
        # self.load_current_company_information()
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

    def update_data_keys(self, data: dict, tag):
        new_data = {}
        if tag == 1: # Checking the keys in Info area
            for key in self.info_keys:
                if key in data:
                        new_data[key] = data[key]
                else:
                    new_data[key] = ""
        if tag == 2:
            date_tags = []
            for key in self.financials_keys:
                if key in data:
                    new_data[key] = data[key]
                    if len(date_tags) < len(data[key].keys()):
                        date_tags = data[key].keys()
                else:
                    new_data[key] = {}
            if date_tags:
                to_fill = {}
                for k in date_tags:
                    to_fill[k] = "Nan"
                for key in new_data.keys():
                    if not new_data[key]:
                        new_data[key] = to_fill
        return new_data

    def get_company_info(self, ticker: str):
        # print(f"+ Getting Company Information for {ticker}")
        company_info = {}
        status = True
        try:
            self.ticker_obj = yfinance.Ticker(ticker)
            company_info = self.ticker_obj.info
            if not company_info:
                raise Exception("Can't retrieve Company Information")
            else:
                if "google_ticker" not in company_info:
                    company_info["google_ticker"] = self.all_ticker_symbols[ticker]
                company_info = self.update_data_keys(company_info, 1)
        except Exception as e:
            status = False
            print(f"- Error for {ticker} : {e}")
        return [status, company_info]

    def get_company_timeseries(self, ticker: str) :
        # print(f"+ Getting Company Timeseries for {ticker}")
        company_ts = {}
        status = True
        try:
            self.ticker_obj = yfinance.Ticker(ticker)
            tdy = datetime.datetime.today()
            str_date = datetime.date(year=tdy.year-2,month=tdy.month, day=tdy.day-1).strftime('%Y-%m-%d')
            end_date = datetime.date(year=tdy.year,month=tdy.month, day=tdy.day-1).strftime('%Y-%m-%d')
            data = self.ticker_obj.history(start=str_date, end=end_date)
            if data.empty:
                raise Exception("Can't retrieve Company Timeseries")
            else:
                for ent_date, ent_row in data.iterrows():
                    date_data = {
                        "Open" : ent_row["Open"],
                        "High" : ent_row["High"],
                        "Low" : ent_row["Low"],
                        "Close" : ent_row["Close"],
                        "Volume" : ent_row["Volume"]
                    }
                    company_ts[ent_date.date().strftime("%Y-%m-%d")] = date_data
        except Exception as e:
            status = False
            print(f"- An unexpected error occurred for {ticker} in retrieving Company TS : {e}")
        if not company_ts:
            status = False
        return [status, company_ts]

    def get_company_financials(self, ticker: str) :
        company_fin = {}
        status = True
        try:
            self.ticker_obj = yfinance.Ticker(ticker)
            """
            Financial Results
            """
            data = self.ticker_obj.get_financials(as_dict=True, pretty=True, freq="yearly")
            data_q = self.ticker_obj.get_financials(as_dict=True, pretty=True, freq="quarterly")
            data.update(data_q)
            if data:
                company_fin.update(self.dataframe_to_dict(data))
            """
            Balance Sheet  Results
            """
            data = self.ticker_obj.get_balance_sheet(as_dict=True, pretty=True, freq="yearly")
            data_q = self.ticker_obj.get_balance_sheet(as_dict=True, pretty=True, freq="quarterly")
            data.update(data_q)
            if data:
                company_fin.update(self.dataframe_to_dict(data))
            """
            Cash Flow Results
            """
            data = self.ticker_obj.get_cash_flow(as_dict=True, pretty=True, freq="yearly")
            data_q = self.ticker_obj.get_cash_flow(as_dict=True, pretty=True, freq="quarterly")
            data.update(data_q)
            if data:
                company_fin.update(self.dataframe_to_dict(data))
            # pprint.pprint(company_fin)
            company_fin = self.update_data_keys(company_fin, 2)
            if not company_fin:
                raise Exception("Missing needed keys")
        except Exception as e:
            status = False
            print(f"- An unexpected error occurred for {ticker} in retrieving Company Financials : {e}")

        return [status, company_fin]

    @staticmethod
    def get_timeseries(timeseries, data, tag):
        value = 0.0
        if data in timeseries:
            value = timeseries[data][tag]
        else:
            k = "-".join(data.split("-")[:-1])
            red_ts = {}
            for key in timeseries.keys():
                if k in key:
                    red_ts[key] = timeseries[key]
            for key in sorted(red_ts.keys(), reverse=True):
                value = red_ts[key][tag]
                if not isnan(value):
                    break
        return value

    @staticmethod
    def dataframe_to_dict(data):
        fin_data = {}
        for tkey in data.keys():
            for vkey in data[tkey].keys():
                if not vkey in fin_data:
                    fin_data[vkey] = {}
                fin_data[vkey][tkey.date().strftime("%Y-%m-%d")] = data[tkey][vkey]
        for vkey in fin_data.keys():
            data = fin_data[vkey]
            fin_data[vkey] = dict(sorted(data.items(), reverse=True))
        return fin_data

    @staticmethod
    def fetch_last_data(data:dict, items:int, as_dict:bool=False) -> list | dict:
        vals = {}
        try:
            keys = sorted(data.keys(), reverse=True)
            for i in range(0,items):
                if i < len(keys):
                    try:
                        data[keys[i]] = 0.0 if data[keys[i]] == "Nan" else float(data[keys[i]])
                        data[keys[i]] = 0.0 if math.isnan(data[keys[i]]) else float(data[keys[i]])
                    except Exception as e:
                        data[keys[i]] = 0.0
                    vals[keys[i]] = data[keys[i]]
                # else:
                #     vals[keys[i]] = 0.0
        except Exception as e:
            print(f"Fetch Data : Error in data {e}")
        if as_dict:
            vals = dict(sorted(vals.items(), reverse=True))
            return vals
        else:
            return list(vals.values())

    '''
    Revenue Growth is calculated based on current and previous value,
    be it yearwise or quarter wise, this will give a comparison about how the company did in 
    last year or last quarter.
    '''
    def get_rev_growth(self, infos, financials, timeseries):
        value = [0.0,0.0]
        try:
            data = infos["revenueGrowth"]
            if not data:
                value[0] = 0.0
            else:
                value[0] = float(data)
            [cur_val, pre_val] = self.fetch_last_data(financials["Total Revenue"], 2)
            if pre_val != 0.0:
                value[1] = (cur_val-pre_val)/pre_val
        except Exception as e:
            print(f"RevG. Error in data {e}")
        return value

    '''
    Profit Growth is calculated based on current and previous value,
    be it yearwise or quarter wise, this will give a comparison about how the company did in 
    last year or last quarter.
    As there is no indicator in the Info it is calculated based on Financial Results.
    When there is no Financial Results it is equal to Revenue Growth.
    '''
    def get_profit_growth(self, infos, financials, timeseries):
        value = [0.0,0.0]
        try:
            [cur_val, pre_val] = self.fetch_last_data(financials["Gross Profit"], 2)
            if pre_val != 0.0:
                value[1] = (cur_val-pre_val)/pre_val
            if value[1] == 0.0:
                value = self.get_rev_growth(infos, financials, timeseries)
        except Exception as e:
            print(f"ProftG. Error in data {e}")
        return value

    '''
    Earning per Share is calculated :
    1. via trailingEps information of the Info -> Based on yearly performance
    2. based on Diluted EPS of the Financial part 
        This will give last statement performance
    These two value can give you information about how was the company in last 12 months and in last 3 months
    '''
    def get_eps(self, infos, financials, timeseries):
        value = [0.0,0.0]
        try:
            data = infos["trailingEps"]
            if not data:
                value[0] = 0.0
            else:
                value[0] = float(data)
            dil_eps = self.fetch_last_data(financials["Diluted EPS"], 4, as_dict=True)
            for eps in sorted(dil_eps.keys(), reverse=True):
                if dil_eps[eps] != 0.0:
                    value[1] = dil_eps[eps]
                    break
        except Exception as e:
            print(f"EPS. Error in data {e}")
        return value

    '''
    Assets vs. Liabilities :
    This is just a ratio of Total Assets and Total Liabilities Net Minority Interest
    Two values gives you a trend from last two statements
    '''
    def get_ass_v_lia(self, infos, financials, timeseries):
        value = [0.0,0.0]
        try:
            [a_cur_val, a_pre_val] = self.fetch_last_data(financials["Total Assets"], 2)
            [l_cur_val, l_pre_val] = self.fetch_last_data(financials["Total Liabilities Net Minority Interest"], 2)
            if l_cur_val != 0.0:
                value[1] = a_cur_val/l_cur_val
            else:
                value[1] = 0.0
            if l_pre_val != 0.0:
                value[0] = a_pre_val/l_pre_val
            else:
                value[0] = 0.0
        except Exception as e:
            print(f"A/L. Error in data {e}")
        return value

    '''
    Debt-to-Equity Ratio (D/E) :
    1. via debtToEquity information of the Info -> Based on yearly performance
    2. based on Net Income and Basic Average Share of the Financial part, this D/E ratio is average of the last reported year
    '''
    def get_de_ratio(self, infos, financials, timeseries):
        value = [0.0,0.0]
        try:
            data = infos["debtToEquity"]
            if not data:
                value[0] = 0.0
            else:
                value[0] = float(data)
            t_debt = self.fetch_last_data(financials["Total Debt"], 8, as_dict=True)
            y_debt = {}
            for k in t_debt.keys():
                if "-" in k:
                    year = k.split("-")[0]
                    debt = t_debt[k]
                    if k in financials["Stockholders Equity"]:
                        equity = financials["Stockholders Equity"][k]
                        if equity != 0.0:
                            if not year in y_debt:
                                y_debt[year] = []
                            y_debt[year].append(debt/equity)
            for k in sorted(y_debt.keys(), reverse=True):
                value[1] = math.fsum(y_debt[k])/len(y_debt[k])
                break
            value[1] *= 100
        except Exception as e:
            print(f"D/E. Error in data {e}")
        return value

    '''
    Free Cash Flow (FCF) :
    1. via freeCashflow information of the Info -> Based on yearly performance
    2. based on "Free Cash Flow" or "Operating Cash Flow"-"Capital Expenditure" of the Financial part,
        Second value will give you a YTD information of the last reported year in financial 
    '''
    def get_fcf_ratio(self, infos, financials, timeseries):
        value = [0.0,0.0]
        try:
            data = infos["freeCashflow"]
            if not data:
                value[0] = 0.0
            else:
                value[0] = float(data)
            qfcf = self.fetch_last_data(financials["Free Cash Flow"], 4, as_dict=True)
            fcf = {}
            for k in qfcf.keys():
                year = k.split("-")[0]
                if year not in fcf:
                    fcf[year] = 0.0
                fcf[year] += qfcf[k]
            if fcf:
                value[1] = fcf[sorted(fcf.keys(), reverse=True)[0]]
            else:
                ocf = self.fetch_last_data(financials["Operating Cash Flow"], 4, as_dict=True)
                for k in ocf:
                    year = k.split("-")[0]
                    if year not in fcf:
                        fcf[year] = 0.0
                    fcf[year] += ocf[k]
                    if k in financials["Capital Expenditure"]:
                        fcf[year] -= financials["Capital Expenditure"][k]
                if fcf:
                    value[1] = fcf[sorted(fcf.keys(), reverse=True)[0]]
        except Exception as e:
            print(f"FCF. Error in data {e}")
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
        value = [0.0,0.0]
        try:
            eps = infos["trailingEps"]
            if not eps:
                eps = 0.0
            price = infos["currentPrice"]
            if not price:
                price = 0.0
            if eps != 0.0:
                value[0] = price/eps
            dil_epss = self.fetch_last_data(financials["Diluted EPS"], 8, as_dict=True)
            pe_yearly = {}
            for k in dil_epss.keys():
                if "-" in k:
                    year = k.split("-")[0]
                    price = self.get_timeseries(timeseries, k, "High")
                    if price == 0.0:
                        price = infos["currentPrice"]
                    dil_eps = dil_epss[k]
                    if dil_eps != 0.0:
                        pe_c = price/dil_eps
                        if year not in pe_yearly:
                            pe_yearly[year] = []
                        pe_yearly[year].append(pe_c)
            for k in sorted(pe_yearly.keys(), reverse=True):
                value[1] = math.fsum(pe_yearly[k])/len(pe_yearly[k])
                break
        except Exception as e:
            print(f"P/E. Error in data {e}")
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
        value = [0.0,0.0]
        try:
            data = infos["priceToSalesTrailing12Months"]
            if not data:
                value[0] = 0.0
            else:
                value[0] = float(data)
            revenue = self.fetch_last_data(financials["Total Revenue"], 4, as_dict=True)
            rev_yearly = {}
            marcap_yearly = {}
            for k in revenue.keys():
                if "-" in k:
                    year = k.split("-")[0]
                    vol = 0.0
                    if k in financials["Basic Average Shares"]:
                        vol = financials["Basic Average Shares"][k]
                    if vol == 0.0 or isnan(vol):
                        vol = infos["sharesOutstanding"]
                    price = self.get_timeseries(timeseries, k, "High")
                    if price == 0.0 or isnan(vol):
                        price = infos["currentPrice"]
                    if year not in marcap_yearly:
                        marcap_yearly[year] = []
                    marcap_yearly[year].append(price*vol)
                    if year not in rev_yearly:
                        rev_yearly[year] = 0.0
                    rev_yearly[year] += revenue[k]
            for k in sorted(marcap_yearly.keys(), reverse=True):
                if k in rev_yearly:
                    if marcap_yearly[k] and rev_yearly[k] != 0.0:
                        value[1] = average(marcap_yearly[k])/rev_yearly[k]
                    break
        except Exception as e:
            print(f"P/S. Error in data {e}")
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
        value = [0.0,0.0]
        try:
            data = infos["priceToBook"]
            if not data:
                value[0] = 0.0
            else:
                value[0] = float(data)
            tot_eqs = self.fetch_last_data(financials["Total Equity Gross Minority Interest"], 8, as_dict=True)
            yearly_pbs = {}
            for k in tot_eqs.keys():
                if "-" in k:
                    year = k.split("-")[0]
                    vol = 0.0
                    if k in financials["Basic Average Shares"]:
                        vol = financials["Basic Average Shares"][k]
                    if vol == 0.0 or isnan(vol):
                        vol = infos["sharesOutstanding"]
                    price = self.get_timeseries(timeseries, k, "High")
                    if price == 0.0:
                        price = infos["currentPrice"]
                    if year not in yearly_pbs:
                        yearly_pbs[year] = []
                    if tot_eqs[k] != 0.0 and vol != 0.0:
                        yearly_pbs[year].append(price/(tot_eqs[k]/vol))
            # print(yearly_pbs)
            for k in sorted(yearly_pbs.keys(), reverse=True):
                if yearly_pbs[k]:
                    value[1] = average(yearly_pbs[k])
                if value[1] != 0.0 or not isnan(value[1]):
                    break
        except Exception as e:
            print(f"P/B. Error in data {e}")
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
    def get_roe(self, infos, financials, timeseries):
        value = [0.0,0.0]
        try:
            data = infos["returnOnEquity"]
            if data:
                value[0] = float(data)
            net_income = self.fetch_last_data(financials["Net Income"], 8, as_dict=True)
            tot_income = {}
            tot_equity = {}
            for k in net_income.keys():
                if k in financials["Stockholders Equity"]:
                    if "-" in k:
                        year = k.split("-")[0]
                        if year not in tot_income:
                            tot_income[year] = []
                        if year not in tot_equity:
                            tot_equity[year] = []
                        tot_income[year].append(net_income[k])
                        tot_equity[year].append(financials["Stockholders Equity"][k])
            for k in sorted(tot_equity.keys(), reverse=True):
                tot_e = math.fsum(tot_equity[k])
                if tot_e != 0.0 or not isnan(tot_e):
                    value[1] = math.fsum(tot_income[k])/tot_e
                if value[1] != 0.0 or not isnan(value[1]):
                    break
        except Exception as e:
            print(f"ROE. Error in data {e}")
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
    def get_div_payout(self, infos, financials, timeseries):
        value = [0.0,0.0]
        try:
            dps = infos["dividendRate"]
            if not dps:
                dps = 0.0
            eps = infos["trailingEps"]
            if not eps:
                eps = 0.0
            if eps != 0.0:
                value[0] = dps/eps
                if isnan(value[0]):
                    value[0] = 0.0
            c_div = self.fetch_last_data(financials["Cash Dividends Paid"], 8, as_dict=True)
            tot_income = {}
            tot_div = {}
            for k in c_div.keys():
                if k in financials["Net Income"]:
                    if "-" in k:
                        year = k.split("-")[0]
                        if not year in tot_income:
                            tot_income[year] = []
                        tot_income[year].append(financials["Net Income"][k])
                        if not year in tot_div:
                            tot_div[year] = []
                        tot_div[year].append(-1*c_div[k])
            for k in sorted(tot_div.keys(), reverse=True):
                tot_d = math.fsum(tot_div[k])
                tot_i = math.fsum(tot_income[k])
                if tot_i != 0.0 or not isnan(tot_i) or not isnan(tot_d):
                    value[1] = tot_d/tot_i
                    if value[1] != 0.0 or not isnan(value[1]):
                        break
        except Exception as e:
            print(f"DIV PAY. Error in data {e}")
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
    def get_cur_ratio(self, infos, financials, timeseries):
        value = [0.0,0.0]
        try:
            data = infos["currentRatio"]
            if data:
                value[0] = float(data)
            net_income = self.fetch_last_data(financials["Total Assets"], 8, as_dict=True)
            tot_asset = {}
            tot_liab = {}
            for k in net_income.keys():
                if k in financials["Total Liabilities Net Minority Interest"]:
                    if "-" in k:
                        year = k.split("-")[0]
                        if year not in tot_asset:
                            tot_asset[year] = []
                        if year not in tot_liab:
                            tot_liab[year] = []
                        tot_asset[year].append(net_income[k])
                        tot_liab[year].append(financials["Total Liabilities Net Minority Interest"][k])
            for k in sorted(tot_liab.keys(), reverse=True):
                tot_e = math.fsum(tot_liab[k])
                if tot_e != 0.0 or not isnan(tot_e):
                    value[1] = math.fsum(tot_asset[k])/tot_e
                if value[1] != 0.0 or not isnan(value[1]):
                    break
        except Exception as e:
            print(f"CUR RAT. Error in data {e}")
        return value

    '''
    Dividend :
    1. This value is taken from currentRatio in Info
    2. Calculate by
        from financials get  yearly "Total Assets" and yearly "Total Liabilities Net Minority Interest" 
        cur_ratio = Total Assets/Total Liabilities Net Minority Interest
    '''
    def get_div(self, infos, financials, timeseries):
        value = [0.0,0.0]
        try:
            dps = infos["dividendRate"]
            if not dps or isnan(dps):
                value[0] = 0.0
            else:
                value[0] = float(dps)
            c_div = self.fetch_last_data(financials["Cash Dividends Paid"], 8, as_dict=True)
            tot_share = {}
            tot_div = {}
            for k in c_div.keys():
                if k in financials["Basic Average Shares"]:
                    if "-" in k:
                        year = k.split("-")[0]
                        if not year in tot_share:
                            tot_share[year] = []
                        tot_share[year].append(financials["Basic Average Shares"][k])
                        if not year in tot_div:
                            tot_div[year] = []
                        if c_div[k] != 0.0 or not isnan(c_div[k]):
                            tot_div[year].append(-1*c_div[k])
            for k in sorted(tot_div.keys(), reverse=True):
                if tot_div[k] and tot_share[k]:
                    tot_d = math.fsum(tot_div[k])
                    tot_s = average(tot_share[k])
                    if isnan(tot_s) or isnan(tot_d):
                        continue
                    if tot_s != 0.0:
                        value[1] = tot_d/tot_s
                if value[1] != 0.0 or not isnan(value[1]):
                    break
        except Exception as e:
            print(f"DIVIDEND. Error in data {e}")
        return value

    '''
    EBITDA :
    1. This value is taken from ebitda in Info
    2. Calculate by
        from financials get average yearly "EBITDA" 
    '''
    def get_ebitda(self, infos, financials, timeseries):
        value = [0.0,0.0]
        try:
            ebit = infos["ebitda"]
            if ebit:
                value[0] = float(ebit)
            ebits = self.fetch_last_data(financials["EBITDA"], 8, as_dict=True)
            tot_ebit = {}
            for k in ebits.keys():
                if "-" in k:
                    year = k.split("-")[0]
                    if not year in tot_ebit:
                        tot_ebit[year] = []
                    if ebits[k] == 0.0 or isnan(ebits[k]):
                        if k in financials["EBIT"]:
                            ebits[k] = float(financials["EBIT"][k])
                    if ebits[k] != 0.0:
                        tot_ebit[year].append(ebits[k])
            # print(tot_ebit)
            for k in sorted(tot_ebit.keys(), reverse=True):
                if tot_ebit[k]:
                    av_ebit = average(tot_ebit[k])
                    if isnan(av_ebit):
                        continue
                    if av_ebit != 0.0:
                        value[1] = av_ebit
                if value[1] != 0.0 or not isnan(value[1]):
                    break
        except Exception as e:
            print(f"EBITDA. Error in data {e}")
        return value

    def get_market_cap(self, infos, financials, timeseries):
        value = [0.0,0.0]
        try:
            mcap = infos["marketCap"]
            if mcap:
                value[0] = float(mcap)

            shares = self.fetch_last_data(financials["Basic Average Shares"], 8, as_dict=True)
            mar_cap = {}
            for k in shares.keys():
                cur_price = self.get_timeseries(timeseries, k, "High")
                if cur_price == 0.0 or isnan(cur_price):
                    cur_price = infos["currentPrice"]
                if "-" in k:
                    year = k.split("-")[0]
                    if not year in mar_cap:
                        mar_cap[year] = []
                    mar_cap[year].append(cur_price*shares[k])
            for k in sorted(mar_cap.keys(), reverse=True):
                if mar_cap[k]:
                    mc = average(mar_cap[k])
                    if mc != 0.0:
                        value[1] = float(mc)
                if value[1] != 0.0 and not isnan(value[1]):
                    break
        except Exception as e:
            print(f"MAR CAP. Error in data {e}")
        return value

    def get_cash(self, infos, financials, timeseries):
        value = [0.0,0.0]
        try:
            tot_cash = infos["totalCash"]
            if tot_cash:
                value[0] = float(tot_cash)

            cncs = self.fetch_last_data(financials["Cash And Cash Equivalents"], 8, as_dict=True)
            tot_cnc = {}
            for k in cncs.keys():
                if "-" in k:
                    year = k.split("-")[0]
                    if not year in tot_cnc:
                        tot_cnc[year] = []
                    tot_cnc[year].append(cncs[k])
            # print(tot_cnc)
            for k in sorted(tot_cnc.keys(), reverse=True):
                tc = math.fsum(tot_cnc[k])
                if tc != 0.0:
                    value[1] = float(tc)
                    if value[1] != 0.0 and not isnan(value[1]):
                        break
        except Exception as e:
            print(f"TOT CASH. Error in data {e}")
        return value

    @staticmethod
    def get_365d_lh(infos, financials, timeseries):
        value = [0.0,0.0]
        try:
            counter = 1
            for date in sorted(timeseries.keys(), reverse=True):
                val = timeseries[date]
                # print(val, value)
                if "High" in val:
                    if not isnan(val["High"]):
                        if value[1] == 0.0:
                            value[1] = val["High"]
                        if value[1] < val["High"]:
                            value[1] = val["High"]
                if "Low" in val:
                    if not isnan(val["Low"]):
                        if value[0] == 0.0:
                            value[0] = val["Low"]
                        if value[0] > val["Low"]:
                            value[0] = val["Low"]
                counter += 1
                if counter > 365:
                    break
        except Exception as e:
            print(f"TOT CASH. Error in data {e}")
        return value


    def update_rating_items(self, infos:dict, financials:dict, timeseries:dict):
        calc_values = {}
        cal_functions = {
            "Revenue Growth" : self.get_rev_growth,
            "Profit Growth" : self.get_profit_growth,
            "Earnings Per Share (EPS)" : self.get_eps,
            "Assets vs. Liabilities" : self.get_ass_v_lia,
            "Debt-to-Equity Ratio (D/E)" : self.get_de_ratio,
            "Free Cash Flow (FCF)" : self.get_fcf_ratio,
            "Price-to-Earnings (P/E) Ratio" : self.get_pe_ratio,
            "Price-to-Sales (P/S) Ratio" : self.get_ps_ratio,
            "Price-to-Book (P/B) Ratio" : self.get_pb_ratio,
            "Return on Equity (ROE)" : self.get_roe,
            "Dividend Payout Ratio" : self.get_div_payout,
            "Current Ratio" : self.get_cur_ratio,
            "Market Cap" : self.get_market_cap,
            "Paid Dividend" : self.get_div,
            "Total Cash" : self.get_cash,
            "365daysLow,High" : self.get_365d_lh,
            "EBITxx" : self.get_ebitda,
        }
        for key in cal_functions.keys():
            calc_values[key] = cal_functions[key](infos, financials, timeseries)
        return calc_values

    def collect_company_information(self, ticker: str):
        # print(f"Getting Company Ticker for {ticker}")
        companies_data = {}
        status, data = self.get_company_info(ticker)
        if status:
            companies_data[ticker] = {}
            companies_data[ticker]["info"] = data
            status, data = self.get_company_timeseries(ticker)
            if status:
                companies_data[ticker]["timeseries"] = data
            status, data = self.get_company_financials(ticker)
            if status:
                companies_data[ticker]["financials"] = data
                companies_data[ticker]["ratings"] = self.update_rating_items(companies_data[ticker]["info"],
                                                                             companies_data[ticker]["financials"],
                                                                             companies_data[ticker]["timeseries"])
                # companies_data[ticker].pop("financials")
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
                self.ticker_obj = None
                counter += 1
                with open(self.company_info_file, 'w', encoding='utf-8') as f:
                    f.write(json.dumps(self.company_info, indent=4))
                    f.close()
            # if counter > 2:
            #     break
        print(f'Ticker with no information {missing_tickers}')



# Example usage
def main():
    # Create collector instance
    collector = CompanyDataCollector("company_info.json", "ticker.json")
    if collector.all_ticker_symbols:
        collector.start_processing()
    else:
        print("No ticker symbols available")


if __name__ == "__main__":
    main()