import datetime
import pprint
import pandas as pd
import yfinance
from math import isnan


class CollectYFinanceData:
    def __init__(self):
        self.info_keys = [
            "symbol",
            "googleticker",
            "longName",
            "shortName",
            "country",
            "industry",
            "sector",
            "language",
            "currency",
            "exchangeTimezoneShortName",
            "fullExchangeName",
            "grossProfits",
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
            "BasicAverageShares",
            "EBIT",
            "EBITDA",
            "GrossProfit",
            "NetIncome",
            "DilutedEPS",
            "CashAndCashEquivalents",
            "TotalRevenue",
            "StockholdersEquity",
            "TotalAssets",
            "TotalDebt",
            "TotalEquityGrossMinorityInterest",
            "TotalLiabilitiesNetMinorityInterest",
            "CapitalExpenditure",
            "CashDividendsPaid",
            "FreeCashFlow",
            "OperatingCashFlow"
        ]
        self.ticker_obj = None


# Functions specific to yahoo finance
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
                    to_fill[k] = 0.0
                for key in new_data.keys():
                    if not new_data[key]:
                        new_data[key] = to_fill
        return new_data

    @staticmethod
    def dataframe_to_dict(data):
        fin_data = {}
        for tkey in data.keys():
            for vkey in data[tkey].keys():
                date_str = vkey.strftime('%Y-%m-%d')
                if not tkey in fin_data:
                    fin_data[tkey] = {}
                if isnan(data[tkey][vkey]):
                    data[tkey][vkey] = 0.0
                fin_data[tkey][date_str] = round(data[tkey][vkey],2)
        for vkey in fin_data.keys():
            data = fin_data[vkey]
            fin_data[vkey] = dict(sorted(data.items(), reverse=True))
        return fin_data

    def get_company_info(self, ticker: str):
        company_info = {}
        status = True
        try:
            self.ticker_obj = yfinance.Ticker(ticker)
            company_info = self.ticker_obj.info
            if not company_info:
                raise Exception("Can't retrieve Company Information")
            else:
                company_info = self.update_data_keys(company_info, 1)
        except Exception as e:
            status = False
            print(f"- Info Error for {ticker}: {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
        return [status, company_info]

    def get_company_timeseries(self, ticker: str, str_date: str, end_date: str) :
        company_ts = {}
        status = True
        try:
            self.ticker_obj = yfinance.Ticker(ticker)
            data = self.ticker_obj.history(start=str_date, end=end_date,
                                           interval='1d', actions=False, auto_adjust=False)
            if data.empty:
                raise Exception("Can't retrieve Company Timeseries")
            else:
                for ent_date, ent_row in data.iterrows():
                    date_data = {
                        "Open" : round(ent_row["Open"],2),
                        "High" : round(ent_row["High"],2),
                        "Low" : round(ent_row["Low"],2),
                        "Close" : round(ent_row["Close"],2),
                        "Volume" : round(ent_row["Volume"],2)
                    }
                    company_ts[pd.to_datetime(ent_date).strftime('%Y-%m-%d')] = date_data
        except Exception as e:
            status = False
            print(f"- TS Error for {ticker}: {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
        if not company_ts:
            status = False
        # company_ts = super.update_ts_indicators(company_ts)
        return [status, company_ts]

    def get_company_financials(self, ticker: str) :
        status = True
        company_fin = {}
        try:
            company_fin = self.fetch_financials(ticker, ["yearly", "quarterly"])
            if not company_fin:
                raise Exception("Can't retrieve Company Financials")
            company_fin = self.update_data_keys(company_fin, 2)
            if not company_fin:
                raise Exception("Missing needed keys")
        except Exception as e:
            status = False
            print(f"- Financial Error for {ticker}: {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
        return [status, company_fin]

    def fetch_latest_financials(self, ticker: str):
        self.ticker_obj = yfinance.Ticker(ticker)
        datas = []
        freqs = ["quarterly", "yearly"]
        for freq in freqs:
            datas.append(self.ticker_obj.get_financials(freq=freq))
            datas.append(self.ticker_obj.get_balance_sheet(freq=freq))
            datas.append(self.ticker_obj.get_cash_flow(freq=freq))
        data = pd.concat(datas, axis=1).fillna(0)
        data = data.iloc[:, 0:1]
        data = data.T.groupby(by=data.columns).agg(self.non_zero_agg).T
        date = pd.Timestamp(data.columns[0]).strftime('%Y-%m-%d')
        date = datetime.datetime.strptime(date, '%Y-%m-%d')
        data = data.to_dict(orient='index')
        data = self.dataframe_to_dict(data)
        return [date, data]

    @staticmethod
    def non_zero_agg(series):
        non_zero_mask = (series != 0) & (series.notna())
        if non_zero_mask.any():
            return series[non_zero_mask].iloc[0]
        else:
            return 0

    def fetch_financials(self, ticker: str, freqs: list) -> dict :
        data = {}
        try:
            self.ticker_obj = yfinance.Ticker(ticker)
            data_t = []
            for freq in freqs:
                data_t.append(self.ticker_obj.get_financials(freq=freq))
                data_t.append(self.ticker_obj.get_balance_sheet(freq=freq))
                data_t.append(self.ticker_obj.get_cash_flow(freq=freq))
            if data_t:
                data = pd.concat(data_t, axis=1).fillna(0)
                data = data.T.groupby(by=data.columns).agg(self.non_zero_agg).T
                data = data.to_dict(orient='index')
                data = self.dataframe_to_dict(data)
        except Exception as e:
            print(f"- Financial Error for {ticker}: {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
        # pprint.pprint(data)
        return data

