import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import yfinance
from math import isnan


class CollectYFinanceData:
    def __init__(self):
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
                    to_fill[k] = "Nan"
                for key in new_data.keys():
                    if not new_data[key]:
                        new_data[key] = to_fill
        return new_data

    @staticmethod
    def dataframe_to_dict(data):
        fin_data = {}
        for tkey in data.keys():
            for vkey in data[tkey].keys():
                if not vkey in fin_data:
                    fin_data[vkey] = {}
                if isnan(data[tkey][vkey]):
                    data[tkey][vkey] = 0.0
                fin_data[vkey][tkey.date().strftime("%Y-%m-%d")] = round(data[tkey][vkey],2)
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
                # if "google_ticker" not in company_info:
                #     company_info["google_ticker"] = self.all_ticker_symbols[ticker]
                company_info = self.update_data_keys(company_info, 1)
        except Exception as e:
            status = False
            print(f"- Info Error for {ticker}: {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
        return [status, company_info]

    def get_company_timeseries(self, ticker: str) :
        # print(f"+ Getting Company Timeseries for {ticker}")
        company_ts = {}
        status = True
        try:
            self.ticker_obj = yfinance.Ticker(ticker)
            tdy = datetime.datetime.today()
            str_date = (tdy - timedelta(days=1) - relativedelta(years=2)).strftime('%Y-%m-%d')
            end_date = (tdy - timedelta(days=1)).strftime('%Y-%m-%d')
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
            print(f"- Financial Error for {ticker}: {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")

        return [status, company_fin]

