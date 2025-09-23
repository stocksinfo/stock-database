import os
import pprint

import requests

from collect_ticker_data import CompanyDataCollector
import json

class CompanyDataCollectorFiscal(CompanyDataCollector):
    def __init__(self, comaney_info_file, ticker_info_file):
        super().__init__(comaney_info_file, ticker_info_file)
        self.info_keys_mapping = {
            "symbol": " ",
            "google_ticker": " ",
            "longName": " ",
            "shortName": " ",
            "country": " ",
            "industry": " ",
            "sector": " ",
            "language": " ",
            "currency": " ",
            "exchangeTimezoneShortName": " ",
            "fullExchangeName": " ",
            "market": " ",
            "currentPrice": " ",
            "sharesOutstanding": " ",
            "marketCap": " ",
            "debtToEquity": " ",
            "revenueGrowth": " ",
            "trailingEps": " ",
            "freeCashflow": " ",
            "priceToBook": " ",
            "ebitda": " ",
            "priceToSalesTrailing12Months": " ",
            "returnOnEquity": " ",
            "dividendRate": " ",
            "currentRatio": " ",
            "totalCash": " ",
            "volume": " ",
        }
        self.financials_keys_mapping = {
            "Basic Average Shares": " ",
            "EBIT": " ",
            "EBITDA": " ",
            "Gross Profit": " ",
            "Net Income": " ",
            "Diluted EPS": " ",
            "Cash And Cash Equivalents": " ",
            "Total Revenue": " ",
            "Stockholders Equity": " ",
            "Total Assets": " ",
            "Total Debt": " ",
            "Total Equity Gross Minority Interest": " ",
            "Total Liabilities Net Minority Interest": " ",
            "Capital Expenditure": " ",
            "Cash Dividends Paid": " ",
            "Free Cash Flow": " ",
            "Operating Cash Flow": " "
        }


    def load_all_ticker_symbols(self):
        self.all_ticker_symbols = {}
        with open(self.ticker_info_file, 'r', encoding='utf-8') as f:
            self.all_ticker_symbols = json.load(f)
            f.close()
        if not self.all_ticker_symbols:
            print(f'No tickers found to process in file {f.name}')
            return
        # company_tickers = {}
        # for k in self.all_ticker_symbols:
        #     company_tickers[f'{self.all_ticker_symbols[k]}_{k}'] = self.all_ticker_symbols[k]
        # self.all_ticker_symbols = company_tickers

    def get_company_info(self, ticker: str):
        company_info = {}
        data = self.fetch_data_fiscal_api("/v1/company/profile", {"ticker": ticker})
        pprint.pprint(data)
        status = True

        return [status, company_info]

    def get_company_timeseries(self, ticker: str):
        company_ts = {}
        status = True

        return [status, company_ts]

    def get_company_financials(self, ticker: str):
        company_ts = {}
        status = True

        return [status, company_ts]

    @staticmethod
    def fetch_data_fiscal_api(req_ext, headers):
        req_info = {}
        api_key = os.environ.get("AK")
        if api_key:
            base_url = "https://api.fiscal.ai"
            headers["apiKey"] = api_key
            req_params = "?"
            for k in headers:
                req_params = f'{req_params}{k}={headers[k]}&'
            req_params = req_params[:-1]
            final_url = f'{base_url}{req_ext}{req_params}'
            response = requests.get(final_url)
            if response.status_code == 200:
                req_info = response.json()
            else:
                print(f'Request {final_url} failed with status code: {response.status_code} {response.text}')
        return req_info
    @staticmethod
    def dataframe_to_dict(data):
        return data
