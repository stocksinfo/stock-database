import datetime
import json
import sys
from datetime import timedelta
from turtledemo.penrose import start

from dateutil.relativedelta import relativedelta
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

    def collect_company_information(self, ticker: str):
        companies_data = {}
        bsd = BasicStockData()
        status, data = bsd.get_company_info(ticker)
        if status:
            companies_data[ticker] = {}
            if not 'googleticker' in data:
                data["googleticker"] = self.all_ticker_symbols[ticker]
            data["googleticker"] = self.all_ticker_symbols[ticker]
            companies_data[ticker]["info"] = data
            tdy = datetime.datetime.today()
            start_date = (tdy - timedelta(days=1) - relativedelta(years=2)).strftime('%Y-%m-%d')
            end_date = (tdy - timedelta(days=1)).strftime('%Y-%m-%d')

            status, data = bsd.get_company_timeseries(ticker, start_date, end_date)
            if status:
                data = bsd.update_ts_indicators(data)
                companies_data[ticker]["timeseries"] = data
            status, data = bsd.get_company_financials(ticker)
            if status:
                companies_data[ticker]["financials"] = data
                companies_data[ticker]["ratings"] = bsd.update_rating_items(companies_data[ticker]["info"],
                                                                             companies_data[ticker]["financials"],
                                                                             companies_data[ticker]["timeseries"])
        return [status, companies_data]

    def start_processing(self):
        missing_tickers = []
        counter = 1
        self.load_current_company_information()
        for ticker in self.all_ticker_symbols.keys():
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
                counter += 1
                with open(self.company_info_file, 'w', encoding='utf-8') as f:
                    f.write(json.dumps(self.company_info, indent=4))
                    f.close()
            # if counter > 2:
            #     break
        print(f'Ticker with no information {missing_tickers}')

