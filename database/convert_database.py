import json
import pprint
import re
import sqlite3
from datetime import datetime

import math
import yfinance
import numpy as np
from pandas.core.dtypes.inference import is_float


class DatabaseManager:
    def __init__(self, db_file="company_info.db"):
        self.db_file = db_file
        self.info_table_name = "info"
        self.fin_table_name = "financials"
        self.rate_table_name = "ratings"
        self.time_table_name = "timeseries"

    def get_connection(self):
        """Get database connection with foreign key support"""
        conn = sqlite3.connect(self.db_file)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.row_factory = sqlite3.Row  # Enable dict-like access
        return conn

    def init_database(self):
        """Initialize database with all required tables"""
        conn = self.get_connection()
        try:
            self._create_stock_timeseries(conn)
            self._create_financial_table(conn)
            self._create_ratings_table(conn)
            self._create_company_info_table(conn)
            # print(f"Database Initialized Successfully")
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"Error initializing database: {e}")

    def reset_database(self):
        conn = self.get_connection()
        try:
            """Reset database with all required tables"""
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            # Iterate through the tables and delete all rows
            for table_name in tables:
                if table_name[0] != 'sqlite_sequence': # Avoid deleting from internal SQLite table
                    cursor.execute(f"DROP TABLE IF EXISTS {table_name[0]};")
            conn.execute("VACUUM;")
            conn.commit()
            conn.close()
        except Exception as e:
            conn.rollback()
            print(f"Error resetting database: {e}")

    def add_entry_to_database(self, ticker:str, table_name:str, table_data:dict):
        """Add tables to database"""
        try:
            sql_queries = []
            values = []
            if table_name == self.info_table_name:
                vals = [ticker, datetime.today().strftime("%Y-%m-%d")]
                cols = ["symbol","Date"]
                for col in table_data.keys():
                    cols.append(col)
                    vals.append(table_data[col])
                if cols and vals:
                    columns = ', '.join(cols)
                    placeholders = ', '.join('?' * len(cols))
                    values.append(tuple(vals))
                    sql_queries.append(f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})')
            elif table_name == self.time_table_name:
                for k in table_data.keys():
                    cols = ["symbol", "Date"]
                    json_string = json.dumps(table_data[k])
                    daily_val = dict(json.loads(json_string))
                    vals = [ticker, k]
                    for dk in daily_val.keys():
                        cols.append(re.sub(r'[^A-Za-z0-9]', '', dk))
                        if is_float(daily_val[dk]):
                            vals.append(float(daily_val[dk]))
                        else:
                            vals.append(str(daily_val[dk]))
                    columns = ', '.join(cols)
                    placeholders = ', '.join('?' * len(cols))
                    values.append(tuple(vals))
                    sql_queries.append(f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})')
            else:
                cols = ["symbol", "Date"]
                for col in table_data.keys():
                    cols.append(col)
                dates = dict()
                for k in table_data.keys():
                    for dt in table_data[k].keys():
                        if dt not in dates:
                            dates[dt] = 1
                for dt in sorted(dates.keys(), reverse=True):
                    dval = f'{ticker};{dt}'
                    for k in table_data.keys():
                        val = ""
                        if dt in table_data[k].keys():
                            val = table_data[k][dt]
                            if val == 'Nan':
                                val = 0.0
                        dval = f'{dval};{val}'
                    values.append(dval)
                new_values = []
                for index in range(len(values)):
                    values[index] = tuple(values[index].split(';'))
                    total = 0.0
                    for val in values[index][2:]:
                        if val == '':
                            val = 0.0
                        total += float(val)
                    if cols and total != 0.0:
                        columns = ', '.join(cols)
                        placeholders = ', '.join('?' * len(cols))
                        sql_queries.append(f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})')
                        new_values.append(tuple(values[index]))
                values = new_values
            if values:
                conn = self.get_connection()
                cursor = conn.cursor()
                for index in range(len(values)):
                    cursor.execute(sql_queries[index], tuple(values[index]))
                    conn.commit()
                conn.close()
        except Exception as e:
            print(f"Error adding entry to database {table_name} : {ticker} -> {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
        return

    def _create_company_info_table(self, conn):
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.info_table_name} (
                symbol VARCHAR(15),
                Date VARCHAR(20),
                googleticker TEXT,
                longName TEXT,
                shortName TEXT,
                country TEXT,
                industry TEXT,
                sector TEXT,
                language TEXT,
                currency TEXT, 
                exchangeTimezoneShortName TEXT,
                fullExchangeName TEXT,
                market TEXT,
                grossProfits DECIMAL(10,2),
                currentPrice DECIMAL(10,2),
                sharesOutstanding BIGINT,
                marketCap BIGINT,
                debtToEquity DECIMAL(10,2),
                revenueGrowth DECIMAL(10,2),
                trailingEps DECIMAL(10,2),
                freeCashflow DECIMAL(10,2),
                priceToBook DECIMAL(10,2),
                ebitda DECIMAL(10,2),
                priceToSalesTrailing12Months DECIMAL(10,2),
                returnOnEquity DECIMAL(10,2),
                dividendRate DECIMAL(10,2),
                currentRatio DECIMAL(10,2),
                totalCash BIGINT,
                volume BIGINT
            );
        ''')
        return

    def _create_financial_table(self, conn):
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.fin_table_name} (
                symbol VARCHAR(15),
                Date VARCHAR(20),
                BasicAverageShares DECIMAL(10,4),
                EBIT DECIMAL(10,4),
                EBITDA DECIMAL(10,4),
                GrossProfit DECIMAL(10,4),
                NetIncome DECIMAL(10,4),
                DilutedEPS DECIMAL(10,4),
                CashAndCashEquivalents DECIMAL(10,4),
                TotalRevenue DECIMAL(10,4),
                StockholdersEquity DECIMAL(10,4),
                TotalAssets DECIMAL(10,4),
                TotalDebt DECIMAL(10,4),
                TotalEquityGrossMinorityInterest DECIMAL(10,4),
                TotalLiabilitiesNetMinorityInterest DECIMAL(10,4),
                CapitalExpenditure DECIMAL(10,4),
                CashDividendsPaid DECIMAL(10,4),
                FreeCashFlow DECIMAL(10,4),
                OperatingCashFlow DECIMAL(10,4),
                PRIMARY KEY (symbol, Date)
           );
        ''')
        pass

    def _create_ratings_table(self, conn):
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.rate_table_name} (
                symbol VARCHAR(15),
                Date VARCHAR(20),
                RevenueGrowth DECIMAL(10,4),
                ProfitGrowth DECIMAL(10,4),
                EarningsPerShareEPS DECIMAL(10,4),
                AssetsVsLiabilities DECIMAL(10,4),
                DebttoEquityRatioDE DECIMAL(10,4),
                FreeCashFlowFCF DECIMAL(10,4),
                PricetoEarningsPERatio DECIMAL(10,4),
                PricetoSalesPSRatio DECIMAL(10,4),
                PricetoBookPBRatio DECIMAL(10,4),
                ReturnonEquityROE DECIMAL(10,4),
                DividendPayoutRatio DECIMAL(10,4),
                CurrentRatio DECIMAL(10,4),
                MarketCap DECIMAL(10,4),
                PaidDividend DECIMAL(10,4),
                TotalCash DECIMAL(10,4),
                daysLowHigh DECIMAL(10,4),
                EBITxx  DECIMAL(10,4),
                PRIMARY KEY (symbol, Date)
            );
        ''')
        pass

    def _create_stock_timeseries(self, conn):
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.time_table_name} (
                symbol VARCHAR(15) ,
                Date VARCHAR(20),
                Open DECIMAL(5,2),
                High DECIMAL(5,2),
                Low DECIMAL(5,2),
                Close DECIMAL(5,2),
                Volume BIGINT,
                YZVolatilityEstimator DECIMAL(10,4),
                RelativeStrengthIndex DECIMAL(10,4),
                BollingerBands TEXT,
                AverageTrueRange TEXT,
                MovingAverageConvergenceDivergence TEXT,
                DollarVolume DECIMAL(10,4),
                StochasticOscillator TEXT,
                PRIMARY KEY (symbol, Date)
            );
        ''')
        pass

