import json
import re
import sqlite3

import yfinance


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
        """Add entry to database"""
        try:
            conn = self.get_connection()
            if table_name != self.time_table_name:
                cols = []
                vals = []
                if table_name != self.info_table_name:
                    vals = [ticker,]
                    cols = ["symbol",]
                for k in table_data.keys():
                    col = re.sub(r'[^A-Za-z0-9]', '', k)
                    col = re.sub(r'^[0-9]+', '', col)
                    cols.append(col)
                    json_string = json.dumps(table_data[k])
                    vals.append(json_string)
                if cols and vals:
                    columns = ', '.join(cols)
                    placeholders = ', '.join('?' * len(cols))
                    values = tuple(vals)
                    cursor = conn.cursor()
                    sql_query = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})'
                    cursor.execute(sql_query, values)
                    conn.commit()
            else:
                cols = ["symbol", "Date", "Value"]
                for k in table_data.keys():
                    vals = [ticker, k]
                    json_string = json.dumps(table_data[k])
                    vals.append(json_string)
                    columns = ', '.join(cols)
                    placeholders = ', '.join('?' * len(cols))
                    values = tuple(vals)
                    cursor = conn.cursor()
                    sql_query = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})'
                    cursor.execute(sql_query, values)
                    conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error adding entry to database {table_name} : {ticker} -> {e}")
        return

    def _create_company_info_table(self, conn):
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.info_table_name} (
                symbol VARCHAR(15) PRIMARY KEY,
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
                volume BIGINT,
                date VARCHAR(20) DEFAULT CURRENT_DATE
            );
        ''')
        return

    def _create_financial_table(self, conn):
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.fin_table_name} (
                symbol VARCHAR(15) PRIMARY KEY,
                BasicAverageShares TEXT,
                EBIT TEXT,
                EBITDA TEXT,
                GrossProfit TEXT,
                NetIncome TEXT,
                DilutedEPS TEXT,
                CashAndCashEquivalents TEXT,
                TotalRevenue TEXT,
                StockholdersEquity TEXT,
                TotalAssets TEXT,
                TotalDebt TEXT,
                TotalEquityGrossMinorityInterest TEXT,
                TotalLiabilitiesNetMinorityInterest TEXT,
                CapitalExpenditure TEXT,
                CashDividendsPaid TEXT,
                FreeCashFlow TEXT,
                OperatingCashFlow TEXT
            );
        ''')
        pass

    def _create_ratings_table(self, conn):
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.rate_table_name} (
                symbol VARCHAR(15) PRIMARY KEY,
                RevenueGrowth TEXT,
                ProfitGrowth TEXT,
                EarningsPerShareEPS TEXT,
                AssetsVsLiabilities TEXT,
                DebttoEquityRatioDE TEXT,
                FreeCashFlowFCF TEXT,
                PricetoEarningsPERatio TEXT,
                PricetoSalesPSRatio TEXT,
                PricetoBookPBRatio TEXT,
                ReturnonEquityROE TEXT,
                DividendPayoutRatio TEXT,
                CurrentRatio TEXT,
                MarketCap TEXT,
                PaidDividend TEXT,
                TotalCash TEXT,
                daysLowHigh TEXT,
                EBITxx TEXT
            );
        ''')
        pass

    def _create_stock_timeseries(self, conn):
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.time_table_name} (
                symbol VARCHAR(15) ,
                Date TEXT,
                Value TEXT,
                PRIMARY KEY (Symbol, Date)
            );
        ''')
        pass


def main():
    print("Initializing SQLite Stock Database...")
    db = DatabaseManager("company_info.db")

    company_info = {}
    # Reading the json file
    with open("../config/company_info.json", "r") as f:
        company_info = json.load(f)
        db.reset_database()
        db.init_database()

    for ticker in company_info.keys():
        print(f'Adding {ticker} to the database...')
        for entry in company_info[ticker].keys():
            db.add_entry_to_database(ticker, entry, company_info[ticker][entry])


main()





