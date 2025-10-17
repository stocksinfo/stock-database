import datetime
import sqlite3
from datetime import timedelta

from config.source.basicStockData import BasicStockData
from database.convert_database import DatabaseManager


class UpdateStockData(BasicStockData):
    def __init__(self, db_file="company_info.db"):
        super().__init__()
        self.db_file = db_file
        self.db_manager = DatabaseManager(self.db_file)
        self.current_ts = None


    def get_connection(self):
        """Get database connection with foreign key support"""
        conn = sqlite3.connect(self.db_file)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.row_factory = sqlite3.Row  # Enable dict-like access
        return conn

    @staticmethod
    def fetch_and_convert(query, cursor, as_list=False):
        cursor.execute(query)
        output = []
        for row in cursor.fetchall():
            if as_list:
                output += dict(row).values()
            else:
                output.append(dict(row))
        return output

    def refresh_info_table(self, table_name, ticker:str, cursor, conn):
        try:
            sql_query = f"SELECT Date FROM {table_name} WHERE symbol='{ticker}';"
            info_date = self.fetch_and_convert(sql_query, cursor, as_list=True)
            # print(sql_query, info_date)
            if info_date:
                info_date = datetime.datetime.strptime(sorted(info_date, reverse=True)[0], '%Y-%m-%d')
                end_date = datetime.datetime.today()
                if info_date < end_date:
                    sql_query = f"PRAGMA table_info({table_name});"
                    columns = self.fetch_and_convert(sql_query, cursor)
                    sta, data = self.get_company_info(ticker)
                    if sta:
                        sql_query = f'UPDATE {table_name} SET Date = ?, '
                        values = [end_date.strftime("%Y-%m-%d"),]
                        for col in columns:
                            key = col['name']
                            if key == 'symbol':
                                continue
                            if key in data:
                                if data[key] != '':
                                    sql_query += f'{key} = ?, '
                                    values.append(data[key])
                        sql_query = sql_query[:-2]
                        sql_query += f' WHERE symbol=?;'
                        values.append(ticker)
                        print(f'+ {ticker}/{str(table_name).upper()}: Updating whole table for Date {end_date.strftime("%Y-%m-%d")}')
                        cursor.execute(sql_query, values)
                        conn.commit()
        except Exception as e:
            print(f"Error adding entry to database {table_name} : {ticker} -> {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
            conn.rollback()

    def refresh_fin_table(self, table_name, ticker:str, cursor, conn):
        try:
            pass
            sql_query = f"SELECT Date FROM {table_name} WHERE symbol='{ticker}' ORDER BY Date;"
            fin_date = self.fetch_and_convert(sql_query, cursor, as_list=True)
            if fin_date:
                fin_date = datetime.datetime.strptime(sorted(fin_date, reverse=True)[0], '%Y-%m-%d')
                end_date, financials = self.fetch_latest_financials(ticker)
                if fin_date.date() < end_date.date():
                    financials = self.update_data_keys(financials, 2)
                    self.db_manager.add_entry_to_database(ticker, table_name, financials)
                    print(f'+ {ticker}/{str(table_name).upper()}: Adding entries from {fin_date.strftime("%Y-%m-%d")}')
        except Exception as e:
            print(f"Error adding entry to database {table_name} : {ticker} -> {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
            conn.rollback()
        return

    @staticmethod
    def sql_to_dict(data:list) -> dict:
        output = {}
        # print(data)
        for entry in data:
            if 'googleticker' in entry.keys(): #info table
                output = entry
            elif 'YZVolatilityEstimator' in entry.keys(): #timeseries
                # print(entry)
                cur_date = None
                for key in entry.keys():
                    if key == 'Date':
                        cur_date = entry[key]
                    elif key != 'symbol':
                        if cur_date not in output:
                            output[cur_date] = {}
                        output[cur_date][key] = entry[key]
            else: # finance table
                cur_date = None
                for key in entry.keys():
                    if key == 'Date':
                        cur_date = entry[key]
                    elif key != 'symbol':
                        entry_name = key
                        if entry_name not in output:
                            output[entry_name] = {}
                        if not entry[key]:
                            entry[key] = 0.0
                        output[entry_name][cur_date] = entry[key]
        return output

    @staticmethod
    def dict_to_sql(ticker: str, data:dict) -> list:
        output = []
        if 'googleticker' in data.keys():
            data['Date'] = datetime.datetime.today().strftime("%Y-%m-%d")
            output.append(data)
        elif 'BasicAverageShares' in data.keys() or 'RevenueGrowth' in data.keys():
            all_data = {}
            for key in data.keys():
                for date_key in data[key].keys():
                    if date_key not in all_data:
                        all_data[date_key] = {
                            'symbol': ticker,
                            'Date': date_key,
                        }
                    all_data[date_key][key] = data[key][date_key]
            output = list(all_data.values())
        else: # timeseries
            for date_key in data.keys():
                line = {
                    'symbol': ticker,
                    'Date': date_key,
                }
                for key in data[date_key].keys():
                    line[key] = data[date_key][key]
                output.append(line)
        return output

    def refresh_rat_table(self, table_name, ticker:str, cursor, conn):
        try:
            sql_query = f"SELECT * FROM {self.db_manager.info_table_name} WHERE symbol='{ticker}';"
            info_data = self.sql_to_dict(self.fetch_and_convert(sql_query, cursor))
            sql_query = f"SELECT * FROM {self.db_manager.fin_table_name} WHERE symbol='{ticker}' ORDER BY Date ASC;"
            fin_data = self.sql_to_dict(self.fetch_and_convert(sql_query, cursor))
            sql_query = f"SELECT * FROM {self.db_manager.time_table_name} WHERE symbol='{ticker}' ORDER BY Date ASC;"
            ts_data = self.sql_to_dict(self.fetch_and_convert(sql_query, cursor))
            sql_query = f"SELECT Date FROM {self.db_manager.rate_table_name} WHERE symbol='{ticker}' ORDER BY Date ASC;"
            rat_dates = sorted(self.fetch_and_convert(sql_query, cursor, as_list=True), reverse=True)
            rat_data = self.update_rating_items(info_data, fin_data, ts_data)
            entries = self.dict_to_sql(ticker, rat_data)
            sql_to_enter = []
            for entry in entries:
                if 'Date' in entry.keys():
                    if entry['Date'] == 'today':
                        sql_query = f"DELETE FROM {table_name} WHERE symbol = '{ticker}' AND Date = 'today';"
                        cursor.execute(sql_query)
                        conn.commit()
                        sql_to_enter.append(entry)
                    else:
                        cur_date = entry['Date']
                        if cur_date not in rat_dates:
                            sql_to_enter.append(entry)
            # print(sql_to_enter)
            if sql_to_enter:
                print(f'+ {ticker}/{str(table_name).upper()}: Updating with {len(sql_to_enter)} entries')
                self.db_manager.add_entry_to_database(ticker, table_name, self.sql_to_dict(sql_to_enter))
        except Exception as e:
            print(f"Error adding entry to database {table_name} : {ticker} -> {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
            conn.rollback()
        return

    def refresh_tim_table(self, table_name, ticker:str, cursor, conn):
        try:
            sql_query = f"SELECT Date FROM {table_name} WHERE symbol='{ticker}';"
            ts_date = self.fetch_and_convert(sql_query, cursor, as_list=True)
            if ts_date:
                ts_date = (datetime.datetime.strptime(sorted(ts_date, reverse=True)[0], '%Y-%m-%d')
                           + timedelta(days=1))
                end_date = datetime.datetime.today() - timedelta(days=1)
                if ts_date < end_date:
                    start_date = ts_date - timedelta(days=20)
                    status, data = self.get_company_timeseries(ticker, start_date.strftime('%Y-%m-%d'),
                                                               end_date.strftime('%Y-%m-%d'))
                    if status:
                        data = self.update_ts_indicators(data)
                        # print(data)
                        timeseries = {}
                        items_to_remove = 0
                        for date_key in data.keys():
                            date = datetime.datetime.strptime(date_key, '%Y-%m-%d')
                            if date.date() > ts_date.date():
                                timeseries[date_key] = data[date_key]
                                print(f'+ {ticker}/{str(table_name).upper()}: Adding entry into {ticker} -> {date_key}')
                                items_to_remove += 1
                        self.db_manager.add_entry_to_database(ticker, table_name, timeseries)
                        if items_to_remove > 0:
                            sql_query = f"SELECT Date FROM {table_name} WHERE symbol='{ticker}';"
                            all_dates = self.fetch_and_convert(sql_query, cursor, as_list=True)
                            all_dates = sorted(all_dates)[:items_to_remove]
                            or_logic = ""
                            for date in all_dates:
                                or_logic += f"Date = '{date}' OR "
                            or_logic = or_logic[:-4]
                            sql_query = f"DELETE FROM {table_name} WHERE symbol = '{ticker}' AND ({or_logic});"
                            cursor.execute(sql_query)
                            conn.commit()

        except Exception as e:
            print(f"Error adding entry to database {table_name} : {ticker} -> {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
            conn.rollback()
        return

    def check_and_update(self, ticker:str, table_name:str, cursor, conn):
        try:
            # print(f'Updating {table_name} from {ticker}')
            if table_name == self.db_manager.info_table_name:
                self.refresh_info_table(table_name, ticker, cursor, conn)
            if table_name == self.db_manager.fin_table_name:
                self.refresh_fin_table(table_name, ticker, cursor, conn)
            if table_name == self.db_manager.time_table_name:
                self.refresh_tim_table(table_name, ticker, cursor, conn)
            if table_name == self.db_manager.rate_table_name:
                self.refresh_rat_table(table_name, ticker, cursor, conn)
        except Exception as e:
            print(f"Error : {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")

