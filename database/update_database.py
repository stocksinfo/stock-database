import datetime
import os
import re
import sqlite3
import time
from datetime import timedelta

from config.source.basicStockData import BasicStockData
from database.convert_database import DatabaseManager


class UpdateStockData(BasicStockData):
    def __init__(self, db_file="company_info.db"):
        super().__init__()
        self.db_file = db_file
        self.db_manager = DatabaseManager(self.db_file)
        self.fin_updated = False

    def get_connection(self):
        """Get database connection with foreign key support"""
        conn = sqlite3.connect(self.db_file)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.row_factory = sqlite3.Row  # Enable dict-like access
        return conn

    @staticmethod
    def get_timeseries_entry(timeseries: dict, date_key: str, tag: str) -> float:
        value = 0.0
        # if date_key in timeseries:
        #     value = timeseries[date_key][tag]
        # else:
        #     last_date = sorted(timeseries.keys())[0]
        #     last_date = datetime.datetime.strptime(last_date, "%Y-%m-%d")
        #     ask_date = datetime.datetime.strptime(date_key, "%Y-%m-%d")
        #     if ask_date < last_date:
        #         value = timeseries[last_date.strftime("%Y-%m-%d")][tag]
        #     else:
        #         k = "-".join(date_key.split("-")[:-1])
        #         red_ts = {}
        #         for key in timeseries.keys():
        #             if k in key:
        #                 red_ts[key] = timeseries[key]
        #         for key in sorted(red_ts.keys(), reverse=True):
        #             value = red_ts[key][tag]
        #             if not isnan(value):
        #                 break
        value = round(value, 2)
        return value

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
                exit(20)
        except Exception as e:
            print(f"Error : {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")

    def refresh_info_table(self, table_name, ticker:str, cursor, conn):
        try:
            sql_query = f"SELECT Date FROM {table_name} WHERE symbol='{ticker}';"
            info_date = self.fetch_and_convert(sql_query, cursor, as_list=True)
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
                    self.fin_updated = True
                    financials = self.update_data_keys(financials, 2)
                    self.db_manager.add_entry_to_database(ticker, table_name, financials)
                    print(f'+ {ticker}/{str(table_name).upper()}: Adding entries from {fin_date.strftime("%Y-%m-%d")}')
                else:
                    self.fin_updated = False
        except Exception as e:
            print(f"Error adding entry to database {table_name} : {ticker} -> {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
            conn.rollback()
        return

    @staticmethod
    def sql_to_dict(data:list) -> dict:
        output = {}
        for entry in data:
            if 'googleticker' in entry: #info table
                output = entry
            else: # finance table
                cur_date = None
                for key in entry.keys():
                    if key == 'Date':
                        cur_date = entry[key]
                    elif key != 'symbol':
                        entry_name = re.sub('([a-z])([A-Z])', r'\1 \2', key)
                        if entry_name not in output:
                            output[entry_name] = {}
                        if not entry[key]:
                            entry[key] = 0.0
                        output[entry_name][cur_date] = entry[key]
        return output

    def refresh_rat_table(self, table_name, ticker:str, cursor, conn):
        try:
            sql_query = f"SELECT * FROM {self.db_manager.info_table_name} WHERE symbol='{ticker}';"
            info_data = self.fetch_and_convert(sql_query, cursor)
            info_data = self.sql_to_dict(info_data)
            sql_query = f"SELECT * FROM {self.db_manager.fin_table_name} WHERE symbol='{ticker}' ORDER BY Date ASC;"
            fin_data = self.fetch_and_convert(sql_query, cursor)
            fin_data = self.sql_to_dict(fin_data)
            # rat_data = self.update_rating_items(info_data, fin_data, {})
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
                        for date_key in data.keys():
                            date = datetime.datetime.strptime(date_key, '%Y-%m-%d')
                            if date.date() > ts_date.date():
                                timeseries[date_key] = data[date_key]
                                print(f'+ {ticker}/{str(table_name).upper()}: Adding entry into {ticker} -> {date_key}')
                        self.db_manager.add_entry_to_database(ticker, table_name, timeseries)
        except Exception as e:
            print(f"Error adding entry to database {table_name} : {ticker} -> {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
            conn.rollback()
        return


def main():
    print("Looking for SQLite Stock Database...")
    if os.path.exists("./company_info.db"):
        db = UpdateStockData("company_info.db")
        try:
            conn = db.get_connection()
            cursor = conn.cursor()
            sql_query = f'SELECT name FROM sqlite_master WHERE type=\'table\' ORDER BY name;'
            tables = [db.db_manager.info_table_name, db.db_manager.fin_table_name,
                      db.db_manager.time_table_name, db.db_manager.rate_table_name]
            sql_query = f'SELECT DISTINCT symbol FROM info;'
            tickers = db.fetch_and_convert(sql_query, cursor, as_list=True)
            start_time = time.perf_counter()
            for ticker in tickers:
                for table in tables:
                    db.check_and_update(ticker, table, cursor, conn)
            print("Time taken: {:.2f} seconds".format(time.perf_counter() - start_time))
        except Exception as e:
            print(f"Error : {e.__class__.__name__} {str(e)} at Line: {e.__traceback__.tb_lineno}")
    else:
        print("File company_info.db doesn't exists, check for the database to update")


if __name__ == '__main__':
    main()
