import os
import time

from database.update_database import UpdateStockData


def main():
    print("Looking for SQLite Stock Database...")
    if os.path.exists("./company_info.db"):
        db = UpdateStockData("company_info.db")
        try:
            conn = db.get_connection()
            cursor = conn.cursor()
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
