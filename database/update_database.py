import os
import sqlite3

from config.source.basicStockData import BasicStockData
from database.convert_database import DatabaseManager


class UpdateStockData(BasicStockData):
    def __init__(self, db_file="company_info.db"):
        super().__init__()


def main():
    print("Looking for SQLite Stock Database...")
    if os.path.exists("./company_info.db"):
        db = UpdateStockData("company_info.db")
    else:
        print("File company_info.db doesn't exists, check for the database to update")

    pass

if __name__ == '__main__':
    main()
