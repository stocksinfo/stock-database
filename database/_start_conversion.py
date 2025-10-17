import json
from database.convert_database import DatabaseManager

def main():
    print("Initializing SQLite Stock Database...")
    db = DatabaseManager("company_info.db")

    company_info = {}
    # Reading the json file
    with open("../config/company_info.json", "r") as f:
        company_info = json.load(f)
        db.reset_database()
        db.init_database()

    for ticker in sorted(company_info.keys()):
        print(f'Adding {ticker} to the database...')
        for entry in company_info[ticker].keys():
            db.add_entry_to_database(ticker, entry, company_info[ticker][entry])

main()





