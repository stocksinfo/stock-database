import json


inf_file = "../config/company_info.json"
ticker_file = "../config/ticker.json"

all_tickers = {}
companies_data = {}
with open(inf_file, 'r', encoding='utf-8') as f:
    companies_data = json.load(f)
    f.close()

with open(ticker_file, 'r', encoding='utf-8') as f:
    all_tickers = json.load(f)
    f.close()

all_tickers = all_tickers.keys()
missing_tickers = []
missing_info = []
missing_financials = []
missing_timeseries = []

for ticker in all_tickers:
    if not ticker in companies_data:
        print(f'{ticker} : No ticker found')
        missing_tickers.append(ticker)
    elif not "info" in companies_data[ticker]:
        print(f'{ticker} : No company info found ')
        missing_info.append(ticker)
    elif not "financials" in companies_data[ticker]:
        print(f'{ticker} : No company financials found ')
        missing_financials.append(ticker)
    elif not "timeseries" in companies_data[ticker]:
        print(f'{ticker} : No company timeseries found ')
        missing_timeseries.append(ticker)

print("Missing Tickers:    ", missing_tickers)
print("Missing info:       ", missing_info)
print("Missing financials: ", missing_financials)
print("Missing timeseries: ", missing_timeseries)