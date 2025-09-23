from collect_ticker_data import CompanyDataCollector

def main():
    # Create collector instance
    collector = CompanyDataCollector("company_info.json", "ticker.json")
    if collector.all_ticker_symbols:
        collector.start_processing()
    else:
        print("No ticker symbols available")


if __name__ == "__main__":
    main()