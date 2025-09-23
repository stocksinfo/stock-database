from collect_ticker_data_fiscal import CompanyDataCollectorFiscal

def main():
    # Create collector instance
    collector = CompanyDataCollectorFiscal("company_info_fiscal.json", "ticker_fiscal.json")
    if collector.all_ticker_symbols:
        collector.start_processing()
    else:
        print("No ticker symbols available")


if __name__ == "__main__":
    main()