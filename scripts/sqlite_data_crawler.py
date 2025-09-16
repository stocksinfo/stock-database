import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.models import DatabaseManager, Stock, StockPrice, TechnicalIndicator
from crawlers.web_crawler import YahooFinanceCrawler
from datetime import datetime, timedelta
import json
import time
from typing import List, Dict

class SQLiteDataCrawler:
    def __init__(self, db_path: str = "stock_database.db"):
        self.db = DatabaseManager(db_path)
        self.stock_manager = Stock(self.db)
        self.price_manager = StockPrice(self.db)
        self.indicator_manager = TechnicalIndicator(self.db)
        self.crawler = YahooFinanceCrawler()
        
        print(f"✅ Initialized SQLite Data Crawler with database: {db_path}")
    
    def load_companies_from_config(self, config_path: str = "config/companies.json") -> List[Dict]:
        """Load companies from configuration file"""
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            return config.get('companies', [])
        except FileNotFoundError:
            print(f"❌ Configuration file not found: {config_path}")
            return []
        except json.JSONDecodeError as e:
            print(f"❌ Error parsing configuration file: {e}")
            return []
    
    def add_companies_to_database(self, companies: List[Dict]) -> int:
        """Add companies from config to database"""
        added_count = 0
        
        for company in companies:
            if not company.get('active', True):
                continue
                
            try:
                # Extract exchange code from ticker if needed
                ticker = company['ticker']
                exchange_code = company['exchange']
                
                stock_id = self.stock_manager.create(
                    isin=company['isin'],
                    wkn=company.get('wkn'),
                    ticker=ticker,
                    name=company['name'],
                    exchange_code=exchange_code,
                    sector=company.get('sector'),
                    industry=company.get('industry'),
                    market_cap_tier=company.get('market_cap_tier'),
                    currency=self._get_currency_for_exchange(exchange_code)
                )
                
                print(f"✅ Added {ticker} ({company['name']}) - ID: {stock_id}")
                added_count += 1
                
            except Exception as e:
                print(f"⚠️  Error adding {company.get('ticker', 'unknown')}: {e}")
                continue
        
        return added_count
    
    def _get_currency_for_exchange(self, exchange_code: str) -> str:
        """Get default currency for exchange"""
        currency_map = {
            'NASDAQ': 'USD', 'NYSE': 'USD',
            'XETRA': 'EUR', 'LSE': 'GBP',
            'TSE': 'JPY', 'BSE': 'INR',
            'TSX': 'CAD', 'ASX': 'AUD',
            'BOVESPA': 'BRL', 'SSE': 'CNY'
        }
        return currency_map.get(exchange_code, 'USD')
    
    def crawl_historical_data(self, years: int = 10, batch_size: int = 5) -> Dict:
        """Crawl historical data for all stocks in database"""
        stocks = self.stock_manager.get_all()
        
        if not stocks:
            print("❌ No stocks found in database")
            return {'success': 0, 'failed': 0, 'total': 0}
        
        print(f"🚀 Starting historical data crawl for {len(stocks)} stocks ({years} years)")
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=years * 365)
        
        success_count = 0
        failed_count = 0
        
        for i, stock in enumerate(stocks, 1):
            print(f"\n📊 [{i}/{len(stocks)}] Processing {stock['ticker']} ({stock['name']})")
            
            try:
                # Check if we already have recent data
                existing_data = self.price_manager.get_data_range(stock['id'])
                if existing_data.get('total_records', 0) > 0:
                    print(f"  ℹ️  Found {existing_data['total_records']} existing records")
                    print(f"  📅 Date range: {existing_data['start_date']} to {existing_data['end_date']}")
                
                # Fetch historical data
                historical_data = self.crawler.get_historical_data(
                    ticker=stock['ticker'],
                    isin=stock['isin'],
                    start_date=start_date.strftime('%Y-%m-%d'),
                    end_date=end_date.strftime('%Y-%m-%d')
                )
                
                if not historical_data:
                    print(f"  ❌ No data retrieved for {stock['ticker']}")
                    failed_count += 1
                    continue
                
                # Store data in database
                inserted = self.price_manager.bulk_insert(
                    stock_id=stock['id'],
                    price_data=historical_data,
                    data_source='yahoo'
                )
                
                print(f"  ✅ Inserted {inserted} price records")
                
                # Calculate technical indicators
                self.indicator_manager.calculate_and_store_sma(stock['id'])
                
                success_count += 1
                
                # Rate limiting - pause between stocks
                if i % batch_size == 0:
                    print(f"  ⏸️  Batch complete, pausing for 2 seconds...")
                    time.sleep(2)
                else:
                    time.sleep(0.5)
                    
            except Exception as e:
                print(f"  ❌ Error processing {stock['ticker']}: {e}")
                failed_count += 1
                continue
        
        result = {
            'success': success_count,
            'failed': failed_count, 
            'total': len(stocks)
        }
        
        print(f"\n🎉 Crawl complete!")
        print(f"  ✅ Successful: {success_count}")
        print(f"  ❌ Failed: {failed_count}")
        print(f"  📊 Total: {len(stocks)}")
        
        return result
    
    def update_single_stock(self, ticker: str, exchange_code: str = None) -> bool:
        """Update data for a single stock"""
        stock = self.stock_manager.get_by_ticker(ticker, exchange_code)
        if not stock:
            print(f"❌ Stock {ticker} not found in database")
            return False
        
        print(f"🔄 Updating {ticker} ({stock['name']})")
        
        try:
            # Get last 30 days of data
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            
            historical_data = self.crawler.get_historical_data(
                ticker=stock['ticker'],
                isin=stock['isin'],
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d')
            )
            
            if historical_data:
                inserted = self.price_manager.bulk_insert(
                    stock_id=stock['id'],
                    price_data=historical_data,
                    data_source='yahoo'
                )
                
                # Update technical indicators
                self.indicator_manager.calculate_and_store_sma(stock['id'])
                
                print(f"✅ Updated {inserted} records for {ticker}")
                return True
            else:
                print(f"❌ No data retrieved for {ticker}")
                return False
                
        except Exception as e:
            print(f"❌ Error updating {ticker}: {e}")
            return False
    
    def get_database_stats(self) -> Dict:
        """Get database statistics"""
        conn = self.db.get_connection()
        try:
            stats = {}
            
            # Stock count
            cursor = conn.execute('SELECT COUNT(*) as count FROM stocks WHERE active = 1')
            stats['total_stocks'] = cursor.fetchone()['count']
            
            # Exchange count
            cursor = conn.execute('SELECT COUNT(*) as count FROM exchanges WHERE active = 1')
            stats['total_exchanges'] = cursor.fetchone()['count']
            
            # Price records count
            cursor = conn.execute('SELECT COUNT(*) as count FROM stock_prices')
            stats['total_price_records'] = cursor.fetchone()['count']
            
            # Date range
            cursor = conn.execute('''
                SELECT 
                    MIN(date) as earliest_date,
                    MAX(date) as latest_date
                FROM stock_prices
            ''')
            date_range = cursor.fetchone()
            stats['date_range'] = {
                'earliest': date_range['earliest_date'],
                'latest': date_range['latest_date']
            }
            
            # Top exchanges by stock count
            cursor = conn.execute('''
                SELECT e.code, e.name, COUNT(s.id) as stock_count
                FROM exchanges e
                LEFT JOIN stocks s ON e.id = s.exchange_id AND s.active = 1
                GROUP BY e.id, e.code, e.name
                ORDER BY stock_count DESC
            ''')
            stats['exchanges'] = [dict(row) for row in cursor.fetchall()]
            
            return stats
            
        finally:
            conn.close()
    
    def print_database_stats(self):
        """Print database statistics"""
        stats = self.get_database_stats()
        
        print("\n📊 DATABASE STATISTICS")
        print("=" * 50)
        print(f"📈 Total Stocks: {stats['total_stocks']}")
        print(f"🏢 Total Exchanges: {stats['total_exchanges']}")
        print(f"💾 Total Price Records: {stats['total_price_records']:,}")
        print(f"📅 Date Range: {stats['date_range']['earliest']} to {stats['date_range']['latest']}")
        
        print(f"\n🏛️  EXCHANGES:")
        for exchange in stats['exchanges']:
            print(f"  • {exchange['code']}: {exchange['stock_count']} stocks")


def main():
    """Main execution function"""
    print("🚀 SQLite Stock Database Crawler")
    print("=" * 50)
    
    # Initialize crawler
    crawler = SQLiteDataCrawler()
    
    # Load and add companies from config
    print("\n1️⃣  Loading companies from configuration...")
    companies = crawler.load_companies_from_config()
    if companies:
        added = crawler.add_companies_to_database(companies)
        print(f"✅ Added {added} companies to database")
    
    # Show current database stats
    crawler.print_database_stats()
    
    # Ask user what to do
    print("\n" + "=" * 50)
    print("CRAWLING OPTIONS:")
    print("1. Crawl ALL historical data (10 years) - Takes 1-2 hours")
    print("2. Update recent data only (30 days)")
    print("3. Crawl specific stock")
    print("4. Show database stats only")
    
    choice = input("\nEnter your choice (1-4): ").strip()
    
    if choice == '1':
        print("\n🚀 Starting full historical data crawl...")
        result = crawler.crawl_historical_data(years=10)
        print(f"\n🎉 Crawl completed: {result['success']}/{result['total']} successful")
        
    elif choice == '2':
        print("\n🔄 Updating recent data for all stocks...")
        stocks = crawler.stock_manager.get_all()
        success = 0
        for stock in stocks:
            if crawler.update_single_stock(stock['ticker'], stock['exchange_code']):
                success += 1
            time.sleep(0.5)
        print(f"\n✅ Updated {success}/{len(stocks)} stocks successfully")
        
    elif choice == '3':
        ticker = input("Enter stock ticker (e.g., AAPL): ").strip().upper()
        exchange = input("Enter exchange code (optional, press Enter to skip): ").strip().upper()
        exchange = exchange if exchange else None
        
        if crawler.update_single_stock(ticker, exchange):
            print(f"✅ Successfully updated {ticker}")
        else:
            print(f"❌ Failed to update {ticker}")
            
    elif choice == '4':
        crawler.print_database_stats()
        
    else:
        print("❌ Invalid choice")
        return
    
    # Final stats
    print("\n" + "=" * 50)
    crawler.print_database_stats()


if __name__ == "__main__":
    main()