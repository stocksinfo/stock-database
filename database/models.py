import sqlite3
from datetime import datetime, date
from typing import List, Dict, Optional, Union
import json
import os

class DatabaseManager:
    def __init__(self, db_path: str = "stock_database.db"):
        self.db_path = db_path
        self.init_database()
    
    def get_connection(self):
        """Get database connection with foreign key support"""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.row_factory = sqlite3.Row  # Enable dict-like access
        return conn
    
    def init_database(self):
        """Initialize database with all required tables"""
        conn = self.get_connection()
        try:
            # Create tables
            self._create_exchanges_table(conn)
            self._create_stocks_table(conn)
            self._create_stock_prices_table(conn)
            self._create_technical_indicators_table(conn)
            
            conn.commit()
            print("✅ Database initialized successfully")
        except Exception as e:
            conn.rollback()
            print(f"❌ Error initializing database: {e}")
        finally:
            conn.close()
    
    def _create_exchanges_table(self, conn):
        """Create exchanges table"""
        conn.execute('''
            CREATE TABLE IF NOT EXISTS exchanges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code VARCHAR(10) UNIQUE NOT NULL,
                name VARCHAR(100) NOT NULL,
                country VARCHAR(50) NOT NULL,
                timezone VARCHAR(50) NOT NULL,
                currency VARCHAR(3) NOT NULL,
                trading_hours_open TIME,
                trading_hours_close TIME,
                active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
    
    def _create_stocks_table(self, conn):
        """Create stocks table with ISIN and WKN"""
        conn.execute('''
            CREATE TABLE IF NOT EXISTS stocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                isin VARCHAR(12) UNIQUE NOT NULL,
                wkn VARCHAR(10),
                ticker VARCHAR(20) NOT NULL,
                name VARCHAR(255) NOT NULL,
                exchange_id INTEGER NOT NULL,
                sector VARCHAR(100),
                industry VARCHAR(100),
                market_cap_tier VARCHAR(10),
                currency VARCHAR(3),
                active BOOLEAN DEFAULT 1,
                data_source VARCHAR(50) DEFAULT 'yahoo',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (exchange_id) REFERENCES exchanges (id),
                UNIQUE(ticker, exchange_id)
            )
        ''')
    
    def _create_stock_prices_table(self, conn):
        """Create stock prices table for OHLC data"""
        conn.execute('''
            CREATE TABLE IF NOT EXISTS stock_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_id INTEGER NOT NULL,
                date DATE NOT NULL,
                open_price DECIMAL(12,4),
                high_price DECIMAL(12,4),
                low_price DECIMAL(12,4),
                close_price DECIMAL(12,4),
                adjusted_close DECIMAL(12,4),
                volume INTEGER,
                data_source VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (stock_id) REFERENCES stocks (id),
                UNIQUE(stock_id, date)
            )
        ''')
        
        # Create indexes for performance
        conn.execute('CREATE INDEX IF NOT EXISTS idx_stock_prices_date ON stock_prices(date)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_stock_prices_stock_date ON stock_prices(stock_id, date)')
    
    def _create_technical_indicators_table(self, conn):
        """Create technical indicators table"""
        conn.execute('''
            CREATE TABLE IF NOT EXISTS technical_indicators (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock_id INTEGER NOT NULL,
                date DATE NOT NULL,
                sma_20 DECIMAL(12,4),
                sma_50 DECIMAL(12,4),
                sma_200 DECIMAL(12,4),
                rsi DECIMAL(5,2),
                macd DECIMAL(12,6),
                macd_signal DECIMAL(12,6),
                bollinger_upper DECIMAL(12,4),
                bollinger_lower DECIMAL(12,4),
                volume_sma DECIMAL(15,0),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (stock_id) REFERENCES stocks (id),
                UNIQUE(stock_id, date)
            )
        ''')
        
        conn.execute('CREATE INDEX IF NOT EXISTS idx_technical_indicators_stock_date ON technical_indicators(stock_id, date)')


class Exchange:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    def create(self, code: str, name: str, country: str, timezone: str, currency: str,
               trading_hours_open: str = None, trading_hours_close: str = None) -> int:
        """Create new exchange"""
        conn = self.db.get_connection()
        try:
            cursor = conn.execute('''
                INSERT INTO exchanges (code, name, country, timezone, currency, 
                                     trading_hours_open, trading_hours_close)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (code, name, country, timezone, currency, trading_hours_open, trading_hours_close))
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()
    
    def get_by_code(self, code: str) -> Optional[Dict]:
        """Get exchange by code"""
        conn = self.db.get_connection()
        try:
            cursor = conn.execute('SELECT * FROM exchanges WHERE code = ?', (code,))
            row = cursor.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
    
    def get_all(self) -> List[Dict]:
        """Get all exchanges"""
        conn = self.db.get_connection()
        try:
            cursor = conn.execute('SELECT * FROM exchanges WHERE active = 1')
            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()


class Stock:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    def create(self, isin: str, ticker: str, name: str, exchange_code: str,
               wkn: str = None, sector: str = None, industry: str = None,
               market_cap_tier: str = None, currency: str = None) -> int:
        """Create new stock"""
        conn = self.db.get_connection()
        try:
            # Get exchange ID
            cursor = conn.execute('SELECT id FROM exchanges WHERE code = ?', (exchange_code,))
            exchange_row = cursor.fetchone()
            if not exchange_row:
                raise ValueError(f"Exchange {exchange_code} not found")
            
            exchange_id = exchange_row['id']
            
            # Insert stock
            cursor = conn.execute('''
                INSERT INTO stocks (isin, wkn, ticker, name, exchange_id, sector, 
                                  industry, market_cap_tier, currency)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (isin, wkn, ticker, name, exchange_id, sector, industry, market_cap_tier, currency))
            
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()
    
    def get_by_isin(self, isin: str) -> Optional[Dict]:
        """Get stock by ISIN"""
        conn = self.db.get_connection()
        try:
            cursor = conn.execute('''
                SELECT s.*, e.code as exchange_code, e.name as exchange_name
                FROM stocks s
                JOIN exchanges e ON s.exchange_id = e.id
                WHERE s.isin = ?
            ''', (isin,))
            row = cursor.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
    
    def get_by_ticker(self, ticker: str, exchange_code: str = None) -> Optional[Dict]:
        """Get stock by ticker and exchange"""
        conn = self.db.get_connection()
        try:
            if exchange_code:
                cursor = conn.execute('''
                    SELECT s.*, e.code as exchange_code, e.name as exchange_name
                    FROM stocks s
                    JOIN exchanges e ON s.exchange_id = e.id
                    WHERE s.ticker = ? AND e.code = ?
                ''', (ticker, exchange_code))
            else:
                cursor = conn.execute('''
                    SELECT s.*, e.code as exchange_code, e.name as exchange_name
                    FROM stocks s
                    JOIN exchanges e ON s.exchange_id = e.id
                    WHERE s.ticker = ?
                ''', (ticker,))
            
            row = cursor.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
    
    def get_all(self, exchange_code: str = None, sector: str = None) -> List[Dict]:
        """Get all stocks with optional filters"""
        conn = self.db.get_connection()
        try:
            query = '''
                SELECT s.*, e.code as exchange_code, e.name as exchange_name
                FROM stocks s
                JOIN exchanges e ON s.exchange_id = e.id
                WHERE s.active = 1
            '''
            params = []
            
            if exchange_code:
                query += ' AND e.code = ?'
                params.append(exchange_code)
            
            if sector:
                query += ' AND s.sector = ?'
                params.append(sector)
            
            query += ' ORDER BY s.name'
            
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()


class StockPrice:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    def bulk_insert(self, stock_id: int, price_data: List[Dict], data_source: str = 'yahoo') -> int:
        """Bulk insert price data"""
        conn = self.db.get_connection()
        try:
            inserted = 0
            for data in price_data:
                try:
                    conn.execute('''
                        INSERT OR REPLACE INTO stock_prices 
                        (stock_id, date, open_price, high_price, low_price, 
                         close_price, adjusted_close, volume, data_source)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        stock_id, data['date'], data.get('open'), data.get('high'),
                        data.get('low'), data.get('close'), data.get('adjusted_close'),
                        data.get('volume'), data_source
                    ))
                    inserted += 1
                except sqlite3.Error as e:
                    print(f"Error inserting price data for date {data.get('date')}: {e}")
                    continue
            
            conn.commit()
            return inserted
        finally:
            conn.close()
    
    def get_historical_data(self, stock_id: int, start_date: str = None, 
                          end_date: str = None, limit: int = None) -> List[Dict]:
        """Get historical price data"""
        conn = self.db.get_connection()
        try:
            query = '''
                SELECT * FROM stock_prices 
                WHERE stock_id = ?
            '''
            params = [stock_id]
            
            if start_date:
                query += ' AND date >= ?'
                params.append(start_date)
            
            if end_date:
                query += ' AND date <= ?'
                params.append(end_date)
            
            query += ' ORDER BY date DESC'
            
            if limit:
                query += ' LIMIT ?'
                params.append(limit)
            
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()
    
    def get_latest_price(self, stock_id: int) -> Optional[Dict]:
        """Get latest price for a stock"""
        conn = self.db.get_connection()
        try:
            cursor = conn.execute('''
                SELECT * FROM stock_prices 
                WHERE stock_id = ? 
                ORDER BY date DESC 
                LIMIT 1
            ''', (stock_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
    
    def get_data_range(self, stock_id: int) -> Dict:
        """Get date range of available data"""
        conn = self.db.get_connection()
        try:
            cursor = conn.execute('''
                SELECT 
                    MIN(date) as start_date,
                    MAX(date) as end_date,
                    COUNT(*) as total_records
                FROM stock_prices 
                WHERE stock_id = ?
            ''', (stock_id,))
            row = cursor.fetchone()
            return dict(row) if row else {}
        finally:
            conn.close()


class TechnicalIndicator:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    def calculate_and_store_sma(self, stock_id: int, periods: List[int] = [20, 50, 200]):
        """Calculate and store Simple Moving Averages"""
        conn = self.db.get_connection()
        try:
            for period in periods:
                query = f'''
                    INSERT OR REPLACE INTO technical_indicators 
                    (stock_id, date, sma_{period})
                    SELECT 
                        stock_id,
                        date,
                        AVG(close_price) OVER (
                            PARTITION BY stock_id 
                            ORDER BY date 
                            ROWS BETWEEN {period-1} PRECEDING AND CURRENT ROW
                        ) as sma_{period}
                    FROM stock_prices 
                    WHERE stock_id = ?
                    ORDER BY date
                '''
                conn.execute(query, (stock_id,))
            
            conn.commit()
            print(f"✅ SMA indicators calculated for stock_id {stock_id}")
        except Exception as e:
            print(f"❌ Error calculating SMA: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def get_latest_indicators(self, stock_id: int) -> Optional[Dict]:
        """Get latest technical indicators"""
        conn = self.db.get_connection()
        try:
            cursor = conn.execute('''
                SELECT * FROM technical_indicators 
                WHERE stock_id = ? 
                ORDER BY date DESC 
                LIMIT 1
            ''', (stock_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()


# Database initialization and sample data
def initialize_sample_data(db_manager: DatabaseManager):
    """Initialize database with sample exchanges and stocks"""
    exchange_manager = Exchange(db_manager)
    stock_manager = Stock(db_manager)
    
    # Sample exchanges
    exchanges_data = [
        ("NYSE", "New York Stock Exchange", "USA", "America/New_York", "USD", "09:30", "16:00"),
        ("NASDAQ", "NASDAQ", "USA", "America/New_York", "USD", "09:30", "16:00"),
        ("XETRA", "Deutsche Börse XETRA", "Germany", "Europe/Berlin", "EUR", "09:00", "17:30"),
        ("LSE", "London Stock Exchange", "UK", "Europe/London", "GBP", "08:00", "16:30"),
        ("TSE", "Tokyo Stock Exchange", "Japan", "Asia/Tokyo", "JPY", "09:00", "15:00"),
    ]
    
    for exchange_data in exchanges_data:
        try:
            exchange_manager.create(*exchange_data)
            print(f"✅ Created exchange: {exchange_data[0]}")
        except sqlite3.IntegrityError:
            print(f"⚠️  Exchange {exchange_data[0]} already exists")
    
    # Sample stocks
    stocks_data = [
        ("US0378331005", "865985", "AAPL", "Apple Inc.", "NASDAQ", "Technology", "Consumer Electronics", "large", "USD"),
        ("US5949181045", "870747", "MSFT", "Microsoft Corporation", "NASDAQ", "Technology", "Software", "large", "USD"),
        ("DE0007164600", "716460", "SAP", "SAP SE", "XETRA", "Technology", "Software", "large", "EUR"),
        ("GB0031348658", "851247", "VOD.L", "Vodafone Group plc", "LSE", "Communication Services", "Telecom Services", "large", "GBP"),
        ("JP3633400001", "853687", "7203.T", "Toyota Motor Corporation", "TSE", "Consumer Cyclical", "Auto Manufacturers", "large", "JPY"),
    ]
    
    for stock_data in stocks_data:
        try:
            stock_manager.create(*stock_data)
            print(f"✅ Created stock: {stock_data[2]} ({stock_data[3]})")
        except sqlite3.IntegrityError:
            print(f"⚠️  Stock {stock_data[2]} already exists")
        except ValueError as e:
            print(f"❌ Error creating stock {stock_data[2]}: {e}")


if __name__ == "__main__":
    # Test the database
    print("Initializing SQLite Stock Database...")
    
    # Create database manager
    db = DatabaseManager("stock_database.db")
    
    # Initialize sample data
    initialize_sample_data(db)
    
    # Test queries
    stock_manager = Stock(db)
    stocks = stock_manager.get_all()
    print(f"\n📊 Total stocks in database: {len(stocks)}")
    
    for stock in stocks:
        print(f"  • {stock['ticker']} ({stock['name']}) - {stock['exchange_code']}")
    
    print("\n✅ Database setup complete!")