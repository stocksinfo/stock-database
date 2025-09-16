import sqlite3

class DatabaseManager:
    def __init__(self, db_file):
        self.db_file = db_file
        self.init_database()
    pass

    def get_connection(self):
        """Get database connection with foreign key support"""
        conn = sqlite3.connect(self.db_file)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.row_factory = sqlite3.Row  # Enable dict-like access
        return conn

    def init_database(self):
        """Initialize database with all required tables"""
        conn = self.get_connection()
        try:
            self._create_stocks_history(conn)
            self._create_stocks_table(conn)
            print(f"Database Initialized Successfully")
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"Error initializing database: {e}")

    @staticmethod
    def _create_stocks_table(conn):
        conn.execute('''
            CREATE TABLE IF NOT EXISTS company (
                ticker VARCHAR(20) UNIQUE NOT NULL,
                name VARCHAR(255) NOT NULL,
                exchange_name VARCHAR(100) NOT NULL,
                sector VARCHAR(100) NOT NULL,
                industry VARCHAR(100) NOT NULL,
                currency VARCHAR(5),
                data_source VARCHAR(50) DEFAULT 'yahoo',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ticker) REFERENCES timeseries (ticker),
                UNIQUE(ticker, exchange_id)
            )
        ''')
        pass

    @staticmethod
    def _create_stocks_history(conn):
        """Create stocks table"""
        conn.execute('''
            CREATE TABLE IF NOT EXISTS timeseries (
                ticker VARCHAR(20) UNIQUE NOT NULL,
                date TEXT NOT NULL,
                Open REAL,
                High REAL,
                Low REAL,
                Close REAL,
                Volume INTEGER,
                PRIMARY KEY (ticker, date)
            )
        ''')
        pass


if __name__ == "__main__":
    # Test the database
    print("Initializing SQLite Stock Database...")

    # Create database manager
    db = DatabaseManager("stock_database.db")

