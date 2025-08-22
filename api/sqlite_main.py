import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.responses import JSONResponse
from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
from pydantic import BaseModel
import uvicorn

from database.models import DatabaseManager, Stock, StockPrice, TechnicalIndicator, Exchange

# Initialize FastAPI app
app = FastAPI(
    title="Stock Database API",
    description="SQLite-based stock database with historical price data and technical indicators",
    version="1.0.0"
)

# Global database instance
db_manager = None

def get_db_manager():
    """Dependency to get database manager"""
    global db_manager
    if db_manager is None:
        db_manager = DatabaseManager("stock_database.db")
    return db_manager

# Pydantic models for API responses
class StockInfo(BaseModel):
    id: int
    isin: str
    wkn: Optional[str]
    ticker: str
    name: str
    exchange_code: str
    exchange_name: str
    sector: Optional[str]
    industry: Optional[str]
    market_cap_tier: Optional[str]
    currency: Optional[str]
    active: bool

class PriceData(BaseModel):
    date: str
    open_price: Optional[float]
    high_price: Optional[float]
    low_price: Optional[float]
    close_price: Optional[float]
    adjusted_close: Optional[float]
    volume: Optional[int]

class TechnicalIndicatorData(BaseModel):
    date: str
    sma_20: Optional[float]
    sma_50: Optional[float]
    sma_200: Optional[float]
    rsi: Optional[float]
    macd: Optional[float]
    macd_signal: Optional[float]
    bollinger_upper: Optional[float]
    bollinger_lower: Optional[float]

class DatabaseStats(BaseModel):
    total_stocks: int
    total_exchanges: int
    total_price_records: int
    date_range: Dict[str, Optional[str]]
    exchanges: List[Dict[str, Any]]

# API Endpoints
@app.get("/", response_model=Dict[str, str])
async def root():
    """API root endpoint"""
    return {
        "message": "Stock Database API",
        "version": "1.0.0",
        "database": "SQLite",
        "status": "running"
    }

@app.get("/health")
async def health_check(db: DatabaseManager = Depends(get_db_manager)):
    """Health check endpoint"""
    try:
        # Test database connection
        conn = db.get_connection()
        cursor = conn.execute("SELECT COUNT(*) as count FROM stocks")
        stock_count = cursor.fetchone()['count']
        conn.close()
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "database": "connected",
            "stocks_count": stock_count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

@app.get("/exchanges", response_model=List[Dict[str, Any]])
async def get_exchanges(db: DatabaseManager = Depends(get_db_manager)):
    """Get all exchanges"""
    try:
        exchange_manager = Exchange(db)
        exchanges = exchange_manager.get_all()
        return exchanges
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stocks", response_model=List[StockInfo])
async def get_stocks(
    exchange: Optional[str] = Query(None, description="Filter by exchange code"),
    sector: Optional[str] = Query(None, description="Filter by sector"),
    limit: Optional[int] = Query(None, description="Limit number of results"),
    db: DatabaseManager = Depends(get_db_manager)
):
    """Get all stocks with optional filtering"""
    try:
        stock_manager = Stock(db)
        stocks = stock_manager.get_all(exchange_code=exchange, sector=sector)
        
        if limit:
            stocks = stocks[:limit]
            
        return stocks
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stocks/search")
async def search_stocks(
    q: str = Query(..., description="Search term (ticker, name, or ISIN)"),
    db: DatabaseManager = Depends(get_db_manager)
):
    """Search stocks by ticker, name, or ISIN"""
    try:
        conn = db.get_connection()
        query = '''
            SELECT s.*, e.code as exchange_code, e.name as exchange_name
            FROM stocks s
            JOIN exchanges e ON s.exchange_id = e.id
            WHERE s.active = 1 AND (
                s.ticker LIKE ? OR 
                s.name LIKE ? OR 
                s.isin LIKE ?
            )
            ORDER BY 
                CASE WHEN s.ticker = ? THEN 1 ELSE 2 END,
                s.name
            LIMIT 10
        '''
        
        search_term = f"%{q.upper()}%"
        cursor = conn.execute(query, (search_term, search_term, search_term, q.upper()))
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return {"query": q, "results": results, "count": len(results)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stocks/{ticker}", response_model=StockInfo)
async def get_stock_by_ticker(
    ticker: str,
    exchange: Optional[str] = Query(None, description="Exchange code for disambiguation"),
    db: DatabaseManager = Depends(get_db_manager)
):
    """Get stock information by ticker"""
    try:
        stock_manager = Stock(db)
        stock = stock_manager.get_by_ticker(ticker.upper(), exchange)
        
        if not stock:
            raise HTTPException(status_code=404, detail=f"Stock {ticker} not found")
        
        return stock
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stocks/isin/{isin}", response_model=StockInfo)
async def get_stock_by_isin(
    isin: str,
    db: DatabaseManager = Depends(get_db_manager)
):
    """Get stock information by ISIN"""
    try:
        stock_manager = Stock(db)
        stock = stock_manager.get_by_isin(isin.upper())
        
        if not stock:
            raise HTTPException(status_code=404, detail=f"Stock with ISIN {isin} not found")
        
        return stock
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stocks/{ticker}/historical", response_model=List[PriceData])
async def get_historical_prices(
    ticker: str,
    exchange: Optional[str] = Query(None, description="Exchange code"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: Optional[int] = Query(1000, description="Maximum number of records"),
    db: DatabaseManager = Depends(get_db_manager)
):
    """Get historical price data for a stock"""
    try:
        stock_manager = Stock(db)
        stock = stock_manager.get_by_ticker(ticker.upper(), exchange)
        
        if not stock:
            raise HTTPException(status_code=404, detail=f"Stock {ticker} not found")
        
        price_manager = StockPrice(db)
        prices = price_manager.get_historical_data(
            stock_id=stock['id'],
            start_date=start_date,
            end_date=end_date,
            limit=limit
        )
        
        return prices
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stocks/{ticker}/latest")
async def get_latest_price(
    ticker: str,
    exchange: Optional[str] = Query(None, description="Exchange code"),
    db: DatabaseManager = Depends(get_db_manager)
):
    """Get latest price for a stock"""
    try:
        stock_manager = Stock(db)
        stock = stock_manager.get_by_ticker(ticker.upper(), exchange)
        
        if not stock:
            raise HTTPException(status_code=404, detail=f"Stock {ticker} not found")
        
        price_manager = StockPrice(db)
        latest_price = price_manager.get_latest_price(stock['id'])
        
        if not latest_price:
            raise HTTPException(status_code=404, detail=f"No price data found for {ticker}")
        
        return {
            "stock": stock,
            "latest_price": latest_price
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stocks/{ticker}/indicators")
async def get_technical_indicators(
    ticker: str,
    exchange: Optional[str] = Query(None, description="Exchange code"),
    db: DatabaseManager = Depends(get_db_manager)
):
    """Get latest technical indicators for a stock"""
    try:
        stock_manager = Stock(db)
        stock = stock_manager.get_by_ticker(ticker.upper(), exchange)
        
        if not stock:
            raise HTTPException(status_code=404, detail=f"Stock {ticker} not found")
        
        indicator_manager = TechnicalIndicator(db)
        indicators = indicator_manager.get_latest_indicators(stock['id'])
        
        return {
            "stock": stock,
            "indicators": indicators
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats", response_model=DatabaseStats)
async def get_database_stats(db: DatabaseManager = Depends(get_db_manager)):
    """Get database statistics"""
    try:
        conn = db.get_connection()
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
        
        conn.close()
        return stats
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stocks/{ticker}/chart")
async def get_chart_data(
    ticker: str,
    exchange: Optional[str] = Query(None, description="Exchange code"),
    period: str = Query("1y", description="Time period: 1m, 3m, 6m, 1y, 2y, 5y, 10y"),
    db: DatabaseManager = Depends(get_db_manager)
):
    """Get chart data for a stock with different time periods"""
    try:
        stock_manager = Stock(db)
        stock = stock_manager.get_by_ticker(ticker.upper(), exchange)
        
        if not stock:
            raise HTTPException(status_code=404, detail=f"Stock {ticker} not found")
        
        # Calculate date range based on period
        end_date = datetime.now().date()
        period_map = {
            '1m': 30, '3m': 90, '6m': 180,
            '1y': 365, '2y': 730, '5y': 1825, '10y': 3650
        }
        
        days = period_map.get(period, 365)
        start_date = end_date - timedelta(days=days)
        
        price_manager = StockPrice(db)
        prices = price_manager.get_historical_data(
            stock_id=stock['id'],
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d')
        )
        
        # Reverse to get chronological order
        prices.reverse()
        
        return {
            "stock": stock,
            "period": period,
            "data": prices,
            "count": len(prices)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Error handlers
@app.exception_handler(404)
async def not_found_handler(request, exc):
    return JSONResponse(
        status_code=404,
        content={"error": "Not found", "detail": str(exc.detail)}
    )

@app.exception_handler(500)
async def internal_error_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "detail": "An unexpected error occurred"}
    )

if __name__ == "__main__":
    print("🚀 Starting Stock Database API Server...")
    print("📊 Database: SQLite")
    print("🌐 Server: http://localhost:8000")
    print("📖 API Docs: http://localhost:8000/docs")
    
    uvicorn.run(
        "sqlite_main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )