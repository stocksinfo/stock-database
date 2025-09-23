DROP TABLE IF EXISTS CompanyInfo;
DROP TABLE IF EXISTS Financials;
DROP TABLE IF EXISTS Ratings;
DROP TABLE IF EXISTS TimeSeries;

CREATE TABLE IF NOT EXISTS CompanyInfo (
    symbol VARCHAR(16) PRIMARY KEY,
    date DATETIME DEFAULT CURRENT_TIMESTAMP,
    google_ticker VARCHAR(32),
    longName VARCHAR(128),
    shortName VARCHAR(128),
    country VARCHAR(64),
    industry VARCHAR(128),
    sector VARCHAR(128),
    language VARCHAR(8),
    currency VARCHAR(8),
    exchangeTimezoneShortName VARCHAR(8),
    fullExchangeName VARCHAR(64),
    market VARCHAR(32),
    currentPrice DECIMAL(20,4),
    sharesOutstanding BIGINT,
    marketCap BIGINT,
    debtToEquity DECIMAL(7,3),
    revenueGrowth DECIMAL(7,3),
    trailingEps DECIMAL(7,3),
    freeCashflow VARCHAR(20),
    priceToBook DECIMAL(20,8),
    ebitda DECIMAL(20,4),
    priceToSalesTrailing12Months DECIMAL(20,8),
    returnOnEquity DECIMAL(7,5),
    dividendRate DECIMAL(10,3),
    currentRatio DECIMAL(7,3),
    totalCash BIGINT,
    volume BIGINT
);

CREATE TABLE IF NOT EXISTS Financials (
    Symbol VARCHAR(16) PRIMARY KEY,
    BasicAverageShares BIGINT,
    EBIT DECIMAL(20,4),
    EBITDA DECIMAL(20,4),
    GrossProfit DECIMAL(20,4),
    NetIncome DECIMAL(20,4),
    DilutedEPS DECIMAL(20,4),
    CashAndCashEquivalents DECIMAL(20,4),
    TotalRevenue DECIMAL(20,4),
    StockholdersEquity DECIMAL(20,4),
    TotalAssets DECIMAL(20,4),
    TotalDebt DECIMAL(20,4),
    TotalEquityGrossMinorityInterest DECIMAL(20,4),
    TotalLiabilitiesNetMinorityInterest DECIMAL(20,4),
    CapitalExpenditure DECIMAL(20,4),
    CashDividendsPaid DECIMAL(20,4),
    FreeCashFlow DECIMAL(20,4),
    OperatingCashFlow DECIMAL(20,4)

);

CREATE TABLE IF NOT EXISTS Ratings (
    Symbol VARCHAR(16) PRIMARY KEY,
    RevenueGrowth TEXT,
    ProfitGrowth TEXT,
    EarningsPerShare TEXT,
    AssetsVsLiabilities TEXT,
    DebttoEquityRatio TEXT,
    FreeCashFlow TEXT,
    PricetoEarningsRatio TEXT,
    PricetoSalesRatio TEXT,
    PricetoBookRatio TEXT,
    ReturnonEquity TEXT,
    DividendPayoutRatio TEXT,
    CurrentRatio TEXT,
    MarketCap TEXT,
    PaidDividend TEXT,
    TotalCash TEXT,
    daysLowHigh TEXT,
    EBITxx TEXT
);

CREATE TABLE IF NOT EXISTS TimeSeries (
    Symbol VARCHAR(16),
    Date DATE,
    Value TEXT,
    PRIMARY KEY (Symbol, Date)
);
