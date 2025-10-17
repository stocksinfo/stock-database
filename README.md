# stock-database
Database containing historical data of stocks from chosen companies:

./config/ticker.json file has the yfinance ticker of all the companies whose information to be imported
./config/_start.py : file has the main function to execute
./config/collect_ticker_data.py: this file has the class to execute the task
./config/get_yfinance_data.py : this file has all the information and operation to retrieve the data from 
yahoo finance API, this file should be replaced in case another API to be used instead of yfinance. 
./config/basicStockData.py: this file extend get_yfinance_data.py file to include all the operations necessary 
to generate the dictionary per ticker, if another API is used this file should be updated for the base Class

run the _start.py and it will create :
1. ./config/company_info.json file that contains all the ticker related data saved in a json format
2. This uses yfinance for the data collection
3. creats a rating key which will contain all related information to judge a ticker

This database is the entry point

./database/_start_conversion.py : this will use the company_info.json file and convert it to a sqllite database for 
                                  the sake of poratability for the website. 
./database/_start_update.py : this one goes through the company_info.db file and update all the entries 
                              Run this once a day to keep your db file upto date 

