# stock-database
Database containing historical data of stocks from chosen companies:

./config/ticker.json file has the yfinance ticker of all the companies whose information to be imported
./config/_start.py : file has the main function to execute
./config/collect_ticker_data.py: this file has the class to execute the task

run the _start.py and it will create :
1. company_info.json file that contains all the ticker related data saved in a json format
2. This uses yfinance for the data collection
3. creats a rating key which will contain all related information to judge a ticker

This database is the entry point

./database/convert_database.py : this will use the json file and convert it to a sqllite database for 
the sake of poratability for the website. 


