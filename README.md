# Task:

1. Create a free account with Polygon [Polygon Documentation](https://polygon.io/docs/stocks/getting-started), Polygon provides financial market data.
2. Create a mini Python project where you can run PySpark locally.
3. Attached is a CSV file with the top 100 companies in the S&P 500. Using local PySpark, read in the CSV file and save to a dataframe.
4. Query the Polygon API to get their stock price data per day (closing price) from 2022-03-01 to 2023-12-31, for each company in the CSV file.
5. Write some PySpark transforms to answer the below questions, for simplification you can ignore the effect of dividends. 
    a. Which stock has had the greatest relative increase in price in this period?
    b. If you had invested $1 million at the beginning of this period by purchasing $10,000 worth of shares in every company in the list equally, how much would you have today? Technical note, you can assume that it is possible to purchase fractional shares.


## Explanation:

When interfacing with the Polygon API, certain constraints necessitated a shift from direct integration to an ingestion process.
   
   - API Call Rate Limit: 
      With a restriction of 5 API calls per minute, the "polygon-api-data.py" script was modified to retrieve market data every 60 seconds, incorporating a pause, and then storing data into a CSV file.
   - Historical Data Limitation: 
      Although the requirement for a maximum of 2 years of historical data was met, there were instances within this timeframe where certain days lacked data availability.


## How it works:

### ploygon-api-data.py: 
   This Python script is designed to fetch historical market data for various stock symbols from the Polygon API and store it in a CSV file. Here's a breakdown of what's happening:

   - Imports: 
      The script imports necessary libraries including pandas, requests, datetime, and time.
   
   - Function Definition:
      fetch_data_from_api: This function takes four parameters: csv_path (path to a CSV file containing stock symbols), api_key (API key for accessing the Polygon API), output_csv (path to the output CSV file where the fetched data will be stored).
      Inside the function, it reads the CSV file containing stock symbols using pd.read_csv.
      It then iterates over each symbol and constructs a URL to fetch historical market data for that symbol from the Polygon API.
      For each symbol, it sends a GET request to the API and checks if the request was successful (status code 200).
      If successful, it extracts the relevant data (open, close, high, low, volume) from the API response and writes it to the output CSV file.
      To comply with the rate limit of 5 requests per minute, it pauses execution for 60 seconds after every 5 requests.
      It also handles exceptions and prints error messages if any.
   
   - Usage:
      It provides an example usage of the fetch_data_from_api function, specifying the paths to the input CSV file containing stock symbols, the API key, and the output CSV file where the fetched data will be stored. However, this part is currently commented out (#fetch_data_from_api(csv_path, api_key, output_csv)).

   In summary, this script automates the process of fetching historical market data for multiple stock symbols from the Polygon API and saves it to a CSV file, while adhering to rate limits imposed by the API.

### main.py:
   This code performs data loading, transformation, analysis, and output generation using PySpark, a distributed data processing framework for big data analytics. Here's a breakdown of what's happening:

   - Importing Libraries: 
      The code imports necessary modules from PySpark, such as SparkSession, and functions like col, sum, and max from pyspark.sql.functions.
   
   - Load Data Function:
      The load_data function loads stock and API data from CSV files into Spark DataFrames.
      It takes paths to the stock CSV file (stock_csv_path), API CSV file (api_csv_path), and an optional SparkSession object (spark).
      If no SparkSession object is provided, it creates a new one.
      It reads the CSV files into Spark DataFrames (df_stocks and df_output) using spark.read.csv.
   
   - Transform Data Function:
      The transform_data function transforms the data by joining the stock and API DataFrames, calculating investment values, and identifying the stock with the greatest relative increase in price.
      It takes DataFrames containing stock data (df_stocks) and API data (df_output), and the investment amount per stock (investment_per_stock).
      It joins the DataFrames on the symbol column, calculates the relative increase in price, computes the current value of each investment, and determines the total value of investments and the stock with the greatest relative increase.
   
   - Main Function:
      The main function serves as the entry point to the script.
      It calls the load_data function to load the data into Spark DataFrames.
      It then calls the transform_data function to transform the data and calculate investment-related metrics.
      The results are printed to the console, including the total value of investments and the stock with the greatest relative increase in price.
      Additionally, a portion of the joined DataFrame with relative increase is displayed.
      Finally, it stops the SparkSession to release the allocated resources.
   
   - Execution:
      The script specifies the paths to the stock and API CSV files, and an example investment per stock.
      It calls the main function with these parameters when executed.
      Overall, this code leverages PySpark to analyze financial data efficiently and generate insights into investment opportunities. It demonstrates a typical workflow for processing large datasets and performing complex analytics tasks using distributed computing.
