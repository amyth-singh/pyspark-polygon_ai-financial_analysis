from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, max

def load_data(stock_csv_path, api_csv_path, spark=None):

    if spark is None:

        spark = SparkSession.builder \
            .appName("main-api-stock-data") \
            .getOrCreate()


    df_stocks = spark.read.csv(stock_csv_path, header=True, inferSchema=True)
    df_output = spark.read.csv(api_csv_path, header=True, inferSchema=True)

    return df_stocks, df_output, spark

def transform_data(df_stocks, df_output, investment_per_stock):
    joined_df = df_stocks.join(df_output, df_stocks['stock_symbol'] == df_output['symbol'], 'inner')
    joined_df = joined_df.withColumn("relative_increase", (col("close") - col("open")) / col("open"))
    joined_df = joined_df.withColumn("current_value", col("close") * (investment_per_stock / col("open")))
    total_value = joined_df.select(sum("current_value")).collect()[0][0]
    greatest_increase = joined_df.select(max("relative_increase")).collect()[0][0]
    greatest_increase_stock = joined_df.filter(col("relative_increase") == greatest_increase).select("symbol", "relative_increase").collect()[0]

    return total_value, greatest_increase_stock, joined_df

def main(stock_csv_path, api_csv_path, investment_per_stock):

    # Load data
    df_stocks, df_output, spark = load_data(stock_csv_path, api_csv_path)

    # Transform data and calculate total value
    total_value, greatest_increase_stock, joined_df = transform_data(df_stocks, df_output, investment_per_stock)

    # Output
    print(f"The total value of your investment would be: ${total_value:.2f}")
    print(f"The stock with the greatest relative increase in price is: {greatest_increase_stock[0]} with a relative increase of {greatest_increase_stock[1]:.2f}")

    # Show joined DataFrame with relative increase
    print("Joined DataFrame with relative increase:")
    joined_df.show(10)

    # Stop SparkSession
    if spark:
        spark.stop()

if __name__ == "__main__":
    stock_csv_path = "path to the stocks.csv file"
    api_csv_path = "path to the output.csv file"
    investment_per_stock = 1000000 / 10  
    main(stock_csv_path, api_csv_path, investment_per_stock)
