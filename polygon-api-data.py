import pandas as pd
import requests
from datetime import datetime
import time

def fetch_data_from_api(csv_path, api_key, output_csv):
    try:
        df = pd.read_csv(csv_path)
        symbol = df['symbol']
        total_symbols = len(symbol)
        response_count = 0  
        
        with open(output_csv, 'a', newline='') as csvfile: 
            for i, item in enumerate(symbol):
                api_path = f"https://api.polygon.io/v2/aggs/ticker/{item}/range/1/day/2022-03-10/2023-12-31?adjusted=true&sort=asc&limit=50000&apiKey={api_key}"
                response = requests.get(api_path)
                if response.status_code == 200:  
                    data = response.json()['results']  
                    for entry in data:
                        timestamp = entry['t']
                        date = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')  # Convert Unix timestamp to human-readable date
                        # Write data to CSV file immediately
                        csvfile.write(f"{item},{date},{entry['o']},{entry['c']},{entry['h']},{entry['l']},{entry['v']}\n")
                    response_count += 1
                    if response_count == 5:  # Limiting to 5 responses per minute
                        time.sleep(60)  # Sleep for 60 seconds to comply with the rate limit
                        response_count = 0  # Reset response count for the next minute
                else:
                    print(f"Failed to fetch data for symbol {item}. Status code: {response.status_code}")
                
            
                print(f"Processed symbol {i+1}/{total_symbols}")
                
    except Exception as e:
        print(f"An error occurred: {str(e)}")

# Usage:
csv_path = "path_to_file_stocks.csv"
api_key = "add_api_key"
output_csv = "path_to_file_output.csv"

# Run to generate CSV
#fetch_data_from_api(csv_path, api_key, output_csv)
