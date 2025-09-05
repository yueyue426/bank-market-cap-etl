# Code for ETL operations on Country-GDP data
# Import necessary libraries
import pandas as pd
import requests
from bs4 import BeautifulSoup
import sqlite3
import numpy as np
from datetime import datetime

def log_progress(message):
    """
    This function logs the mentioned message of a given stage of
    the code execution to a log file.
    Function returns nothing
    """
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour:Minute:Second
    now = datetime.now() # get the current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("./code_log.txt", "a") as f:
        f.write(timestamp + " : " + message + "\n")

def extract(url, table_attribs):
    """
    This function aims to extract the required information from
    the provided url and save it to a data frame.
    The function returns the data frame
    """
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    tables = soup.find_all('tbody')
    rows = tables[0].find_all('tr')
    df = pd.DataFrame(columns=table_attribs)

    for row in rows:
        cols = row.find_all('td')
        if len(cols) != 0:
            links = cols[1].find_all('a')
            bank_name = links[1].contents[0]
            market_cap_raw = cols[2].contents[0].replace('\n', '')
            market_cap = float(market_cap_raw) # cast data type as float

            data_dict = {
                "Name": bank_name,
                "MC_USD_Billion": market_cap
            }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)

    return df

def transform(df, csv_path):
    """
    This function accesses the CSV file for exchange rate information,
    and adds three columns to the data frame, each containing the
    transformed version of Market Cap column to respective currencies.
    """
    exchange_rate_df = pd.read_csv(csv_path)

    # Convert exchange rate file to a dictionary
    exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']

    df['MC_GBP_Billion'] = [np.round(x * exchange_rate ['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    return df

def load_to_csv(df, output_path):
    """
    This function saves the final data frame as a CSV file in the
    provided path.
    Function returns nothing.
    """
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    """
    This function saves the final data frame to a database table
    with the provided name.
    Function returns nothing.
    """
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    """
    This function runs the query on the database table and prints
    the output on the terminal.
    Function returns nothing.
    """
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

# ===========================================================================
# Define relevant attributes and processes
url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_path = 'source/exchange_rate.csv'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
output_path = './Largest_banks_data.csv'

log_progress("Preliminaries complete. Initiating ETL process")

df = extract(url, table_attribs)

log_progress("Data extraction complete. Initiating Transform process")

df = transform(df, csv_path)

log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(df, output_path)

log_progress("Data saved to CSV file")

sql_connection = sqlite3.connect(db_name)

log_progress("SQL connection initiated")

load_to_db(df, sql_connection, table_name)

log_progress("Data loaded to Database as table. Running the queries")

log_progress("Running the first query:")

query_statement = f"SELECT * FROM Largest_banks"
run_query(query_statement, sql_connection)

log_progress("Running the second query:")

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, sql_connection)

log_progress("Running the third query:")
query_statement = f"SELECT Name FROM Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)

log_progress("Process Complete.")

sql_connection.close()

