# Code for ETL operations on Country-GDP data
# Import necessary libraries
from operator import index

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
    '''
    This function accesses the CSV file for exchange rate information,
    and adds three columns to the data frame, each containing the
    transformed version of Market Cap column to respective currencies.
    '''

    return df

def load_to_csv(df, output_path):
    '''
    This function saves the final data frame as a CSV file in the
    provided path.
    Function returns nothing.
    '''

def load_to_db(df, sql_connection, table_name):
    '''
    This function saves the final data frame to a database table
    with the provided name.
    Function returns nothing.
    '''

def run_query(query_statement, sql_connection):
    '''
    This function runs the query on the database table and prints
    the output on the terminal.
    Function returns nothing.
    '''
