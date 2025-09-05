# Code for ETL operations on Country-GDP data
# Import necessary libraries
import pandas as pd
import requests
from bs4 import BeautifulSoup
import sqlite3
import numpy as np
from datetime import datetime

def log_progress(message):
    '''
    This function logs the mentioned message of a given stage of
    the code execution to a log file.
    Function returns nothing
    '''

def extract(url, table_attribs):
    '''
    This function aims to extract the required information from
    the provided url and save it to a data frame.
    The function returns the data frame
    '''

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
