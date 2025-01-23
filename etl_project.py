# Importing the required libraries
import sqlite3
from datetime import datetime

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup


# DECLARATIONS
# -----------------------------------------------------------------------------
# Data source location
URL = "https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"

# File names
CSVPATH = "Countries_by_GDP.csv"
DATABASE = "World_Economies.db"
LOGFILE = "etl_project_log.txt"

# Database table and attributes
TABLE_NAME = "Countries_by_GDP"
ATTRIBUTES = ["Country", "GDP_USD_millions"]


# FUNCTIONS definitions
# -----------------------------------------------------------------------------
def extract(url: str, attributes: list) -> pd.DataFrame:
    """
    Extract information from URL and saves it to a dataframe

    Args:
        url        (str): Universal resource locator of source data
        attributes (str): Table attributes of pandas data frame

    Returns:
        pd.DataFrame : Pandas data frame containing extracted data
    """

    # data frame initialisation
    df = pd.DataFrame(columns=attributes)

    # loading webpage for web scraping
    html = requests.get(url).text
    soup = BeautifulSoup(html, "html.parser")

    # scraping data ( 3rd table from URL )
    tables = soup.find_all("tbody")
    rows = tables[2].find_all("tr")

    # table rows loop
    for row in rows:

        # table data and href extraction from row
        cols = row.find_all("td")

        # extract data from valid rows and assign it to data frame
        if len(cols) != 0:
            if cols[0].find('a') is not None and '—' not in cols[2]:
                data_dict = {"Country": cols[0].a.contents[0],
                             "GDP_USD_millions": float(cols[2].contents[0].replace(',',''))}
                dfs = [df,pd.DataFrame(data_dict, index=[0])] 
                df  = pd.concat([df for df in dfs  if not df.empty], ignore_index=True)

    # extracted data frame
    return df


def transform(df: pd.DataFrame):
    """
    Convert GDP data to billions USD rounded by 2 decimal places

    Args:
        df (pd.DataFrame): Pandas data frame

    Returns:
        pd.DataFrame : Pandas data frame containing transformed data
    """

    df.iloc[:, 1] = round(df.iloc[:, 1] * 0.001, 2)
    df=df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"})

    return df


def load_to_csv(df: pd.DataFrame, csv_path: str) -> None:
    """
    Saves dataframe as CSV file in the provided path.

    Args:
        df         (pd.DataFrame): Pandas data frame
        csv_path            (str): CSV file path

    Returns:
        None
    """

    df.to_csv(csv_path)


def load_to_db( df: pd.DataFrame, sql_connection: sqlite3.connect, table_name: str) -> None:
    """
    Saves dataframe as database table with the provided name

    Args:
        data              (pd.DataFrame): Pandas data frame
        sql_connection (sqlite3.connect): SQLite3 connection to database
        table_name                 (str): Name of database table

    Returns:
        None
    """

    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)


def run_query(query_statement: str, sql_connection: sqlite3.connect) -> None:
    """
    Runs query on the database table and prints output

    Args:
        query_statement            (str): SQL query statement
        sql_connection (sqlite3.connect): SQLite3 connection to database

    Returns:
        None
    """

    print(pd.read_sql(query_statement, sql_connection))


def log_progress(message,logfile=LOGFILE):    
    """
    Runs query on the database table and prints output

    Args:
        query_statement            (str): SQL query statement
        sql_connection (sqlite3.connect): SQLite3 connection to database

    Returns:
        None
    """
    timestamp_format = "%Y-%h-%d-%H:%M:%S"  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(logfile, "a") as f:
        f.write(timestamp + " : " + message + "\n")


# Data extraction
log_progress('Preliminaries complete. Initiating ETL process')
df = extract(URL, ATTRIBUTES)


# Data transformation
log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df)


# Storing data in CSV file
log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, CSVPATH)

log_progress('Data saved to CSV file')


# Storing data in a database
conn = sqlite3.connect(DATABASE)
log_progress('SQL Connection initiated.')

load_to_db(df, conn, TABLE_NAME)
log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from {TABLE_NAME} WHERE GDP_USD_billions >= 100"
run_query(query_statement, conn)
log_progress('Process Complete.')

conn.close()
