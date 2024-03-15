'''Python Project for Data Engineering Course on Coursera'''
import sqlite3
from datetime import datetime
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests



def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("code_log.txt", "a") as f:
        f.write(timestamp + ' : ' + message + '¸\n')

def extract(url, *table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    tables = soup.find_all('tbody')
    rows = tables[0].find_all('tr')[1:]
    df = pd.DataFrame(columns=table_attribs)
    for row in rows:
        col = row.find_all('td')
        data_dict = {"Rank": int(col[0].contents[0][:-1]),
                "Name": str(col[1].contents[2].contents[0]),
                "MC_USD_Billion": float(col[2].contents[0][:-1])}
        df1 = pd.DataFrame(data_dict, index=[0])
        df = pd.concat([df,df1], ignore_index=True)
    print(df)
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    data = pd.read_csv(csv_path).to_dict(orient='split')['data']
    exchange_rate = { d[0]:d[1] for d in data }
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    print(df)
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, header=True, index=False)

def load_to_db(df, conn, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, conn, if_exists='replace', index=False)

def run_query(query_statement, conn):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    query_output = pd.read_sql(query_statement, conn)
    print(query_output)

if __name__ == "__main__":
    DATA_URL = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
    log_progress('Preliminaries complete. Initiating ETL process')
    rankings_df = extract(DATA_URL,"Rank","Name","MC_USD_Billion")
    log_progress('Data extraction complete. Initiating Transformation process')
    rankings_df = transform(rankings_df, 'exchange_rate.csv')
    log_progress('Data transformation complete. Initiating Loading process')
    load_to_csv(rankings_df,'data.csv')
    log_progress('Data saved to CSV file')
    sql_connection = sqlite3.connect('Banks.db')
    log_progress('SQL Connection initiated')
    load_to_db(rankings_df,sql_connection ,'Largest_banks')
    log_progress('Data loaded to Database as a table, Executing queries')
    run_query('SELECT * FROM Largest_banks',sql_connection)
    run_query('SELECT AVG(MC_GBP_Billion) FROM Largest_banks',sql_connection)
    run_query('SELECT Name from Largest_banks LIMIT 5',sql_connection)
    sql_connection.close()
    log_progress('Process Complete')
