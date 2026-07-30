#!/usr/bin/env python3

"""
Philippine Senate API:
https://open-congress-api.bettergov.ph/api/scalar#description/introduction

Goal: Generate a Dashboard Report showing the latest bills
"""

import requests
import asyncio
import httpx
import duckdb
import pandas as pd
import time

db_lock = asyncio.Lock()
rows = []

def init_dw_connection() -> duckdb.DuckDBPyConnection:
    db_conn = duckdb.connect(database='senate_analysis_dw.duckdb')

    # db_conn.execute('CREATE OR REPLACE SCHEMA staging;')
    # db_conn.execute('CREATE OR REPLACE SCHEMA production;')

    # Create fact, dim & bridge Tables
    dim_congress_sql = '''
        CREATE OR REPLACE TABLE dim_congress (
            congress_id VARCHAR PRIMARY KEY,
            cong_num VARCHAR,
            cong_name VARCHAR, 
            cong_ordinal VARCHAR,
            start_date DATE,
            end_date DATE,
            start_year INTEGER,
            end_year INTEGER,
            year_range VARCHAR
        );
    '''
    

    dim_author = '''
        CREATE OR REPLACE TABLE dim_author (
            author_id VARCHAR PRIMARY KEY,
            author_type VARCHAR,
            first_name VARCHAR, 
            middle_name VARCHAR,
            last_name VARCHAR,
            alias VARCHAR
        );
    '''

    fact_tbl_sql = '''
        CREATE OR REPLACE TABLE fact_congress_bill (
            bill_id VARCHAR PRIMARY KEY ,
            bill_name VARCHAR,
            type VARCHAR,
            congress_id VARCHAR,
            short_title VARCHAR,
            long_title VARCHAR,
            date_filled DATE,
            scope VARCHAR,
            FOREIGN KEY (congress_id) REFERENCES dim_congress(congress_id)
        );
    '''

    bridge_sql = '''
        CREATE OR REPLACE TABLE bridge_author_bill (
            bill_id VARCHAR,
            author_id VARCHAR,
            PRIMARY KEY (bill_id, author_id),
            FOREIGN KEY (bill_id) REFERENCES fact_congress_bill(bill_id),
            FOREIGN KEY (author_id) REFERENCES dim_author(author_id)
        );
    '''

    db_conn.execute(dim_congress_sql)
    db_conn.execute(dim_author)
    db_conn.execute(fact_tbl_sql)
    db_conn.execute(bridge_sql)

    print('Data Warehouse Initialized')

def insert_dw_tables(db_conn:duckdb.DuckDBPyConnection, tbl_name: str):
    db_conn.execute(
        'INSERT INTO query_table($1) COLUMNS(?)',
        [tbl_name, 1]
    )

def load_congress(url: str) -> dict:

    res_dict = {}
    
    with duckdb.connect(database='senate_analysis_dw.duckdb') as db_conn:
        columns = db_conn.sql('SELECT * FROM dim_congress').columns
        rows = []
        with requests.Session() as sesh:   

            try:
                response = sesh.get(url+'/api/congresses')
                
                if response.status_code == 200 :
                    res_dict = response.json()
                    if len(res_dict['data']) > 0:
                        for row in res_dict['data']:
                            insert_cong_deets = []
                            insert_cong_deets.append(row['id'])
                            insert_cong_deets.append(row['congress_number'])
                            insert_cong_deets.append(row['name'])
                            insert_cong_deets.append(row['ordinal'])
                            insert_cong_deets.append(row['start_date'])
                            insert_cong_deets.append(row['end_date'])
                            insert_cong_deets.append(row['start_year'])
                            insert_cong_deets.append(row['end_year'])
                            insert_cong_deets.append(row['year_range'])
                            rows.append(insert_cong_deets)

                        df = pd.DataFrame(data=rows,columns=columns)
                        db_conn.execute('INSERT OR IGNORE INTO dim_congress SELECT * FROM df')
                        print(df)

                    else: print('\nNo results match query_param.\n')

            except duckdb.ConstraintException as c:
                print(f'ERROR: {c}')
            except Exception as e:
                print(f'Request failed: {response.status_code}')
                print(f"An unexpected {type(e).__name__} occurred: {e}")

async def load_documents(url, client, offset):
    global rows
    query_param = {
        'limit':'100',
        'offset': offset
    }

    try:
        response = await client.get(url+'/api/documents', params=query_param)
        if response.status_code == 200:
            res_json  = response.json()
            
            for row in res_json['data']:
                print(row)
                insert_doc = []
                insert_doc.append(row['id'])
                insert_doc.append(row['name'])
                insert_doc.append(row['type'])
                with duckdb.connect(database='senate_analysis_dw.duckdb') as db_conn:
                    cong_id = db_conn.execute('SELECT congress_id FROM dim_congress WHERE cong_num = ?',[row['congress']]).fetchone()
                insert_doc.append(str(cong_id[0]))
                insert_doc.append(row['title'])
                insert_doc.append(row['long_title'])
                insert_doc.append(row['date_filed'])
                insert_doc.append(row['scope'])
                rows.append(insert_doc)

            if len(rows) >= 10000:
                async with db_lock:
                    with duckdb.connect(database='senate_analysis_dw.duckdb') as db_conn:
                        columns = db_conn.sql('SELECT * FROM fact_congress_bill').columns

                    df = pd.DataFrame(data=rows, columns=columns)
                    db_conn.execute('INSERT OR IGNORE INTO fact_congress_bill SELECT * FROM df')
                    print(f'Successfully Added: {len(rows)} rows into fact_congress_bill records...')
                    rows.clear()

    except Exception as e:
        print(f'ERRROR {type(e).__name__}: {e}')

async def main_load_docs():
    global rows
    url = 'https://open-congress-api.bettergov.ph'

    with requests.Session() as sesh:
        response = sesh.get(url+'/api/documents', params={'limit':'100'})
        if response.status_code == 200:
            total_rows = response.json()['pagination']['total']
            limit = response.json()['pagination']['limit']

            async with httpx.AsyncClient() as client:
                tasks = [load_documents(url, client, offset) for offset in range(0, total_rows, limit)]
                await asyncio.gather(*tasks)

    if rows:
        async with db_lock:
            with duckdb.connect(database='senate_analysis_dw.duckdb') as db_conn:
                columns = db_conn.sql('SELECT * FROM fact_congress_bill').columns
                df = pd.DataFrame(data=rows, columns=columns)
                db_conn.execute('INSERT OR IGNORE INTO fact_congress_bill SELECT * FROM df')
            print(f'Successfully Added: {len(rows)} rows into fact_congress_bill records...')
    return total_rows
    

if __name__ == '__main__':
    latest_cong = {}
    cong_docs = {}

    # init_dw_connection() 

    url = 'https://open-congress-api.bettergov.ph'

    # load_congress(url)

    print("Starting optimized data ingestion pipeline...\n")
    start_time = time.time()

    total_rows = asyncio.run(main_load_docs())

    end_time = time.time()

    print(f"\nFinished! Ingested {total_rows} rows in {end_time - start_time:.2f} seconds.\n")

