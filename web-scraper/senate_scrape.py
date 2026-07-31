#!/usr/bin/env python3

"""
Philippine Senate API:
https://open-congress-api.bettergov.ph/api/scalar#description/introduction

Goal: Generate a Dashboard Report showing the latest bills
"""

import requests
import asyncio
import aiohttp
import duckdb
import pandas as pd
import time

def init_dw_connection() -> duckdb.DuckDBPyConnection:
    db_conn = duckdb.connect(database='senate_analysis_dw.duckdb')

    # db_conn.execute('CREATE OR REPLACE SCHEMA staging;')
    # db_conn.execute('CREATE OR REPLACE SCHEMA production;')

    # Create fact, dim & bridge Tables
    dim_congress_sql = '''
        CREATE OR REPLACE TABLE dim_congress (
            congress_id VARCHAR PRIMARY KEY,
            cong_num VARCHAR UNIQUE,
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
            authors VARCHAR []
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

async def load_documents(url, client, offset, sem):
    max_attempts = 5
    wait_delay = 2

    query_param = {
        'limit':'100',
        'offset': offset
    }
    async with sem:
        for attempt in range(max_attempts):
            try:
                response = await client.get(url+'/api/documents', params=query_param)
                if response.status == 200:
                    res_json  = await response.json()
                    result = [ 
                    [   row['id'], 
                        row['name'],
                        row['type'],
                        row['congress'],
                        row['title'],
                        row['long_title'],
                        row['date_filed'],
                        row['scope'],
                        row['authors']
                    ] for row in res_json['data'] ]
                    return result

                elif response.status == 500:
                    wait_time = wait_delay * (2 ** attempt)
                    print(f'Server Error 500. Retrying attempt {attempt + 1}/{max_attempts} in {wait_time}s...')
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    print(f'ERROR: {response.status}: {await response.text()}, offset: {offset}')
                    return []
                
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                wait_time = wait_delay * (2 ** attempt)
                print(f" Network error {type(e).__name__} at offset {offset}. "
                      f"Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
                continue

            except Exception as e:
                print(f'ERRROR {type(e).__name__}: {e}')
                return []

        print(f'ALL ATEMPTS FAILED AT offset: {offset}')
        return []

async def main_load_docs():

    buffer_threshold = 15000
    total_count = 0

    url = 'https://open-congress-api.bettergov.ph'

    sem = asyncio.Semaphore(5)
    try:
        with duckdb.connect(database='senate_analysis_dw.duckdb') as db_conn:
            columns = db_conn.sql('SELECT * FROM fact_congress_bill').columns
            db_conn.execute("SET GLOBAL pandas_analyze_sample=100000")

            async with aiohttp.ClientSession() as client:
                async with client.get(url+'/api/documents', params={'limit':'100'}) as response:
                    if response.status != 200:
                        print('ERROR!')
                        return 0
                    
                    res = await response.json()
                    total_rows = res['pagination']['total']
                    limit = res['pagination']['limit']
                    index_start = 0 
                    load_rows = 0

                    while index_start < total_rows:
                        load_rows = min(index_start+buffer_threshold, total_rows)

                        start_time = time.time()
                        tasks = [load_documents(url, client, offset, sem) for offset in range(index_start, load_rows, limit)]
                        results = await asyncio.gather(*tasks)

                        index_start = load_rows # set for next iteration
                        rows = [row for result in results for row in result]
                        
                        if len(rows) > 0:
                            df = pd.DataFrame(data=rows, columns=columns,)
                            db_conn.execute('INSERT OR IGNORE INTO fact_congress_bill SELECT * FROM df')
                            print(f'Successfully Added: {len(rows)} rows into fact_congress_bill records... ({time.time()-start_time:.2f} secs)')
                            total_count += len(rows)
                            rows.clear()

                        await asyncio.sleep(5) 
                    return total_count
    except Exception as e:
        print(f'ERROR at main. {type(e).__name__}: {e}')
        return total_count
        

if __name__ == '__main__':
    # init_dw_connection() 

    url = 'https://open-congress-api.bettergov.ph'

    # load_congress(url)

    print("Starting optimized data ingestion pipeline...\n")
    start_time = time.time()
    total_rows = asyncio.run(main_load_docs())
    print(f"\nFinished! Ingested {total_rows} rows in {time.time() - start_time:.2f} seconds.\n")

