#!/usr/bin/env python3

"""
Philippine Senate API:
https://open-congress-api.bettergov.ph/api/scalar#description/introduction

Goal: Generate a Dashboard/Report showing the bills created by current senators
"""
import requests
import asyncio
import aiohttp
import duckdb
import pandas as pd
import time
import re

def init_dw_connection():
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
            first_name VARCHAR, 
            middle_name VARCHAR,
            last_name VARCHAR,
            name_prefix VARCHAR,
            name_suffix VARCHAR,
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

    brigde_auth_cong_sql = '''
        CREATE OR REPLACE TABLE bridge_author_congress (
            congress_num VARCHAR,
            author_id VARCHAR,
            type VARCHAR,
            sub_type VARCHAR,
            cong_name VARCHAR,
            PRIMARY KEY (congress_num, author_id),
            FOREIGN KEY (congress_num) REFERENCES dim_congress(congress_num),
            FOREIGN KEY (author_id) REFERENCES dim_author(author_id)
        );
    '''

    db_conn.execute(dim_congress_sql)
    db_conn.execute(dim_author)
    db_conn.execute(fact_tbl_sql)
    db_conn.execute(bridge_sql)
    db_conn.execute(brigde_auth_cong_sql)

def load_congress():
    res_dict = {}
    url = 'https://open-congress-api.bettergov.ph'

    print("Starting dim_congress ingest...\n")
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
                        return (len(rows))
                    else: print('\nNo results match query_param.\n') 
                return None

            except duckdb.ConstraintException as c:
                print(f'ERROR: {c}')
                return None
            except Exception as e:
                print(f'Request failed: {response.status_code}')
                print(f"An unexpected {type(e).__name__} occurred: {e}")
                return None

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

                elif response.status == 500: # retry api call when server error 500 occured. max 5 attempts
                    wait_time = wait_delay * (2 ** attempt)
                    print(f'load_documents(). Server Error 500. Retrying attempt {attempt + 1}/{max_attempts} in {wait_time}s...')
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    print(f'load_documents() ERROR: {response.status}: {await response.text()}, offset: {offset}')
                    return []
                
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                wait_time = wait_delay * (2 ** attempt)
                print(f"load_documents() Network error {type(e).__name__} at offset {offset}. "
                      f"Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
                continue

            except Exception as e:
                print(f'load_documents() ERRROR {type(e).__name__}: {e}')
                return []

        print(f'load_documents() ALL ATEMPTS FAILED AT offset: {offset}')
        return []

async def load_people(url, client, offset, sem):
    max_attempts = 5
    wait_delay = 2

    query_param = {
        'limit':'100',
        'offset': offset
    }

    async with sem:
        for attempt in range(max_attempts):
            try:
                response = await client.get(url+'/api/people', params=query_param)
                if response.status == 200:
                    res_json  = await response.json()
                    result = [ 
                    [   row['id'],
                        row['first_name'],
                        row['middle_name'],
                        row['last_name'],
                        row['name_prefix'],
                        row['name_suffix'],
                        row['aliases']
                    ] for row in res_json['data'] ]
                    return result

                elif response.status == 500:
                    wait_time = wait_delay * (2 ** attempt) # retry api call when server error 500 occured. max 5 attempts
                    print(f'load_people() Server Error 500. Retrying attempt {attempt + 1}/{max_attempts} in {wait_time}s...')
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    print(f'load_people()  ERROR: {response.status}: {await response.text()}, offset: {offset}')
                    return []
                
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                wait_time = wait_delay * (2 ** attempt)
                print(f"load_people()  Network error {type(e).__name__} at offset {offset}. "
                      f"Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
                continue

            except Exception as e:
                print(f'load_people() ERRROR {type(e).__name__}: {e}')
                return []

        print(f'load_people() ALL ATEMPTS FAILED AT offset: {offset}')
        return []
            
async def load_ppl_cong(url, client, sem, id):
    max_attempts = 5
    wait_delay = 2

    async with sem:
        for attempt in range(max_attempts):
            try:
                response = await client.get(url+'/api/people/'+id+'/groups')
                if response.status == 200:
                    res_json  = await response.json()
                    result = [ [ row['congress'], id, row['type'], row['subtype'], row['name'] ] for row in res_json['data'] ]
                    return result

                elif response.status == 500:
                    wait_time = wait_delay * (2 ** attempt) # retry api call when server error 500 occured. max 5 attempts
                    print(f'load_ppl_cong() Server Error 500. Retrying attempt {attempt + 1}/{max_attempts} in {wait_time}s...')
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    print(f'load_ppl_cong() ERROR: {response.status}: {await response.text()}')
                    return []
                
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                wait_time = wait_delay * (2 ** attempt)
                print(f"load_ppl_cong()  Network error {type(e).__name__} "
                      f"Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
                continue

            except Exception as e:
                print(f'load_ppl_cong() ERRROR {type(e).__name__}: {e}')
                return []

        print(f'load_ppl_cong() ALL ATEMPTS FAILED AT offset: {offset}')
        return []
    
async def main_load():

    buffer_threshold = 15000

    url = 'https://open-congress-api.bettergov.ph'

    sem = asyncio.Semaphore(5)
    
    try:
        with duckdb.connect(database='senate_analysis_dw.duckdb') as db_conn:
            fact_columns = db_conn.sql('SELECT * FROM fact_congress_bill').columns
            author_columns = db_conn.sql('SELECT * FROM dim_author').columns
            auth_cong_cols= db_conn.sql('SELECT * FROM bridge_author_congress').columns
            db_conn.execute("SET GLOBAL pandas_analyze_sample=100000")

            async with aiohttp.ClientSession() as client:
                
                print("Starting fact_congress_bill ingest...\n")
                section_running = 'load_documents()'
                async with client.get(url+'/api/documents', params={'limit':'100'}) as response:
                    if response.status != 200:
                        print('ERROR!')
                        return None
                    
                    res = await response.json()
                    total_rows = res['pagination']['total']
                    limit = res['pagination']['limit']

                total_count = 0
                index_start = 0 
                load_rows = 0

                all_start_time = time.time()
                while index_start < total_rows:
                    # limit total processed rows (15,000 rows each iteration) 
                    load_rows = min(index_start+buffer_threshold, total_rows)

                    start_time = time.time()

                    # run concurrent function calls and gather results for all function calls before continuing to next steps
                    tasks = [load_documents(url, client, offset, sem) for offset in range(index_start, load_rows, limit)]
                    results = await asyncio.gather(*tasks)

                    index_start = load_rows # set for next iteration
                    rows = [row for result in results for row in result]

                    # insert gathered rows into duckdb table.
                    if len(rows) > 0:
                        df = pd.DataFrame(data=rows, columns=fact_columns,)
                        db_conn.execute('INSERT OR IGNORE INTO fact_congress_bill SELECT * FROM df')
                        print(f'Successfully Added: {len(rows)} rows into fact_congress_bill records... ({time.time()-start_time:.2f} secs)')
                        total_count += len(rows)
                        rows.clear()

                    await asyncio.sleep(5) 
                print(f"\nFinished fact_congress_bill Ingest! Ingested {total_count} rows in total of {time.time() - all_start_time:.2f} seconds.\n")
                await asyncio.sleep(30)

                print("Starting dim_author ingest...\n")
                section_running = 'load_people()'
                async with client.get(url+'/api/people', params={'limit':'100'}) as response:
                    if response.status != 200:
                        print('ERROR!')
                        return None
                    
                    res = await response.json()
                    total_rows = res['pagination']['total']
                    limit = res['pagination']['limit']

                total_count = 0
                index_start = 0 
                load_rows = 0

                all_start_time = time.time()
                while index_start < total_rows:
                    # limit total processed rows (15,000 rows each iteration) 
                    load_rows = min(index_start+buffer_threshold, total_rows)

                    start_time = time.time()
                    # run concurrent function calls and gather results for all function calls before continuing to next steps
                    tasks = [load_people(url, client, offset, sem) for offset in range(index_start, load_rows, limit)]
                    results = await asyncio.gather(*tasks)

                    index_start = load_rows # set for next iteration
                    rows = [row for result in results for row in result]

                    # insert gathered rows into duckdb table.
                    if len(rows) > 0:
                        df = pd.DataFrame(data=rows, columns=author_columns)
                        db_conn.execute('INSERT OR IGNORE INTO dim_author SELECT * FROM df')
                        print(f'Successfully Added: {len(rows)} rows into dim_author records... ({time.time()-start_time:.2f} secs)')
                        total_count += len(rows)
                        rows.clear()

                    await asyncio.sleep(5) 

                print(f"\nFinished dim_author Ingest! Ingested {total_count} rows in total of {time.time() - all_start_time:.2f} seconds.\n")
                await asyncio.sleep(5)

                print("Starting bridge table bridge_author_congress ingest...\n")

                all_start_time = time.time()

                # get a list of people/authors
                people_list = db_conn.execute('''SELECT author_id FROM dim_author''').fetchall()
                id_list = [id for row in people_list for id in row]

                section_running = 'load_ppl_cong()'
                total_count = 0
                start_time = time.time()

                # run concurrent function calls and gather results for all function calls before continuing to next steps
                tasks = [load_ppl_cong(url, client, sem, id) for id in id_list]
                results = await asyncio.gather(*tasks)

                rows = [row for result in results for row in result]

                # insert gathered rows into duckdb table.
                if len(rows) > 0:
                    df = pd.DataFrame(data=rows, columns=auth_cong_cols)
                    db_conn.execute('INSERT OR IGNORE INTO bridge_author_congress SELECT * FROM df')
                    print(f'Successfully Added: {len(rows)} rows into bridge_author_congress records... ({time.time()-start_time:.2f} secs)')
                    total_count += len(rows)
                    rows.clear()

                print(f"\nFinished bridge_author_congress Ingest! Ingested {total_count} rows in total of {time.time() - start_time:.2f} seconds.\n")
                await asyncio.sleep(5)

    except Exception as e:
        print(f'ERROR at main_load_docs:{section_running}. {type(e).__name__}: {e}')
        return None

def load_ppl_bill():
    insert_row = []

    with duckdb.connect('senate_analysis_dw.duckdb') as db_conn:
        # ingest data to bridge_author_bill based on authors column of fact_congress_bill
        bill_author_list = db_conn.execute('SELECT bill_id, UNNEST(authors) FROM fact_congress_bill WHERE len(authors) > 0').fetchall()
        auth_bill_cols = db_conn.sql('SELECT * from bridge_author_bill').columns

        for row in bill_author_list:
            result = re.search(r'\'id\': (.*?)\,', row[1])
            insert_row.append([row[0], result[1]])

        df = pd.DataFrame(data=insert_row, columns=auth_bill_cols)
        db_conn.execute('INSERT INTO bridge_author_bill SELECT * FROM df') #insert data into duckdb table uwing pandas data frame


if __name__ == '__main__': 
    all_start_time = time.time()
    opt = ''

    print('Choose an option:\n(1) Create Tables\n(2) Data Ingest (may take time to complete)\n(3) Generate Report\n')
    option = int(input('Enter num: '))

    if option == 1: # Create Data tables
        
        init_dw_connection()
        print('\n Data Warehouse Initialized! \n')

        opt = input('\nContinue with Data ingest (y/n)? ')

    if option == 2 or opt.lower() == 'y': # Ingest data into data tables
        start_time = time.time()
        print("Starting optimized data ingestion pipeline...\n")

        total_rows = load_congress() #loads dim_congress table
        if total_rows is not None: print(f"\nFinished dim_congress Ingest! Ingested {total_rows} rows in {time.time() - start_time:.2f} seconds.\n")

        total_rows = asyncio.run(main_load()) # concurrent api calls. loads fact_congress_bill, dim_author & bridge_author_congress tables

        start_time = time.time()
        load_ppl_bill() #loads bridge_author_bill tables
        if total_rows is not None: print(f"\nFinished bridge_author_bill Ingest! Ingested {total_rows} rows in {time.time() - start_time:.2f} seconds.\n")
        
        print(f"\n======== Completed data ingestion! ({time.time() - all_start_time:.2f}s) ========\n")

        option = input('\n Continue with Report Generation (y/n)? ')
        
    if option == 3 or opt.lower() == 'y':
        print('Report Generated!')
    

    print('\nTHANK YOU!\n')
