#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import psycopg2
from urllib.request import urlretrieve, urlopen
import sqlparse
import pprint
from multiprocessing import Pool
import re
import time
from psycopg2.errors import InvalidSchemaName, DuplicateTable
#import boto3

# Friends don't let friends hardcode credentials
redshift_endpoint = os.getenv("REDSHIFT_ENDPOINT")
if ':' in redshift_endpoint:
    redshift_endpoint = redshift_endpoint.split(':')[0]
redshift_user = os.getenv("REDSHIFT_USER")
redshift_pass = os.getenv("REDSHIFT_PASS")
redshift_iamrole = os.getenv("REDSHIFT_IAMROLE")

port = 5439
dbname = 'tpcds'
schema = 'tpcds'
usercount = 100

def get_queries():
    with urlopen('https://raw.githubusercontent.com/awslabs/amazon-redshift-utils/master/src/CloudDataWarehouseBenchmark/Cloud-DWB-Derived-from-TPCDS/30TB/ddl.sql') as f:
        raw_queries = f.read().decode('utf-8')

    create_table_statements = []
    copy_table_statements = []
    audit_table_statements = []
    for query in [x.strip() for x in sqlparse.split(raw_queries)]:
        try:
            index_start_comment = query.index('/*',0)
            index_end_comment = query.index('*/',index_start_comment)
            query = query[:index_start_comment] + query[index_end_comment + 2:]
            query = query.strip()
        except ValueError:
            pass

        if query.startswith('select'):
            row_count = int(query.split('--')[-1])
            audit_query = query.split('--')[0]
            audit_table_statements.append((audit_query, row_count))
        elif query.startswith('create'):
            create_table_statements.append(query)
        elif query.startswith('copy'):
            copy_table_statements.append(query)
        else:
            print('This isn\'t supposed to happen...', query)
    return dict(
        create_table_statements = create_table_statements,
        copy_table_statements = copy_table_statements,
        audit_table_statements = audit_table_statements
    )

def run_query_no_results(query, isolation_level=psycopg2.extensions.ISOLATION_LEVEL_DEFAULT):
    with psycopg2.connect(
        host=redshift_endpoint,
        user=redshift_user,
        port=port,
        password=redshift_pass,
        dbname=dbname,
        options=f'-c search_path={schema}'
    ) as conn:
        conn.set_isolation_level(isolation_level)
        cur = conn.cursor()
        cur.execute('SET search_path = {0},public'.format(schema))
        cur.execute(query)
        conn.commit()
    return True

def run_query_one_result(query):
    with psycopg2.connect(
        host=redshift_endpoint,
        user=redshift_user,
        port=port,
        password=redshift_pass,
        dbname=dbname,
        options=f'-c search_path={schema}'
    ) as conn:
        cur = conn.cursor()
        cur.execute('SET search_path = {0},public'.format(schema))
        cur.execute(query)
        conn.commit()
        return cur.fetchone()

def run_query_all_results(query):
    with psycopg2.connect(
        host=redshift_endpoint,
        user=redshift_user,
        port=port,
        password=redshift_pass,
        dbname=dbname,
        options=f'-c search_path={schema}'
    ) as conn:
        cur = conn.cursor()
        cur.execute('SET search_path = {0},public'.format(schema))
        cur.execute(query)
        conn.commit()
        return cur.fetchall()

def do_schema(schema):
    with psycopg2.connect(
        host=redshift_endpoint,
        user=redshift_user,
        port=port,
        password=redshift_pass,
        dbname=dbname,
        options=f'-c search_path={schema}'
    ) as conn:
        cur = conn.cursor()
        cur.execute('CREATE SCHEMA IF NOT EXISTS {0}'.format(schema))
        conn.commit()
        return True
    return False

def do_create(queries):
    for query in queries:
        try:
            run_query_no_results(query)
        except DuplicateTable:
            pass

def do_load(queries):
    p = Pool(5)
    queries = [x.replace('credentials \'aws_access_key_id=<USER_ACCESS_KEY_ID> ;aws_secret_access_key=<USER_SECRET_ACCESS_KEY>\'', 'iam_role \'{0}\''.format(os.environ['REDSHIFT_IAMROLE'])) for x in queries]
    results = p.map(run_query_no_results, queries)

def do_audit(queries):
    for query_tuple in queries:
        query = query_tuple[0]
        tablename = re.match(r'select\ count\(\*\)\ from\ (?P<tablename>\w+);?', query).group('tablename')
        row_count = query_tuple[1]
        result = run_query_one_result(query)
        if result[0] == row_count:
            print('Row Count Matches!', tablename)
        else:
            print('Row Count MisMatch!', tablename, result[0], row_count)

def get_tpcds_tables():
    results = run_query_all_results(r'''
select table_name from information_schema.tables where table_catalog = 'tpcds' and table_schema = 'tpcds' and table_type = 'BASE TABLE'
    ''')
    tables = [x[0] for x in results]
    return tables

def do_vacuum():
    tables = get_tpcds_tables()
    for table in tables:
        run_query_no_results('VACUUM FULL {0}'.format(table), isolation_level = psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

def do_analyze():
    tables = get_tpcds_tables()
    for table in tables:
        run_query_no_results('ANALYZE {0}'.format(table))

def do_set_numRows(queries, database):
    for query_tuple in queries:
        query = query_tuple[0]
        tablename = re.match(r'select\ count\(\*\)\ from\ (?P<tablename>\w+);?', query).group('tablename')
        row_count = query_tuple[1]
        query_to_run = "ALTER TABLE {0}.{1} SET TABLE PROPERTIES ('numRows'='{2}')".format(database, tablename, row_count)
        print(query_to_run)
        run_query_no_results("ALTER TABLE {0}.{1} SET TABLE PROPERTIES ('numRows'='{2}')".format(database, tablename, row_count), isolation_level = psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

def do_create_users(usercount):
    for i in range(0, usercount):
        print(i)
        run_query_no_results('CREATE USER tpcds{0} WITH PASSWORD \'{1}\''.format(str(i).zfill(3), redshift_pass), isolation_level = psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        run_query_no_results('GRANT ALL ON SCHEMA {0} to tpcds{1}'.format(schema, str(i).zfill(3)), isolation_level = psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        run_query_no_results('ALTER USER tpcds{0} SET search_path=\'{1}\''.format(str(i).zfill(3), schema), isolation_level = psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

def do_spectrum_schema():
    query = '''
CREATE EXTERNAL SCHEMA spectrum_tpcds
FROM DATA CATALOG
DATABASE '{0}'
IAM_ROLE '{1}'
CREATE EXTERNAL DATABASE IF NOT EXISTS
    '''.format(database, redshift_iam)
    run_query_no_results(query)

if __name__ == "__main__":
    myqueries = get_queries()
    # for query in sorted(myqueries['audit_table_statements'], key=lambda x: x[1], reverse=True):
    #     sorted_table_list = query[0].strip().split(' ')[-1].rstrip(';')
    # sorted_table_list = [x[0].strip().split(' ')[-1].rstrip(';') for x in sorted(myqueries['audit_table_statements'], key=lambda x: x[1], reverse=True)]
    # print(sorted_table_list)
    # for query in myqueries['copy_table_statements']:
    #     print(query)

    try:
        action = sys.argv[1]
    except IndexError:
        action = None

    if action == 'create':
        do_schema(schema)
        print('Creating Tables')
        start_time = time.time()
        do_create(myqueries['create_table_statements'])
        end_time = time.time()
        print('Create Runtime:', end_time - start_time)

    elif action == 'load':
        print('Loading Tables')
        start_time = time.time()
        do_load(myqueries['copy_table_statements'])
        end_time = time.time()
        print('Load Runtime:', end_time - start_time)

    elif action == 'audit':
        print('Auditing Tables')
        start_time = time.time()
        do_audit(myqueries['audit_table_statements'])
        end_time = time.time()
        print('Audit Runtime:', end_time - start_time)

    elif action == 'vacuum':
        print('Vacuuming Tables')
        start_time = time.time()
        do_vacuum()
        end_time = time.time()
        print('Vacuum Runtime:', end_time - start_time)

    elif action == 'analyze':
        print('Analyzing Tables')
        start_time = time.time()
        do_vacuum()
        end_time = time.time()
        print('Runtime:', end_time - start_time)

    elif action == 'setnumrows':
        try:
            database = sys.argv[2]
        except IndexError:
            print('Usage: python ./load_tpcds.py setnumrows \{database\}')
        print('Setting Number of Rows Tables')
        start_time = time.time()
        do_set_numRows(myqueries['audit_table_statements'], database)
        end_time = time.time()
        print('set numRows Runtime:', end_time - start_time)

    elif action == 'createusers':
        print('Creating 100 Users')
        start_time = time.time()
        do_create_users(99)
        end_time = time.time()
        print('Create Users Runtime:', end_time - start_time)

    elif action == 'all':
        print('Creating Schema')
        do_schema(schema)
        print('Creating Tables')
        start_time = time.time()
        do_create(myqueries['create_table_statements'])
        end_time = time.time()
        print('Create Runtime:', end_time - start_time)

        print('Loading Tables')
        start_time = time.time()
        do_load(myqueries['copy_table_statements'])
        end_time = time.time()
        print('Load Runtime:', end_time - start_time)

        print('Auditing Tables')
        start_time = time.time()
        do_audit(myqueries['audit_table_statements'])
        end_time = time.time()
        print('Audit Runtime:', end_time - start_time)

        print('Vacuuming Tables')
        start_time = time.time()
        do_vacuum()
        end_time = time.time()
        print('Vacuum Runtime:', end_time - start_time)

        print('Analyzing Tables')
        start_time = time.time()
        do_analyze()
        end_time = time.time()
        print('Analyze Runtime:', end_time - start_time)

        print('Creating {0} Users'.format(usercount))
        start_time = time.time()
        do_create_users(usercount-1)
        end_time = time.time()
        print('Create Users Runtime:', end_time - start_time)

        # print('Setting Number of Rows')
        # start_time = time.time()
        # do_set_numRows()
        # end_time = time.time()
        # print('numRows Runtime:', end_time - start_time)

    # elif action == 'athena':
    #     tables = ['store_sales']
    #     athena_client = boto3.client('athena')
    #     for table in tables:
    #         response = athena_client.start_query_execution(
    #             QueryString='SELECT count(*) FROM {0}'.format(table),
    #             #ClientRequestToken='string',
    #             QueryExecutionContext={
    #                 'Database': schema
    #             },
    #             ResultConfiguration={
    #                 'OutputLocation': 'string',
    #                 'EncryptionConfiguration': {
    #                     'EncryptionOption': 'SSE_S3'|'SSE_KMS'|'CSE_KMS',
    #                     'KmsKey': 'string'
    #                 }
    #             },
    #             WorkGroup='string'
    #         )

    else:
        print('Usage: python ./load_tpcds.py [ create | load | audit | vacuum | analyze | all ]')