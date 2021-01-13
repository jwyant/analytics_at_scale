#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import psycopg2
from urllib.request import urlretrieve
from multiprocessing import Pool
import json
import re
from random import randint
from pprint import pprint

# Hardcoded credentials are bad
redshift_endpoint = os.getenv("REDSHIFT_ENDPOINT")
redshift_user = os.getenv("REDSHIFT_USER")
redshift_pass = os.getenv("REDSHIFT_PASS")
port = 5439
dbname = 'tpcds'
schema = 'tpcds'

num_streams = int(sys.argv[1]) or 11
try:
    query_tag = sys.argv[2]
except IndexError:
    query_tag = None

ncsqs = [1, 2, 5, 14, 22, 23, 30, 33, 36, 47, 56, 57, 58, 59, 60, 64, 65, 69, 70, 77, 80, 81, 83]
include_ncsqs = False

# Download Queries
def download_queries():
    try:
        os.makedirs('queries')
    except FileExistsError:
        pass
    for i in range(0,11):
        print('Downloading query_{0}.sql...'.format(i))
        urlretrieve('https://raw.githubusercontent.com/awslabs/amazon-redshift-utils/master/src/CloudDataWarehouseBenchmark/Cloud-DWB-Derived-from-TPCDS/30TB/queries/query_{0}.sql'.format(i), 'queries/query_{0}.sql'.format(i))

def format_queries():
    queries = []
    for i in range(0,11):
        with open('queries/query_{0}.sql'.format(i), 'r') as f:
            fullqueryfiletext = f.read()
            nocachequery = ' '.join([x for x in fullqueryfiletext.split(';')[0].splitlines() if not x.startswith('--')])
            print(nocachequery)
            rawqueries = fullqueryfiletext.split(';')[1:]
            iter_rawqueries = iter(rawqueries)
            current, next_item = None, next(iter_rawqueries)
            while True:
                if not current:
                    current, next_item = next_item, next(iter_rawqueries)
                    continue
                try:
                    q = ' '.join([x for x in current.splitlines() if not x.startswith('--') if x])
                    if q.startswith('set'):
                        query_match = re.match(r'''set query_group to \'TPC-DS\ (?P<query_template>(?:(?!\ ).)*)\ stream\.(?P<stream_num>\d+)\.(?P<stream_query_num>\d+)\'''',q)
                        query_dict = dict(
                            query_template = query_match.group('query_template'),
                            stream_num = query_match.group('stream_num'),
                            stream_query_num = query_match.group('stream_query_num'),
                            setquery = q,
                            queries = []
                        )
                        query_dict['setquery'] = q.replace('TPC-DS query', ' '.join(['TPC-DS', query_tag, 'query']))
                        query_dict['query_num'] = re.match(r'query(\d+)\w?.tpl', query_dict['query_template']).group(1)
                        print(query_dict)
                    else:
                        query_dict['queries'].append(q)
                    if (' '.join([x for x in next_item.splitlines() if not x.startswith('--') if x]).startswith('set')) and (include_ncsqs or int(query_dict['query_num']) not in ncsqs):
                        print('Adding', str(query_dict['query_num']))
                        queries.append(query_dict)
                    current, next_item = next_item, next(iter_rawqueries)
                except StopIteration:
                    break
    return nocachequery, queries

def run_queries(inputtuple):
    worker_num = inputtuple[0]
    redshift_tpcds_user = 'tpcds{0}'.format(str(worker_num).zfill(3))
    i = inputtuple[1]
    print('Starting Worker {0}, Stream {1}...'.format(str(worker_num).zfill(2), str(i).zfill(2)))
    with open('queries.json', 'r') as jf:
        dict_queries = json.load(jf)
    conn = psycopg2.connect(
            host=redshift_endpoint,
            user=redshift_tpcds_user,
            port=port,
            password=redshift_pass,
            dbname=dbname,
            options=f'-c search_path={schema}'
        )
    cur = conn.cursor()
    cur.execute(dict_queries['nocachequery'])
    cur.execute('''set search_path to tpcds''')

    stream_queries = [x for x in dict_queries['queries'] if x['stream_num'] == str(i)]
    iter_stream_queries = iter(stream_queries)
    while True:
        try:
            current_query = next(iter_stream_queries)
            cur.execute(current_query['setquery'])
            j = 1
            for q in current_query['queries']:
                cur.execute(q)
                try:
                    print('Worker {0}, Stream {1}, Query {2}, Part {3}, RowCount: {4}, Result {5}'.format(str(worker_num).zfill(2), current_query['stream_num'].zfill(2), current_query['stream_query_num'], j, cur.rowcount, cur.fetchone()))
                except psycopg2.ProgrammingError as e:
                    print(e)
                j += 1
        except StopIteration:
            # Restart iterator
            stream_queries = [x for x in dict_queries['queries'] if x['stream_num'] == str(i)]
            iter_stream_queries = iter(stream_queries)

if __name__ == '__main__':
    download_queries()
    nocachequery, queries = format_queries()
    dict_queries = dict(
        nocachequery = nocachequery,
        queries = queries
    )
    with open('queries.json', 'w') as jf:
        jf.write(json.dumps(dict_queries, indent=4))

    p = Pool(num_streams)
    print(p.map(run_queries, [(x, x%11) for x in range(0,num_streams)]))
