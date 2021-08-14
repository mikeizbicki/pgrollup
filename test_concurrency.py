#!/usr/bin/python3
'''
Test that rollup tables behave sanely under high transaction concurrency.
'''

from multiprocessing import Process, Barrier, Queue
import os
import psycopg2
import random
import time

# setup logging
import logging
logging.basicConfig(
    format='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    )

# process command line args
import argparse
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--db', default='test_concurrency', help='the database to connect work in; WARNING: this will drop and recreate the db if it already exists')
parser.add_argument('--runtime', type=int, default=10,  help='the number of seconds to run transactions for')
parser.add_argument('--numproc', type=int, default=20,  help='the number of concurrent processes inserting transactions')
parser.add_argument('--mode', choices=['manual','cron','trigger'], default='manual',  help='the mode that rollup tables will be created in')
parser.add_argument('--delay_seconds', type=float, default=0.0)
parser.add_argument('--multiplicity', type=int, default=100)
args = parser.parse_args()

################################################################################
# initialize the test database
################################################################################

logging.info('create the database')
conn = psycopg2.connect()
conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT);
try:
    curs = conn.cursor()
    curs.execute(f'CREATE DATABASE {args.db};')
    conn.commit()
except psycopg2.errors.DuplicateDatabase:
    logging.warning(f'dropping database {args.db}')
    curs = conn.cursor()
    curs.execute(f'DROP DATABASE {args.db};')
    curs.execute(f'CREATE DATABASE {args.db};')
    conn.commit()
conn.close()

logging.info('create the relations needed for testing')
init = f'''
-- load pgrollup and configure it for automatic rollup creation to facilitate tests
CREATE OR REPLACE LANGUAGE plpython3u;
{'CREATE EXTENSION IF NOT EXISTS pg_cron;' if args.mode=='cron' else ''}
CREATE EXTENSION IF NOT EXISTS pgrollup;
CREATE EVENT TRIGGER pgrollup_from_matview_trigger ON ddl_command_end WHEN TAG IN ('CREATE MATERIALIZED VIEW') EXECUTE PROCEDURE pgrollup_from_matview_event();
UPDATE pgrollup_settings SET value='{args.mode}' WHERE name='default_mode';

-- we'll insert data into this table
CREATE TABLE test (
    id BIGSERIAL PRIMARY KEY,
    username TEXT,
    num INTEGER
);

-- this is the rollup table that we want to ensure stays correct
CREATE MATERIALIZED VIEW test_view AS (
    SELECT username,sum(num) FROM test GROUP BY username
);
'''
conn = psycopg2.connect(f'dbname={args.db}')
curs = conn.cursor()
curs.execute(init)
conn.commit()

################################################################################
# run the transactions
################################################################################

# the queue will be used to store all the transaction counts returned by each process
queue = Queue()

# the run_barrier is used to ensure that all processes are executing simultaneously;
# no proces will begin running until num_barriers processes have called run_barrier.wait()
num_barriers = args.numproc
if args.mode == 'manual':
    num_barriers += 1
run_barrier = Barrier(num_barriers, timeout=10)


def get_process_function(sql, count_transactions=True, delay_seconds=0, multiplicity=1):
    '''
    Return a function that can be passed to Process.
    The returned function will perform all the transaction work of the process.
    '''
    def anon():
        # create db connection
        conn = psycopg2.connect(f'dbname={args.db}')
        curs = conn.cursor()

        # wait until all other processes are ready
        run_barrier.wait()
        start_time = time.time()

        # loop for the specified time running the transactions
        transactions = 0
        while time.time()-start_time < args.runtime:
            for i in range(multiplicity):
                curs.execute(sql)
            time.sleep(random.random()*delay_seconds)
            conn.commit()
            transactions += 1
        logging.debug(f'finished inserting transactions; total transactions processed: {transactions}')

        # "return" the number of transactions performed
        if count_transactions:
            queue.put(transactions)

        # close the db connection
        conn.close()
    return anon


# valid values for inserting into the test table
usernames = ['alice', 'bob', 'charlie', None]
nums = list(range(5))+[None]

logging.info('create the transaction processes')
procs = []
for i in range(args.numproc):
    username = usernames[i%len(usernames)]
    if username:
        username_str = "'"+username+"'"
    else:
        username_str = 'NULL'
    num = nums[i%len(nums)]
    if num:
        num_str = str(num)
    else:
        num_str = 'NULL'
    sql = f"INSERT INTO test (username,num) VALUES ({username_str}, {num_str});"
    proc = Process(target=get_process_function(sql, multiplicity=args.multiplicity, delay_seconds=args.delay_seconds))
    proc.start()
    procs.append(proc)

if args.mode == 'manual':
    logging.info('create the rollup process (manual mode only)')
    sql = "select do_rollup('test_view');"
    proc = Process(target=get_process_function(sql, count_transactions=False, delay_seconds=1, multiplicity=1))
    proc.start()
    procs.append(proc)

logging.info('waiting for all processes to finish')
total_transactions = 0
for i in range(args.numproc):
    transactions = queue.get()
    total_transactions += transactions
logging.info(f'total_transactions={total_transactions}')
logging.info(f'transactions/second={total_transactions/args.runtime}')
logging.info(f'transactions/second/proc={total_transactions/args.runtime/args.numproc}')
for proc in procs:
    proc.join()

if args.mode in ['manual', 'cron']:
    logging.info('perform final do_rollup to ensure the rollup is up to date')
    sql = "select do_rollup('test_view');"
    curs = conn.cursor()
    curs.execute(sql)

################################################################################
# verify correctness
################################################################################

logging.info('verifying correctness')
curs = conn.cursor()
sql = 'select "test.username","sum" from test_view order by "test.username";'
curs.execute(sql)
results = curs.fetchall()
logging.info(f'results={results}')

curs = conn.cursor()
sql = 'select "test.username","sum" from test_view_groundtruth order by "test.username";'
curs.execute(sql)
groundtruth = curs.fetchall()
logging.info(f'groundtruth={groundtruth}')

assert results==groundtruth
logging.info(f'tests succeeded!')
