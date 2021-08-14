import time
import psycopg2
from multiprocessing import Process, Barrier

dsn = "dbname=regress"
run_time = 10
num_transactions = 20

################################################################################
# initialize the test database
################################################################################

try:
    conn = psycopg2.connect()
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT);
    curs = conn.cursor()
    curs.execute('CREATE DATABASE regress;')
    conn.commit()
except psycopg2.errors.DuplicateDatabase:
    curs.execute('DROP DATABASE regress;')
    curs.execute('CREATE DATABASE regress;')
    conn.commit()

init = '''
-- load pgrollup and configure it for automatic rollup creation to facilitate tests
CREATE LANGUAGE plpython3u;
CREATE EXTENSION pgrollup;
CREATE EVENT TRIGGER pgrollup_from_matview_trigger ON ddl_command_end WHEN TAG IN ('CREATE MATERIALIZED VIEW') EXECUTE PROCEDURE pgrollup_from_matview_event();
UPDATE pgrollup_settings SET value='manual' WHERE name='default_mode';

-- we'll insert data into this table
CREATE TABLE test (
    id SERIAL PRIMARY KEY,
    username TEXT,
    num INTEGER
);

-- this is the rollup table that we want to ensure stays correct
CREATE MATERIALIZED VIEW test_view AS (
    SELECT username,sum(num) FROM test GROUP BY username
);
'''
conn = psycopg2.connect(dsn)
curs = conn.cursor()
curs.execute(init)
conn.commit()

################################################################################
# run the transactions
################################################################################

run_barrier = Barrier(num_transactions+1, timeout=10)

def get_process_function(sql, delay_seconds=0):
    def anon():
        # create db connection
        conn = psycopg2.connect(dsn)
        curs = conn.cursor()

        # wait until all other processes are ready
        run_barrier.wait()
        start_time = time.time()

        # loop for the specified time running the transactions
        transactions = 0
        while time.time()-start_time < run_time:
            curs.execute(sql)
            conn.commit()
            transactions += 1
            time.sleep(delay_seconds)
        print("transactions=",transactions)

        # close the db connection
        conn.close()
    return anon


# valid values for inserting into the test table
usernames = ['alice', 'bob', 'charlie']
nums = list(range(5))

# create the insertion procs
procs = []
for i in range(num_transactions):
    username = usernames[i%len(usernames)]
    num = nums[i%len(nums)]
    sql = f"INSERT INTO test (username,num) VALUES ('{username}',{num});"
    proc = Process(target=get_process_function(sql))
    proc.start()
    procs.append(proc)

# create the rollup proc
sql = "select do_rollup('test_view');"
proc = Process(target=get_process_function(sql, delay_seconds=0))
proc.start()
procs.append(proc)

# wait for all procs to finish
for proc in procs:
    proc.join()

# perform one last do_rollup to ensure the rollup table is up-to-date
sql = "select do_rollup('test_view');"
curs = conn.cursor()
curs.execute(sql)

################################################################################
# verify correctness
################################################################################

curs = conn.cursor()
sql = 'select "test.username","sum" from test_view order by "test.username";'
curs.execute(sql)
results = curs.fetchall()
print("results=",results)

curs = conn.cursor()
sql = 'select "test.username","sum" from test_view_groundtruth order by "test.username";'
curs.execute(sql)
results_groundtruth = curs.fetchall()
print("results_groundtruth=",results_groundtruth)

assert results==results_groundtruth

