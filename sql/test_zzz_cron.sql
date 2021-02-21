create or replace language plpython3u;
create extension if not exists hll;
create extension if not exists pg_rollup;
create extension if not exists pg_cron;

create table test (
    id serial primary key,
    name text,
    num int
);

insert into test (name,num) values
    ('alice', 1),
    ('alice', 2),
    ('alice', 3),
    ('alice', 4),
    ('alice', 5),
    ('bill', 5),
    ('bill', 5),
    ('bill', 5),
    ('charlie', 1),
    ('charlie', 1),
    ('charlie', 1),
    ('charlie', 3),
    ('charlie', NULL),
    ('dave', 4),
    ('elliot', 5),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 8),
    (NULL, 9),
    (NULL, NULL),
    (NULL, NULL),
    (NULL, NULL),
    (NULL, NULL);

UPDATE pg_rollup_settings SET value='cron' WHERE name='default_mode';

select create_rollup(
    'test',
    'test_rollup1',
    wheres => 'name',
    key => 'id'
);

select create_rollup(
    'test',
    'test_rollup2',
    wheres => 'name,num',
    key => 'id'
);

select create_rollup(
    'test',
    'test_rollup3',
    wheres => 'name',
    distincts => 'num',
    key => 'id',
    mode => 'trigger'
);

select create_rollup(
    'test',
    'test_rollup4',
    distincts => 'name,num',
    key => 'id',
    mode => 'cron'
);

insert into test (name,num) values
    ('alice', 1),
    ('alice', 2),
    ('alice', 3),
    ('alice', 4),
    ('alice', 5),
    ('bill', 5),
    ('bill', 5),
    ('bill', 5),
    ('charlie', 1),
    ('charlie', 1),
    ('charlie', 1),
    ('charlie', 3),
    ('charlie', NULL),
    ('dave', 4),
    ('elliot', 5),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 8),
    (NULL, 9),
    (NULL, NULL),
    (NULL, NULL),
    (NULL, NULL),
    (NULL, NULL);

-- sleep for sufficient time for the cron jobs to run;
-- wait just over 3 minutes to ensure that they run
select pg_sleep(200);

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup2');
select assert_rollup('test_rollup3');
select assert_rollup('test_rollup4');
