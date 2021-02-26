create or replace language plpython3u;
create extension if not exists hll;
create extension if not exists pg_rollup;
create extension if not exists pg_cron;

create temporary table test_cron (
    id serial primary key,
    name text,
    num int
);

insert into test_cron (name,num) values
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
    'test_cron',
    'test_cron_rollup1',
    wheres => 'name',
    key => 'id'
);

select create_rollup(
    'test_cron',
    'test_cron_rollup2',
    wheres => 'name,num',
    key => 'id'
);

select create_rollup(
    'test_cron',
    'test_cron_rollup3',
    wheres => 'name',
    rollups => 'hll(num)',
    key => 'id',
    mode => 'trigger'
);

select create_rollup(
    'test_cron',
    'test_cron_rollup4',
    rollups => 'hll(name),hll(num)',
    key => 'id',
    mode => 'cron'
);

insert into test_cron (name,num) values
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

-- cron jobs do not run in the environment created by the make installcheck command;
-- therefore, the rollup commands will not get executed and the tables will be out of date;
-- to test_cron the cron mode, therefore, we will inspect the output of the cron job list
-- and verify that all jobs that should be added have been added
select * from cron.job;

UPDATE pg_rollup_settings SET value='trigger' WHERE name='default_mode';
