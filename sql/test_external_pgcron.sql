SET client_min_messages TO WARNING;
create or replace language plpython3u;
create extension if not exists pgrollup;
create extension if not exists pg_cron;

create table test_cron (
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

UPDATE pgrollup_settings SET value='cron' WHERE name='default_mode';

CREATE MATERIALIZED VIEW test_cron_rollup1 AS (
    SELECT 
        name,
        count(*)
    FROM test_cron
    GROUP BY name
);

CREATE MATERIALIZED VIEW test_cron_rollup2 AS (
    SELECT 
        name,
        num,
        count(*)
    FROM test_cron
    GROUP BY name,num
);

CREATE MATERIALIZED VIEW test_cron_rollup3 AS (
    SELECT 
        name,
        sum(num)
    FROM test_cron
    GROUP BY name
);
SELECT rollup_mode('test_cron_rollup3','trigger');

CREATE MATERIALIZED VIEW test_cron_rollup4 AS (
    SELECT 
        name,
        sum(num)
    FROM test_cron
    GROUP BY name
);
SELECT rollup_mode('test_cron_rollup4','cron');

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
SELECT * FROM cron.job;

UPDATE pgrollup_settings SET value='trigger' WHERE name='default_mode';

DROP TABLE test_cron CASCADE;
