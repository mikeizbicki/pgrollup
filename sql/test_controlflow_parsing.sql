SET client_min_messages TO WARNING;
create or replace language plpython3u;
create extension if not exists pg_rollup;

create temporary table testparsing (
    id serial primary key,
    name text,
    num int
);

insert into testparsing (name,num) values
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

select create_rollup($$
CREATE INCREMENTAL MATERIALIZED VIEW testparsing_rollup1 AS (
    SELECT
        count(*) AS count
    FROM testparsing
    GROUP BY name
);
$$);

select create_rollup($$
CREATE INCREMENTAL MATERIALIZED VIEW testparsing_rollup2 AS (
    select count(*) as count
    from testparsing
    group by name,num
);
$$);

select create_rollup($$
CREATE INCREMENTAL MATERIALIZED VIEW testparsing_rollup3 AS (
    select
        sum(num) as sum,
        count(*) as count_all,
        count(num),
        max(num),
        min(num)
    from testparsing
    group by name
);
$$);

select create_rollup($$
CREATE INCREMENTAL MATERIALIZED VIEW testparsing_rollup4 AS (
    select
        sum(num) as sum,
        count(*) as count_all,
        count(num),
        max(num),
        min(num)
    from testparsing
);
$$);

select assert_rollup('testparsing_rollup1');
select assert_rollup('testparsing_rollup2');
select assert_rollup('testparsing_rollup3');
select assert_rollup('testparsing_rollup4');

insert into testparsing (name,num) values
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

select assert_rollup('testparsing_rollup1');
select assert_rollup('testparsing_rollup2');
select assert_rollup('testparsing_rollup3');
select assert_rollup('testparsing_rollup4');
