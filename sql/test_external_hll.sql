SET client_min_messages TO WARNING;
create or replace language plpython3u;
create extension if not exists hll;
drop extension pgrollup;
create extension if not exists pgrollup;

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

CREATE MATERIALIZED VIEW test_rollup1 AS (
    SELECT
        name,
        round(hll_cardinality(hll_add_agg(hll_hash_text(name)))) AS distinct_name,
        round(hll_cardinality(hll_add_agg(hll_hash_integer(num)))) AS distinct_num
    FROM test
    GROUP BY name
);

CREATE MATERIALIZED VIEW test_rollup2 AS (
    SELECT
        name,
        hll_count(name) AS distinct_name,
        hll_count(num)  AS distinct_num
    FROM test
    GROUP BY name
);

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup2');

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


select assert_rollup('test_rollup1');
select assert_rollup('test_rollup2');

select * from test_rollup1;
select * from test_rollup2;

drop table test cascade;
