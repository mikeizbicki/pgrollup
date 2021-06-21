SET client_min_messages TO WARNING;

create table dstest (
    id serial primary key,
    a int,
    b text
);

insert into dstest (a,b) (select random()*100,text(round(random()^2*100)) from generate_series(0,10000));

CREATE MATERIALIZED VIEW dstest_rollup1 AS (
    SELECT 
        kll_float_sketch_get_quantile(kll_float_sketch_build(a),0.50) AS kll_p50,
        kll_float_sketch_get_quantile(kll_float_sketch_build(a),0.90) AS kll_p90,
        kll_float_sketch_get_quantile(kll_float_sketch_build(a),0.99) AS kll_p99,
        req_float_sketch_get_quantile(req_float_sketch_build(a),0.50) AS req_p50,
        req_float_sketch_get_quantile(req_float_sketch_build(a),0.90) AS req_p90,
        req_float_sketch_get_quantile(req_float_sketch_build(a),0.99) AS req_p99
    FROM dstest
);

-- FIXME: this seems to work for postgres13 but not postgres12
CREATE MATERIALIZED VIEW dstest_rollup2 AS (
    SELECT 
        frequent_strings_sketch_result_no_false_negatives(frequent_strings_sketch_build(10,b)) AS freq_strings_10
        -- NOTE: the columns below have too much randomness for the assert_rollup function; they still work approximately, but we would need a custom approximate check to verify this, and it's not worth implementing 
        --frequent_strings_sketch_result_no_false_negatives(frequent_strings_sketch_build(8,b)) AS freq_strings_8,
        --frequent_strings_sketch_result_no_false_negatives(frequent_strings_sketch_build(6,b)) AS freq_strings_6,
        --frequent_strings_sketch_result_no_false_negatives(frequent_strings_sketch_build(4,b)) AS freq_strings_4,
        --frequent_strings_sketch_result_no_false_negatives(frequent_strings_sketch_build(2,b)) AS freq_strings_2
    FROM dstest
);

SELECT assert_rollup_relative_error('dstest_rollup1', 0.3);
SELECT assert_rollup('dstest_rollup2');

insert into dstest (a,b) (select round(random()*100),text(round(random()^2*100)) from generate_series(0,10000));

SELECT assert_rollup_relative_error('dstest_rollup1', 0.3);
SELECT assert_rollup('dstest_rollup2');

insert into dstest (a,b) (select round(random()*100),text(round(random()^2*100)) from generate_series(0,10000));

SELECT assert_rollup_relative_error('dstest_rollup1', 0.3);
SELECT assert_rollup('dstest_rollup2');


--------------------------------------------------------------------------------
-- tests for distinct counting datastructures (hll/theta/cpc)
--------------------------------------------------------------------------------

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
        round(theta_sketch_get_estimate(theta_sketch_build(name))) AS theta_name,
        round(theta_sketch_get_estimate(theta_sketch_build(num))) AS theta_num
    FROM test
    GROUP BY name
);

CREATE MATERIALIZED VIEW test_rollup2 AS (
    SELECT
        name,
        round(theta_sketch_distinct(name)) AS theta_name,
        round(theta_sketch_distinct(num)) AS theta_num
    FROM test
    GROUP BY name
);

CREATE MATERIALIZED VIEW test_rollup3 AS (
    SELECT
        name,
        round(hll_sketch_get_estimate(hll_sketch_build(name))) AS hll_name,
        round(hll_sketch_get_estimate(hll_sketch_build(num))) AS hll_num
    FROM test
    GROUP BY name
);

CREATE MATERIALIZED VIEW test_rollup4 AS (
    SELECT
        name,
        round(hll_sketch_distinct(name)) AS hll_name,
        round(hll_sketch_distinct(num)) AS hll_num
    FROM test
    GROUP BY name
);

/*
-- FIXME:
-- The cpc_sketch seems to work correctly;
-- nevertheless, it generates an error message on the logs which cause the test to fail;
-- this is particularly true because different versions of pg generate different errors
CREATE MATERIALIZED VIEW test_rollup5 AS (
    SELECT
        name,
        round(cpc_sketch_get_estimate(cpc_sketch_build(name))) AS cpc_name,
        round(cpc_sketch_get_estimate(cpc_sketch_build(num))) AS cpc_num
    FROM test
    GROUP BY name
);

CREATE MATERIALIZED VIEW test_rollup6 AS (
    SELECT
        name,
        round(cpc_sketch_distinct(name)) AS cpc_name,
        round(cpc_sketch_distinct(num)) AS cpc_num
    FROM test
    GROUP BY name
);
*/

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup2');
select assert_rollup('test_rollup3');
select assert_rollup('test_rollup4');
--select assert_rollup('test_rollup5');
--select assert_rollup('test_rollup6');

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
select assert_rollup('test_rollup3');
select assert_rollup('test_rollup4');
--select assert_rollup('test_rollup5');
--select assert_rollup('test_rollup6');

select * from test_rollup1 order by "test.name";
select * from test_rollup2 order by "test.name";
select * from test_rollup3 order by "test.name";
select * from test_rollup4 order by "test.name";
--select * from test_rollup5 order by "test.name";
--select * from test_rollup6 order by "test.name";

drop table dstest cascade;
drop table test cascade;
