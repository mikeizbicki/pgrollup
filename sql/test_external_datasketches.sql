SET client_min_messages TO WARNING;
create or replace language plpython3u;
create extension if not exists datasketches;
drop extension pgrollup;
create extension if not exists pgrollup;

create table dstest (
    id serial primary key,
    a int,
    b text
);

insert into dstest (a,b) (select random()*100,text(round(random()^2*100)) from generate_series(0,10000));

CREATE MATERIALIZED VIEW dstest_rollup1 AS (
    SELECT 
        kll_float_sketch_get_quantile(kll_float_sketch_build(a),0.50) AS p50,
        kll_float_sketch_get_quantile(kll_float_sketch_build(a),0.90) AS p90,
        kll_float_sketch_get_quantile(kll_float_sketch_build(a),0.99) AS p99
    FROM dstest
);

CREATE MATERIALIZED VIEW dstest_rollup2 AS (
    SELECT 
        frequent_strings_sketch_result_no_false_negatives(frequent_strings_sketch_build(10,b)) AS freq_strings_10
        -- NOTE: the columns below have too much randomness for the ass_rollup function; they still work approximately, but we would need a custom approximate check to verify this, and it's not worth implementing 
        --frequent_strings_sketch_result_no_false_negatives(frequent_strings_sketch_build(8,b)) AS freq_strings_8,
        --frequent_strings_sketch_result_no_false_negatives(frequent_strings_sketch_build(6,b)) AS freq_strings_6,
        --frequent_strings_sketch_result_no_false_negatives(frequent_strings_sketch_build(4,b)) AS freq_strings_4,
        --frequent_strings_sketch_result_no_false_negatives(frequent_strings_sketch_build(2,b)) AS freq_strings_2
    FROM dstest
);

SELECT assert_rollup_relative_error('dstest_rollup1', 0.2);
SELECT assert_rollup('dstest_rollup2');

insert into dstest (a,b) (select round(random()*100),text(round(random()^2*100)) from generate_series(0,10000));

SELECT assert_rollup_relative_error('dstest_rollup1', 0.2);
SELECT assert_rollup('dstest_rollup2');

insert into dstest (a,b) (select round(random()*100),text(round(random()^2*100)) from generate_series(0,10000));

SELECT assert_rollup_relative_error('dstest_rollup1', 0.2);
SELECT assert_rollup('dstest_rollup2');

drop table dstest cascade;
