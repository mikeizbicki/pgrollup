create or replace language plpython3u;
create extension if not exists hll;
create extension if not exists pg_rollup;
create extension if not exists datasketches;

CREATE OR REPLACE FUNCTION kll_float_sketch_union(a kll_float_sketch, b kll_float_sketch) RETURNS kll_float_sketch AS $$
    select kll_float_sketch_merge(sketch) from (select a as sketch union all select b) t;
$$ LANGUAGE 'sql' IMMUTABLE PARALLEL SAFE;

create table dstest (
    id serial primary key,
    a int
);

insert into dstest (a) (select * from generate_series(0,10000));

select create_rollup(
    'dstest',
    'dstest_rollup1',
    rollups => $$
        kll_float_sketch(a)
    $$
);

select create_rollup(
    'dstest',
    'dstest_rollup2',
    rollups => $$
        theta_sketch(a)
    $$
);

-- FIXME:
-- kll_float_sketch is non-deterministic due to an internal random number generation;
-- this causes the groundtruth and the rollup table to diverge slightly,
-- and so we can't test for equality

--select assert_rollup('dstest_rollup1');
select assert_rollup('dstest_rollup2');

insert into dstest (a) (select * from generate_series(0,10000));

--select assert_rollup('dstest_rollup1');
select assert_rollup('dstest_rollup2');

