create or replace language plpython3u;
create extension if not exists datasketches;
drop extension pg_rollup;
create extension if not exists pg_rollup;

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

-- FIXME:
-- there are no correctness tests for frequent_strings_sketch,
-- we're only testing that there's no errors
select create_rollup(
    'dstest',
    'dstest_rollup2',
    rollups => $$
        frequent_strings_sketch(text(a))
    $$
);

SELECT assert_rollup_relative_error('dstest_rollup1', 0.2);

insert into dstest (a) (select * from generate_series(0,10000));

SELECT assert_rollup_relative_error('dstest_rollup1', 0.2);

insert into dstest (a) (select * from generate_series(0,10000));

SELECT assert_rollup_relative_error('dstest_rollup1', 0.2);
