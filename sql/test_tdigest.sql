create or replace language plpython3u;
drop extension pg_rollup;
create extension if not exists tdigest;
create extension if not exists pg_rollup;

create table tdigest_test (
    id serial primary key,
    a int
);

insert into tdigest_test (a) (select * from generate_series(0,10000));

select create_rollup(
    'tdigest_test',
    'tdigest_test_rollup1',
    rollups => $$
        tdigest(a)
    $$
);

select assert_rollup('tdigest_test_rollup1');

insert into tdigest_test (a) (select * from generate_series(0,10000));

SELECT assert_rollup_relative_error('tdigest_test_rollup1', 0.1);

insert into tdigest_test (a) (select * from generate_series(100000,200000));

SELECT assert_rollup_relative_error('tdigest_test_rollup1', 0.1);
