SET client_min_messages TO WARNING;
create or replace language plpython3u;
create extension if not exists pg_rollup;

create table testfloat (
    id serial primary key,
    num bigint
);

create materialized view testfloat_rollup1 as (
    select 
        count(num),
        avg(num),
        var_pop(num),
        var_samp(num),
        variance(num),
        stddev(num),
        stddev_pop(num),
        stddev_samp(num)
    from testfloat
);


insert into testfloat (num) values (null);
select * from testfloat_rollup1;
select * from testfloat_rollup1_groundtruth;

insert into testfloat (num) values (1);
select * from testfloat_rollup1;
select * from testfloat_rollup1_groundtruth;

insert into testfloat (num) values (2);
select * from testfloat_rollup1;
select * from testfloat_rollup1_groundtruth;

insert into testfloat (num) values (2);
select * from testfloat_rollup1;
select * from testfloat_rollup1_groundtruth;

insert into testfloat (num) values (2);
select * from testfloat_rollup1;
select * from testfloat_rollup1_groundtruth;

select assert_rollup_relative_error('testfloat_rollup1', 1e-12);

insert into testfloat (num) (select * from generate_series(1,10000));

select assert_rollup_relative_error('testfloat_rollup1', 1e-12);

insert into testfloat (num) (select * from generate_series(1,5000));

select assert_rollup_relative_error('testfloat_rollup1', 1e-12);

insert into testfloat (num) (select * from generate_series(1e10,1e10+5000));

select assert_rollup_relative_error('testfloat_rollup1', 1e-12);

insert into testfloat (num) (select * from generate_series(1,5000));

select assert_rollup_relative_error('testfloat_rollup1', 1e-12);

insert into testfloat (num) (select * from generate_series(1,1e5));

select assert_rollup_relative_error('testfloat_rollup1', 1e-12);

insert into testfloat (num) (select * from generate_series(1,5));

select assert_rollup_relative_error('testfloat_rollup1', 1e-12);

do $$
begin
   for counter in 1..5000 loop
        insert into testfloat (num) (select * from generate_series(1,1));
   end loop;
end;
$$ language plpgsql;

select assert_rollup_relative_error('testfloat_rollup1', 1e-12);

insert into testfloat (num) values (null);

select assert_rollup_relative_error('testfloat_rollup1', 1e-12);

--select 'testfloat_rollup1',* from testfloat_rollup1 union select 'testfloat_rollup1_groundtruth',* from testfloat_rollup1_groundtruth;

drop table testfloat cascade;
