create or replace language plpython3u;
create extension if not exists pg_rollup;

create temporary table testfloat (
    id serial primary key,
    num bigint
);

insert into testfloat (num) (select * from generate_series(1,10000));

select create_rollup(
    'testfloat',
    'testfloat_rollup1',
    rollups => $$
        count(num),
        avg(num),
        var_pop(num),
        var_samp(num)
    $$
);


CREATE OR REPLACE FUNCTION relative_error(a double precision, b double precision) RETURNS DOUBLE PRECISION AS $$
    select greatest(abs(a),abs(b))/least(abs(a),abs(b))-1;
$$ LANGUAGE 'sql' STRICT IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION check_testfloat_rollup1() RETURNS BOOLEAN AS $$
select
    relative_error(testfloat_rollup1."count(num)"   ,testfloat_rollup1_groundtruth."count(num)"      ) < 1e-12
AND relative_error(testfloat_rollup1."avg(num)"     ,testfloat_rollup1_groundtruth."avg(num)"        ) < 1e-12
AND relative_error(testfloat_rollup1."var_pop(num)" ,testfloat_rollup1_groundtruth."var_pop(num)"    ) < 1e-12
AND relative_error(testfloat_rollup1."var_samp(num)",testfloat_rollup1_groundtruth."var_samp(num)"   ) < 1e-12
from testfloat_rollup1,testfloat_rollup1_groundtruth;
$$ LANGUAGE 'sql' STRICT IMMUTABLE PARALLEL SAFE;

--insert into testfloat (num) values (null);

select check_testfloat_rollup1();

select check_testfloat_rollup1();

insert into testfloat (num) (select * from generate_series(1,10000));

select check_testfloat_rollup1();

insert into testfloat (num) (select * from generate_series(1,5000));

select check_testfloat_rollup1();

insert into testfloat (num) (select * from generate_series(1e10,1e10+5000));

select check_testfloat_rollup1();

insert into testfloat (num) (select * from generate_series(1,5000));

select check_testfloat_rollup1();

insert into testfloat (num) (select * from generate_series(1,1e5));

select check_testfloat_rollup1();

insert into testfloat (num) (select * from generate_series(1,5));

select check_testfloat_rollup1();

do $$
begin
   for counter in 1..5000 loop
        insert into testfloat (num) (select * from generate_series(1,1));
   end loop;
end;
$$ language plpgsql;

select check_testfloat_rollup1();

--insert into testfloat (num) values (null);

select check_testfloat_rollup1();

select 'testfloat_rollup1',* from testfloat_rollup1 union select 'testfloat_rollup1_groundtruth',* from testfloat_rollup1_groundtruth;

