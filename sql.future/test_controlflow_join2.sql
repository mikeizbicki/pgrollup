--SET client_min_messages TO WARNING;
create or replace language plpython3u;
create extension if not exists pg_rollup;

-- FIXME:
-- I believe there may be a bug in the manual mode when the joined tables are rolled up in separate transactions.

SELECT setseed(0);

CREATE OR REPLACE FUNCTION randint(n int) RETURNS INT AS $$
    SELECT CASE 
        WHEN random()<0.5 THEN NULL
        ELSE floor(random()*n)::int
        END;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION mkvalues(n int) RETURNS SETOF RECORD AS $$
    SELECT a,b,c FROM (SELECT generate_series(1,n), randint(100) as a, randint(20) as b, randint(50) as c) t;
$$ LANGUAGE SQL;

create temporary table testjoin2_1 (
    pk serial primary key,
    num1 int,
    num2 int,
    num3 int
);

create temporary table testjoin2_2 (
    pk serial primary key,
    num1 int,
    num2 int,
    num3 int
);

create temporary table testjoin2_3 (
    pk serial primary key,
    num1 int,
    num2 int,
    num3 int
);

create temporary table testjoin2_4 (
    pk serial primary key,
    num1 int,
    num2 int,
    num3 int
);

create temporary table testjoin2_5 (
    pk serial primary key,
    num1 int,
    num2 int,
    num3 int
);

insert into testjoin2_1 (num1,num2,num3) (select * from mkvalues(100) as f(num1 int, num2 int, num3 int));
insert into testjoin2_2 (num1,num2,num3) (select * from mkvalues(100) as f(num1 int, num2 int, num3 int));
insert into testjoin2_3 (num1,num2,num3) (select * from mkvalues(100) as f(num1 int, num2 int, num3 int));
insert into testjoin2_4 (num1,num2,num3) (select * from mkvalues(100) as f(num1 int, num2 int, num3 int));
insert into testjoin2_5 (num1,num2,num3) (select * from mkvalues(100) as f(num1 int, num2 int, num3 int));


select pgrollup($$
CREATE INCREMENTAL MATERIALIZED VIEW testjoin2__rollup1 AS (
    SELECT
        sum(t1.num1),
        sum(t2.num1),
        sum(num1)
    FROM testjoin2_1 AS t1
    RIGHT JOIN testjoin2_2 AS t2 USING (num1)
);
$$, dry_run => False);

/*
select pgrollup($$
CREATE INCREMENTAL MATERIALIZED VIEW testjoin2__rollup2 AS (
    SELECT
        sum(t1.num1),
        sum(t2.num1),
        sum(t3.num1),
        sum(t4.num1)
    FROM testjoin2_1 AS t1
    JOIN testjoin2_2 AS t2 USING (num1)
    JOIN testjoin2_3 AS t3 USING (num1)
    RIGHT JOIN testjoin2_4 AS t4 USING (num1)
);
$$, dry_run => False);

select pgrollup($$
CREATE INCREMENTAL MATERIALIZED VIEW testjoin2__rollup3 AS (
    SELECT
        sum(t1.num1),
        sum(t2.num1),
        sum(t3.num1),
        sum(t4.num1)
    FROM testjoin2_1 AS t1
    JOIN testjoin2_2 AS t2 USING (num1)
    RIGHT JOIN testjoin2_3 AS t3 USING (num1)
    RIGHT JOIN testjoin2_4 AS t4 USING (num1)
);
$$, dry_run => False);

select pgrollup($$
CREATE INCREMENTAL MATERIALIZED VIEW testjoin2__rollup4 AS (
    SELECT
        sum(t1.num1),
        sum(t2.num1),
        sum(t3.num1),
        sum(t4.num1)
    FROM testjoin2_1 AS t1
    RIGHT JOIN testjoin2_2 AS t2 USING (num1)
    RIGHT JOIN testjoin2_3 AS t3 USING (num1)
    RIGHT JOIN testjoin2_4 AS t4 USING (num1)
);
$$, dry_run => False);
*/

/*
select pgrollup($$
CREATE INCREMENTAL MATERIALIZED VIEW testjoin2__rollup3 AS (
    SELECT
        sum(t1.num2),
        sum(t2.num2)
    FROM testjoin2_1 AS t1
    FULL JOIN testjoin2_2 AS t2 USING (num1)
    GROUP BY num1
);
$$, dry_run => False);

select pgrollup($$
CREATE INCREMENTAL MATERIALIZED VIEW testjoin2__rollup4 AS (
    SELECT
        sum(t1.num2),
        sum(t2.num2),
        sum(t3.num2),
        sum(t4.num2),
        sum(t5.num2)
    FROM testjoin2_1 AS t1
    FULL JOIN testjoin2_2 AS t2 USING (num1)
    RIGHT JOIN testjoin2_3 AS t3 USING (num1)
    LEFT JOIN testjoin2_4 AS t4 USING (num1)
    FULL JOIN testjoin2_5 AS t5 USING (num1)
);
$$, dry_run => False);

select pgrollup($$
CREATE INCREMENTAL MATERIALIZED VIEW testjoin2__rollup5 AS (
    SELECT
        sum(t1.num2),
        sum(t2.num2),
        sum(t3.num2),
        sum(t4.num2),
        sum(t5.num2)
    FROM testjoin2_1 AS t1
    LEFT JOIN testjoin2_2 AS t2 USING (num1)
    JOIN testjoin2_3 AS t3 USING (num1)
    RIGHT JOIN testjoin2_4 AS t4 USING (num1)
    JOIN testjoin2_5 AS t5 USING (num1)
    GROUP BY t4.num3,t5.num3
    HAVING t4.num3=t5.num3
);
$$, dry_run => False);

select pgrollup($$
CREATE INCREMENTAL MATERIALIZED VIEW testjoin2__rollup6 AS (
    SELECT
        sum(t1.num2),
        sum(t2.num2),
        sum(t3.num2),
        sum(t4.num2),
        sum(t5.num2),
        sum(t1.num3 + t2.num3 + t3.num3 - t4.num3 + t5.num3),
        sum(t1.num3 - t2.num3 + t3.num3),
        sum(t1.num3 + t3.num3 - t5.num3)
    FROM testjoin2_1 AS t1
    FULL OUTER JOIN testjoin2_2 AS t2 USING (num1)
    FULL OUTER JOIN testjoin2_3 AS t3 ON (t3.num2=t1.pk)
    FULL OUTER JOIN testjoin2_4 AS t4 ON (t4.num1=t2.num2)
    FULL OUTER JOIN testjoin2_5 AS t5 ON (t5.num2=t4.num1)
    GROUP BY t4.num2
    HAVING t4.num2 < 50
);
$$, dry_run => False);
*/

do $$
BEGIN
FOR counter IN 1..1 LOOP
    --insert into testjoin2_1 (num1,num2,num3) (select * from mkvalues(20) as f(num1 int, num2 int, num3 int));
    --insert into testjoin2_2 (num1,num2,num3) (select * from mkvalues(30) as f(num1 int, num2 int, num3 int));
    --insert into testjoin2_3 (num1,num2,num3) (select * from mkvalues(50) as f(num1 int, num2 int, num3 int));
    --insert into testjoin2_4 (num1,num2,num3) (select * from mkvalues(50) as f(num1 int, num2 int, num3 int));
    --insert into testjoin2_5 (num1,num2,num3) (select * from mkvalues(50) as f(num1 int, num2 int, num3 int));
END LOOP;
END;
$$ LANGUAGE plpgsql;

select * from testjoin2__rollup1_groundtruth;

CREATE table tmp AS (select * from mkvalues(20) as f(num1 int, num2 int, num3 int));

    SELECT
        sum(t1.num1),
        sum(t2.num1),
        sum(num1)
    FROM testjoin2_1 AS t1
    RIGHT JOIN testjoin2_2 AS t2 USING (num1);

    SELECT
        sum(t1.num1),
        sum(t2.num1),
        sum(num1)
    FROM (SELECT * FROM testjoin2_1 UNION ALL SELECT 1,* FROM tmp) AS t1
    RIGHT JOIN testjoin2_2 AS t2 USING (num1);

    SELECT
        0, 
        sum(t2.num1),
        sum(num1)
    FROM testjoin2_2 AS t2;

    SELECT
        sum(t1.num1),
        sum(t2.num1),
        sum(num1)
    FROM tmp AS t1
    RIGHT JOIN testjoin2_2 AS t2 USING (num1);

select distinct num1 from tmp;
select distinct num1 from tmp where num1 not in (select num1 from testjoin2_1);
select t2.num1 FROM testjoin2_2 as t2 WHERE t2.num1 in (select distinct num1 from tmp);
select t2.num1 FROM testjoin2_2 as t2 WHERE t2.num1 in (select distinct num1 from tmp) and t2.num1 not in (select num1 from testjoin2_1);
    --RIGHT JOIN testjoin2_2 AS t2 USING (num1)

insert into testjoin2_1 (num1,num2,num3) (select * from tmp);
select * from testjoin2__rollup1_groundtruth;

select distinct on (rollup_name) assert_rollup(rollup_name) from pgrollup_rollups where rollup_name like 'testjoin2_%';

select * from testjoin2__rollup1;

select * from testjoin2__rollup1_groundtruth;

    SELECT
        sum(num1)
    FROM testjoin2_1 AS t1
    RIGHT OUTER JOIN testjoin2_2 AS t2 USING (num1);


    SELECT
        sum(num1)
    FROM testjoin2_1 AS t1
    JOIN testjoin2_2 AS t2 USING (num1);
