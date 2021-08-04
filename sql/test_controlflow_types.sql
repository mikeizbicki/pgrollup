SET client_min_messages TO WARNING;

create table test1 (
    id serial primary key,
    num1 int,
    num2 smallint,
    num3 bigint,
    num4 double precision,
    num5 real,
    num6 numeric
);

create materialized view test1_rollup1 as (
    select 
        count(*),
        sum(num1)                       AS "sum(num1)",
        sum(num1) :: smallint           AS "sum(num1) :: smallint",
        sum(num1) :: integer            AS "sum(num1) :: integer",
        sum(num1) :: bigint             AS "sum(num1) :: bigint",
        sum(num1) :: double precision   AS "sum(num1) :: double precision",
        sum(num1) :: real               AS "sum(num1) :: real",
        sum(num1) :: numeric            AS "sum(num1) :: numeric",
        sum(num2) AS "sum(num2)",
        sum(num3) AS "sum(num3)",
        sum(num4) AS "sum(num4)",
        sum(num5) AS "sum(num5)",
        sum(num6) AS "sum(num6)",
        sum(num6) :: smallint           AS "sum(num6) :: smallint",
        sum(num6) :: integer            AS "sum(num6) :: integer",
        sum(num6) :: bigint             AS "sum(num6) :: bigint",
        sum(num6) :: double precision   AS "sum(num6) :: double precision",
        sum(num6) :: real               AS "sum(num6) :: real",
        sum(num6) :: numeric            AS "sum(num6) :: numeric"
    from test1
);

select column_name, data_type from information_schema.columns where table_name = 'test1_rollup1';

drop table test1 cascade;
