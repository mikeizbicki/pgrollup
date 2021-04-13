SET client_min_messages TO WARNING;
create or replace language plpython3u;
create extension if not exists pg_rollup;

create table testtetris (
    id serial primary key,
    num int,
    val boolean
);

CREATE MATERIALIZED VIEW testtetris_rollup1 AS (
    SELECT
        sum(num),
        bool_and(val)
    FROM testtetris
);

select pgrollup_manage_all();

select * from testtetris_rollup1_raw;

-- FIXME: add more column types

drop table testtetris cascade;
