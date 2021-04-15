SET client_min_messages TO WARNING;
create or replace language plpython3u;
create extension if not exists pg_rollup;

create table testbool (
    id serial primary key,
    a bool,
    b bool,
    c bool,
    d bool
);

create materialized view testbool_rollup1 as (
    select 
        bool_and(a) as and_a,
        bool_and(b) as and_b,
        bool_and(c) as and_c,
        bool_and(d) as and_d,
        bool_or(a) as or_a,
        bool_or(b) as or_b,
        bool_or(c) as or_c,
        bool_or(d) as or_d
    from testbool
);

insert into testbool (a,b,c,d) values (TRUE,FALSE,NULL,NULL);
select assert_rollup('testbool_rollup1');
insert into testbool (a,b,c,d) values (TRUE,FALSE,NULL,NULL);
select assert_rollup('testbool_rollup1');
insert into testbool (a,b,c,d) values (FALSE,TRUE,TRUE,FALSE);
select assert_rollup('testbool_rollup1');
insert into testbool (a,b,c,d) values (NULL,NULL,FALSE,TRUE);
select assert_rollup('testbool_rollup1');
insert into testbool (a,b,c,d) values (TRUE,FALSE,NULL,NULL);
select assert_rollup('testbool_rollup1');

drop table testbool cascade;
