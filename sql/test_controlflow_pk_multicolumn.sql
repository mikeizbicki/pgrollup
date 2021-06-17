SET client_min_messages TO WARNING;
create or replace language plpython3u;
create extension if not exists pgrollup;

create table test_multipk (
    id serial,
    name text,
    num int,
    primary key(name, id)
);

insert into test_multipk (name,num) values
    ('alice', 1),
    ('alice', 2),
    ('alice', 3),
    ('alice', 4),
    ('alice', 5),
    ('bill', 5),
    ('bill', 5),
    ('bill', 5),
    ('charlie', 1),
    ('charlie', 1),
    ('charlie', 1),
    ('charlie', 3),
    ('charlie', NULL),
    ('dave', 4),
    ('elliot', 5);

create materialized view test_multipk_rollup1 as (
    select name,count(*)
    from test_multipk
    group by name
);

select * from pgrollup_rollups;

select assert_rollup('test_multipk_rollup1');

insert into test_multipk (name,num) values
    ('alice', 1),
    ('alice', 2),
    ('alice', 3),
    ('alice', 4),
    ('alice', 5),
    ('bill', 5),
    ('bill', 5),
    ('bill', 5),
    ('charlie', 1),
    ('charlie', 1),
    ('charlie', 1),
    ('charlie', 3),
    ('charlie', NULL),
    ('dave', 4),
    ('elliot', 5);

select assert_rollup('test_multipk_rollup1');

select rollup_mode('test_multipk_rollup1','manual');

insert into test_multipk (name,num) values
    ('alice', 1),
    ('alice', 2),
    ('alice', 3),
    ('alice', 4),
    ('alice', 5),
    ('bill', 5),
    ('bill', 5),
    ('bill', 5),
    ('charlie', 1),
    ('charlie', 1),
    ('charlie', 1),
    ('charlie', 3),
    ('charlie', NULL),
    ('dave', 4),
    ('elliot', 5);

select * from pgrollup_rollups;
select do_rollup('test_multipk_rollup1');
select * from pgrollup_rollups;

select assert_rollup('test_multipk_rollup1');

drop table test_multipk cascade;


