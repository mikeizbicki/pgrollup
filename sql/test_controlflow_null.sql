SET client_min_messages TO WARNING;

create table test1 (
    id serial primary key,
    name text,
    num int not null
);

create table test2 (
    id serial primary key,
    name text not null,
    num int
);

create table test3 (
    id serial primary key,
    name text not null,
    num int not null
);

insert into test1 (name,num) values
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
    ('dave', 4),
    ('elliot', 5);

insert into test2 (name,num) values
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
    ('dave', 4),
    ('elliot', 5);

insert into test3 (name,num) values
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
    ('dave', 4),
    ('elliot', 5);

create materialized view test1_rollup1 as (
    select count(*)
    from test1
);

create materialized view test1_rollup2 as (
    select name,count(*)
    from test1
    group by name
);

create materialized view test1_rollup3 as (
    select num,count(*)
    from test1
    group by num
);

create materialized view test1_rollup4 as (
    select name,num,count(*)
    from test1
    group by name,num
);

create materialized view test2_rollup1 as (
    select count(*)
    from test2
);

create materialized view test2_rollup2 as (
    select name,count(*)
    from test2
    group by name
);

create materialized view test2_rollup3 as (
    select num,count(*)
    from test2
    group by num
);

create materialized view test2_rollup4 as (
    select name,num,count(*)
    from test2
    group by name,num
);

create materialized view test3_rollup1 as (
    select count(*)
    from test3
);

create materialized view test3_rollup2 as (
    select name,count(*)
    from test3
    group by name
);

create materialized view test3_rollup3 as (
    select num,count(*)
    from test3
    group by num
);

create materialized view test3_rollup4 as (
    select name,num,count(*)
    from test3
    group by name,num
);

-- the following psql command prints table information;
-- the important thing is that it will display whether a column is nullable or not,
-- and this should be set automatically for us by pgrollup
\d test1_rollup1_raw
\d test1_rollup2_raw
\d test1_rollup3_raw
\d test1_rollup4_raw
\d test2_rollup1_raw
\d test2_rollup2_raw
\d test2_rollup3_raw
\d test2_rollup4_raw
\d test3_rollup1_raw
\d test3_rollup2_raw
\d test3_rollup3_raw
\d test3_rollup4_raw

select assert_rollup('test1_rollup1');
select assert_rollup('test1_rollup2');
select assert_rollup('test1_rollup3');
select assert_rollup('test1_rollup4');
select assert_rollup('test2_rollup1');
select assert_rollup('test2_rollup2');
select assert_rollup('test2_rollup3');
select assert_rollup('test2_rollup4');
select assert_rollup('test3_rollup1');
select assert_rollup('test3_rollup2');
select assert_rollup('test3_rollup3');
select assert_rollup('test3_rollup4');

insert into test1 (name,num) values
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
    ('dave', 4),
    ('elliot', 5);

insert into test2 (name,num) values
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
    ('dave', 4),
    ('elliot', 5);

insert into test3 (name,num) values
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
    ('dave', 4),
    ('elliot', 5);

select assert_rollup('test1_rollup1');
select assert_rollup('test1_rollup2');
select assert_rollup('test1_rollup3');
select assert_rollup('test1_rollup4');
select assert_rollup('test2_rollup1');
select assert_rollup('test2_rollup2');
select assert_rollup('test2_rollup3');
select assert_rollup('test2_rollup4');
select assert_rollup('test3_rollup1');
select assert_rollup('test3_rollup2');
select assert_rollup('test3_rollup3');
select assert_rollup('test3_rollup4');

drop table test1 cascade;
drop table test2 cascade;
drop table test3 cascade;
