SET client_min_messages TO WARNING;
create or replace language plpython3u;
create extension if not exists pg_rollup;

create table test (
    id serial primary key,
    name text,
    num int
);

insert into test (name,num) values
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
    ('elliot', 5),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 8),
    (NULL, 9),
    (NULL, NULL),
    (NULL, NULL),
    (NULL, NULL),
    (NULL, NULL);

create materialized view test_rollup1 as (
    select name,count(*)
    from test
    group by name
);

create materialized view test_rollup1b as (
    select name,count(*)
    from test
    group by name
);

create materialized view test_rollup2 as (
    select name,num,count(*)
    from test
    group by name,num
);

create materialized view test_rollup2b as (
    select name,num,count(*)
    from test
    group by name,num
);

create materialized view test_rollup3 as (
    select name,count(num)
    from test
    group by name
);

create materialized view test_rollup3b as (
    select name,count(num)
    from test
    group by name
);

create materialized view test_rollup4 as (
    select count(name) AS count1,count(num) AS count2
    from test
);

create materialized view test_rollup4b as (
    select count(name) AS count1,count(num) AS count2
    from test
);

select rollup_mode('test_rollup1','manual');
select rollup_mode('test_rollup1b','manual');
select rollup_mode('test_rollup2','manual');
select rollup_mode('test_rollup2b','manual');
select rollup_mode('test_rollup3','manual');
select rollup_mode('test_rollup3b','manual');
select rollup_mode('test_rollup4','manual');
select rollup_mode('test_rollup4b','manual');

insert into test (name,num) values
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
    ('elliot', 5),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 8),
    (NULL, 9),
    (NULL, NULL),
    (NULL, NULL),
    (NULL, NULL),
    (NULL, NULL);

select do_rollup('test_rollup1');
select do_rollup('test_rollup1b');
select do_rollup('test_rollup2');
select do_rollup('test_rollup2b');
select do_rollup('test_rollup3');
select do_rollup('test_rollup3b');
select do_rollup('test_rollup4');
select do_rollup('test_rollup4b');

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup1b');
select assert_rollup('test_rollup2');
select assert_rollup('test_rollup2b');
select assert_rollup('test_rollup3');
select assert_rollup('test_rollup3b');
select assert_rollup('test_rollup4');
select assert_rollup('test_rollup4b');

insert into test (name,num) values
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
    ('elliot', 5),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 8),
    (NULL, 9),
    (NULL, NULL),
    (NULL, NULL),
    (NULL, NULL),
    (NULL, NULL);


select do_rollup('test_rollup1');
select do_rollup('test_rollup1b');
select do_rollup('test_rollup2');
select do_rollup('test_rollup2b');
select do_rollup('test_rollup3');
select do_rollup('test_rollup3b');
select do_rollup('test_rollup4');
select do_rollup('test_rollup4b');

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup1b');
select assert_rollup('test_rollup2');
select assert_rollup('test_rollup2b');
select assert_rollup('test_rollup3');
select assert_rollup('test_rollup3b');
select assert_rollup('test_rollup4');
select assert_rollup('test_rollup4b');

select do_rollup('test_rollup1');
select do_rollup('test_rollup1b');
select do_rollup('test_rollup2');
select do_rollup('test_rollup2b');
select do_rollup('test_rollup3');
select do_rollup('test_rollup3b');
select do_rollup('test_rollup4');
select do_rollup('test_rollup4b');

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup1b');
select assert_rollup('test_rollup2');
select assert_rollup('test_rollup2b');
select assert_rollup('test_rollup3');
select assert_rollup('test_rollup3b');
select assert_rollup('test_rollup4');
select assert_rollup('test_rollup4b');

select do_rollup('test_rollup1');
select do_rollup('test_rollup1b');
select do_rollup('test_rollup2');
select do_rollup('test_rollup2b');
select do_rollup('test_rollup3');
select do_rollup('test_rollup3b');
select do_rollup('test_rollup4');
select do_rollup('test_rollup4b');

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup1b');
select assert_rollup('test_rollup2');
select assert_rollup('test_rollup2b');
select assert_rollup('test_rollup3');
select assert_rollup('test_rollup3b');
select assert_rollup('test_rollup4');
select assert_rollup('test_rollup4b');


insert into test (name,num) values
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
    ('elliot', 5),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 8),
    (NULL, 9),
    (NULL, NULL),
    (NULL, NULL),
    (NULL, NULL),
    (NULL, NULL);

insert into test (name,num) values
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
    ('elliot', 5),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 8),
    (NULL, 9),
    (NULL, NULL),
    (NULL, NULL),
    (NULL, NULL),
    (NULL, NULL);

insert into test (name,num) values
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
    ('elliot', 5),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 8),
    (NULL, 9),
    (NULL, NULL),
    (NULL, NULL),
    (NULL, NULL),
    (NULL, NULL);

select do_rollup('test_rollup1');
select do_rollup('test_rollup1b');
select do_rollup('test_rollup2');
select do_rollup('test_rollup2b');
select do_rollup('test_rollup3');
select do_rollup('test_rollup3b');
select do_rollup('test_rollup4');
select do_rollup('test_rollup4b');

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup1b');
select assert_rollup('test_rollup2');
select assert_rollup('test_rollup2b');
select assert_rollup('test_rollup3');
select assert_rollup('test_rollup3b');
select assert_rollup('test_rollup4');
select assert_rollup('test_rollup4b');

insert into test (name,num) values
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
    ('elliot', 5),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 8),
    (NULL, 9),
    (NULL, NULL),
    (NULL, NULL),
    (NULL, NULL),
    (NULL, NULL);

select rollup_mode('test_rollup1','trigger');
select rollup_mode('test_rollup1b','trigger');
select rollup_mode('test_rollup2','trigger');
select rollup_mode('test_rollup2b','trigger');
select rollup_mode('test_rollup3','trigger');
select rollup_mode('test_rollup3b','trigger');
select rollup_mode('test_rollup4','trigger');
select rollup_mode('test_rollup4b','trigger');

insert into test (name,num) values
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
    ('elliot', 5),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 8),
    (NULL, 9),
    (NULL, NULL),
    (NULL, NULL),
    (NULL, NULL),
    (NULL, NULL);

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup1b');
select assert_rollup('test_rollup2');
select assert_rollup('test_rollup2b');
select assert_rollup('test_rollup3');
select assert_rollup('test_rollup3b');
select assert_rollup('test_rollup4');
select assert_rollup('test_rollup4b');

select rollup_mode('test_rollup1','manual');
select rollup_mode('test_rollup1b','manual');
select rollup_mode('test_rollup2','manual');
select rollup_mode('test_rollup2b','manual');
select rollup_mode('test_rollup3','manual');
select rollup_mode('test_rollup3b','manual');
select rollup_mode('test_rollup4','manual');
select rollup_mode('test_rollup4b','manual');

select rollup_mode('test_rollup1','trigger');
select rollup_mode('test_rollup1b','trigger');
select rollup_mode('test_rollup2','trigger');
select rollup_mode('test_rollup2b','trigger');
select rollup_mode('test_rollup3','trigger');
select rollup_mode('test_rollup3b','trigger');
select rollup_mode('test_rollup4','trigger');
select rollup_mode('test_rollup4b','trigger');

select rollup_mode('test_rollup1','trigger');
select rollup_mode('test_rollup1b','trigger');
select rollup_mode('test_rollup2','trigger');
select rollup_mode('test_rollup2b','trigger');
select rollup_mode('test_rollup3','trigger');
select rollup_mode('test_rollup3b','trigger');
select rollup_mode('test_rollup4','trigger');
select rollup_mode('test_rollup4b','trigger');

select rollup_mode('test_rollup1','manual');
select rollup_mode('test_rollup1b','manual');
select rollup_mode('test_rollup2','manual');
select rollup_mode('test_rollup2b','manual');
select rollup_mode('test_rollup3','manual');
select rollup_mode('test_rollup3b','manual');
select rollup_mode('test_rollup4','manual');
select rollup_mode('test_rollup4b','manual');

select rollup_mode('test_rollup1','manual');
select rollup_mode('test_rollup1b','manual');
select rollup_mode('test_rollup2','manual');
select rollup_mode('test_rollup2b','manual');
select rollup_mode('test_rollup3','manual');
select rollup_mode('test_rollup3b','manual');
select rollup_mode('test_rollup4','manual');
select rollup_mode('test_rollup4b','manual');

select rollup_mode('test_rollup1','manual');
select rollup_mode('test_rollup1b','manual');
select rollup_mode('test_rollup2','manual');
select rollup_mode('test_rollup2b','manual');
select rollup_mode('test_rollup3','trigger');
select rollup_mode('test_rollup3b','trigger');
select rollup_mode('test_rollup4','trigger');
select rollup_mode('test_rollup4b','trigger');

insert into test (name,num) values
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
    ('elliot', 5),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 8),
    (NULL, 9),
    (NULL, NULL),
    (NULL, NULL),
    (NULL, NULL),
    (NULL, NULL);

select do_rollup('test_rollup1');
select do_rollup('test_rollup1b');
select do_rollup('test_rollup2');
select do_rollup('test_rollup2b');
select do_rollup('test_rollup3');
select do_rollup('test_rollup3b');
select do_rollup('test_rollup4');
select do_rollup('test_rollup4b');

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup1b');
select assert_rollup('test_rollup2');
select assert_rollup('test_rollup2b');
select assert_rollup('test_rollup3');
select assert_rollup('test_rollup3b');
select assert_rollup('test_rollup4');
select assert_rollup('test_rollup4b');

select rollup_mode('test_rollup1','manual');
select rollup_mode('test_rollup1b','manual');
select rollup_mode('test_rollup2','manual');
select rollup_mode('test_rollup2b','manual');
select rollup_mode('test_rollup3','manual');
select rollup_mode('test_rollup3b','manual');
select rollup_mode('test_rollup4','manual');
select rollup_mode('test_rollup4b','manual');

insert into test (name,num) values
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
    ('elliot', 5),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 1),
    (NULL, 8),
    (NULL, 9),
    (NULL, NULL),
    (NULL, NULL),
    (NULL, NULL),
    (NULL, NULL);

select do_rollup('test_rollup1',NULL,5);
select do_rollup('test_rollup1b',NULL,5);
select do_rollup('test_rollup2',NULL,5);
select do_rollup('test_rollup2b',NULL,5);
select do_rollup('test_rollup3',NULL,5);
select do_rollup('test_rollup3b',NULL,5);
select do_rollup('test_rollup4',NULL,5);
select do_rollup('test_rollup4b',NULL,5);

select do_rollup('test_rollup1',NULL,5);
select do_rollup('test_rollup1b',NULL,5);
select do_rollup('test_rollup2',NULL,5);
select do_rollup('test_rollup2b',NULL,5);
select do_rollup('test_rollup3',NULL,5);
select do_rollup('test_rollup3b',NULL,5);
select do_rollup('test_rollup4',NULL,5);
select do_rollup('test_rollup4b',NULL,5);

select do_rollup('test_rollup1',NULL,5);
select do_rollup('test_rollup1b',NULL,5);
select do_rollup('test_rollup2',NULL,5);
select do_rollup('test_rollup2b',NULL,5);
select do_rollup('test_rollup3',NULL,5);
select do_rollup('test_rollup3b',NULL,5);
select do_rollup('test_rollup4',NULL,5);
select do_rollup('test_rollup4b',NULL,5);

select do_rollup('test_rollup1',NULL,5);
select do_rollup('test_rollup1b',NULL,5);
select do_rollup('test_rollup2',NULL,5);
select do_rollup('test_rollup2b',NULL,5);
select do_rollup('test_rollup3',NULL,5);
select do_rollup('test_rollup3b',NULL,5);
select do_rollup('test_rollup4',NULL,5);
select do_rollup('test_rollup4b',NULL,5);

select do_rollup('test_rollup1',NULL,5);
select do_rollup('test_rollup1b',NULL,5);
select do_rollup('test_rollup2',NULL,5);
select do_rollup('test_rollup2b',NULL,5);
select do_rollup('test_rollup3',NULL,5);
select do_rollup('test_rollup3b',NULL,5);
select do_rollup('test_rollup4',NULL,5);
select do_rollup('test_rollup4b',NULL,5);

select do_rollup('test_rollup1',NULL,5);
select do_rollup('test_rollup1b',NULL,5);
select do_rollup('test_rollup2',NULL,5);
select do_rollup('test_rollup2b',NULL,5);
select do_rollup('test_rollup3',NULL,5);
select do_rollup('test_rollup3b',NULL,5);
select do_rollup('test_rollup4',NULL,5);
select do_rollup('test_rollup4b',NULL,5);

select do_rollup('test_rollup1',NULL,5);
select do_rollup('test_rollup1b',NULL,5);
select do_rollup('test_rollup2',NULL,5);
select do_rollup('test_rollup2b',NULL,5);
select do_rollup('test_rollup3',NULL,5);
select do_rollup('test_rollup3b',NULL,5);
select do_rollup('test_rollup4',NULL,5);
select do_rollup('test_rollup4b',NULL,5);

select do_rollup('test_rollup1',NULL,5);
select do_rollup('test_rollup1b',NULL,5);
select do_rollup('test_rollup2',NULL,5);
select do_rollup('test_rollup2b',NULL,5);
select do_rollup('test_rollup3',NULL,5);
select do_rollup('test_rollup3b',NULL,5);
select do_rollup('test_rollup4',NULL,5);
select do_rollup('test_rollup4b',NULL,5);

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup1b');
select assert_rollup('test_rollup2');
select assert_rollup('test_rollup2b');
select assert_rollup('test_rollup3');
select assert_rollup('test_rollup3b');
select assert_rollup('test_rollup4');
select assert_rollup('test_rollup4b');

drop table test cascade;
