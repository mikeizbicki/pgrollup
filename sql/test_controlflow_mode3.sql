SET client_min_messages TO WARNING;

-- this test ensures that do_rollup works even when there's no data in the table

UPDATE pgrollup_settings SET value='manual' WHERE name='default_mode';

create table test (
    id serial primary key,
    name text,
    num int
);

create materialized view test_rollup1 as (
    select name,count(*)
    from test
    group by name
);


select do_rollup('test_rollup1');
select do_rollup('test_rollup1');
select do_rollup('test_rollup1');
select do_rollup('test_rollup1');
select do_rollup('test_rollup1');
select do_rollup('test_rollup1');
select do_rollup('test_rollup1');

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
select do_rollup('test_rollup1');
select do_rollup('test_rollup1');
select do_rollup('test_rollup1');
select do_rollup('test_rollup1');

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
select do_rollup('test_rollup1');
select do_rollup('test_rollup1');
select do_rollup('test_rollup1');
select do_rollup('test_rollup1');

select drop_rollup('test_rollup1');
drop table test cascade;
UPDATE pgrollup_settings SET value='trigger' WHERE name='default_mode'

