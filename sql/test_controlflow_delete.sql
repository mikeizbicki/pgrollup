create or replace language plpython3u;
create extension if not exists pg_rollup;

create temporary table test (
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

select create_rollup(
    'test',
    'test_rollup1',
    wheres => 'name'
);

select create_rollup(
    'test',
    'test_rollup2',
    wheres => 'name,num'
);


select assert_rollup('test_rollup1');
select assert_rollup('test_rollup2');


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
select assert_rollup('test_rollup2');

delete from test where name='alice';
delete from test where name is null and num is null;

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup2');

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
select assert_rollup('test_rollup2');

drop table test cascade;