create or replace language plpython3u;
create extension if not exists hll;
create extension if not exists topn;
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

select create_rollup(
    'test',
    'test_rollup3',
    wheres => 'name',
    rollups => 'hll(num)'
);

select create_rollup(
    'test',
    'test_rollup4',
    rollups => $$
        hll(name),
        hll(num)
    $$
);

select create_rollup(
    'test',
    'test_rollup5',
    wheres => 'name',
    rollups => $$
        sum(num) as sum,
        count(*) as count_all,
        count(num),
        max(num),
        min(num)
    $$
);

select create_rollup(
    'test',
    'test_rollup6',
    rollups => $$
        sum(num) as sum,
        count(*) as count_all,
        count(num),
        max(num),
        min(num)
    $$
);

select create_rollup(
    'test',
    'test_rollup7',
    wheres => 'num',
    rollups => $$
        topn(name)
    $$
);

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup2');
select assert_rollup('test_rollup3');
select assert_rollup('test_rollup4');
select assert_rollup('test_rollup5');
select assert_rollup('test_rollup6');
select assert_rollup('test_rollup7');


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
select assert_rollup('test_rollup3');
select assert_rollup('test_rollup4');
select assert_rollup('test_rollup5');
select assert_rollup('test_rollup6');
select assert_rollup('test_rollup7');

select drop_rollup('test_rollup1');
select drop_rollup('test_rollup2');
select drop_rollup('test_rollup3');
select drop_rollup('test_rollup4');
select drop_rollup('test_rollup5');
select drop_rollup('test_rollup6');
select drop_rollup('test_rollup7');

drop table test cascade;
