/*
 * this file tests the types in the basic test case, but puts more emphasis on null values;
 * there were some bugs where null values were not getting coalesced into zero,
 * and this file is designed to catch those edge cases;
 * it doesn't test more exotic types, but hopefully that's not necessary
 */
create or replace language plpython3u;
create extension if not exists pg_rollup;

create temporary table nulltest (
    id serial primary key,
    name text,
    num int
);

insert into nulltest (name,num) values
    (NULL, NULL);

select create_rollup(
    'nulltest',
    'nulltest_rollup1',
    wheres => 'name'
);

select create_rollup(
    'nulltest',
    'nulltest_rollup2',
    wheres => 'name,num'
);

select create_rollup(
    'nulltest',
    'nulltest_rollup3',
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
    'nulltest',
    'nulltest_rollup4',
    rollups => $$
        sum(num) as sum,
        count(*) as count_all,
        count(num),
        max(num),
        min(num)
    $$
);

select assert_rollup('nulltest_rollup1');
select assert_rollup('nulltest_rollup2');
select assert_rollup('nulltest_rollup3');
select assert_rollup('nulltest_rollup4');

insert into nulltest (name,num) values
    (NULL, NULL);

select * from nulltest_rollup3_raw;
select * from nulltest_rollup3_groundtruth_raw;

select assert_rollup('nulltest_rollup1');
select assert_rollup('nulltest_rollup2');
select assert_rollup('nulltest_rollup3');
select assert_rollup('nulltest_rollup4');

insert into nulltest (name,num) values
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

select * from nulltest_rollup3_raw;
select * from nulltest_rollup3_groundtruth_raw;

select assert_rollup('nulltest_rollup1');
select assert_rollup('nulltest_rollup2');
select assert_rollup('nulltest_rollup3');
select assert_rollup('nulltest_rollup4');

insert into nulltest (name,num) values
    (NULL, NULL);

select assert_rollup('nulltest_rollup1');
select assert_rollup('nulltest_rollup2');
select assert_rollup('nulltest_rollup3');
select assert_rollup('nulltest_rollup4');

insert into nulltest (name,num) values
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

select assert_rollup('nulltest_rollup1');
select assert_rollup('nulltest_rollup2');
select assert_rollup('nulltest_rollup3');
select assert_rollup('nulltest_rollup4');


select * from nulltest_rollup3_raw;
select * from nulltest_rollup3_groundtruth_raw;
