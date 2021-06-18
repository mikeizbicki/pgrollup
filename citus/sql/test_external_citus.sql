SET client_min_messages TO WARNING;
create extension citus;
create extension plpython3u;
create extension pgrollup;

-- FIXME:
-- citus does not work with triggers, and so the default trigger mode does not work;
-- pgrollup should be smart enough to default to manual mode when creating the triggers fails;
-- since it's not, we must manually specify the default mode here;
-- you cannot specify the mode after creating the rollup because rollup creation will fail
update pgrollup_settings set value='manual' where name='default_mode';

create table test (
    id serial,
    name text,
    num int,
    primary key (name,id)
);
select create_distributed_table('test', 'name');

/*
 * FIXME: a citus table must be empty when we create the rollup table.
 *
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
    ('elliot', 5);
*/


-- for each rollup below, we create two versions;
-- the first is a normal postgresql table,
-- and the second will be a distributed table managed by citus
CREATE MATERIALIZED VIEW test_rollup1 AS (
    SELECT
        name AS name,
        sum(num) as sum,
        count(*) as count_all,
        count(num),
        max(num),
        min(num)
    FROM test
    GROUP BY name
);

CREATE MATERIALIZED VIEW test_rollup1b AS (
    SELECT
        name AS name,
        sum(num) as sum,
        count(*) as count_all,
        count(num),
        max(num),
        min(num)
    FROM test
    GROUP BY name
);
select create_distributed_table('test_rollup1b_raw', 'test.name');

CREATE MATERIALIZED VIEW test_rollup2 AS (
    SELECT
        name AS name,
        count(name) AS count_name,
        count(num)  AS count_num
    FROM test
    GROUP BY name
);

CREATE MATERIALIZED VIEW test_rollup2b AS (
    SELECT
        name AS name,
        count(name) AS count_name,
        count(num)  AS count_num
    FROM test
    GROUP BY name
);
select create_distributed_table('test_rollup2b_raw', 'test.name');

select * from pgrollup_rollups;

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
    ('elliot', 5);

select do_rollup('test_rollup1');
select do_rollup('test_rollup2');

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup2');

insert into test (name,num) values
    ('alice', 1),
    ('alice', NULL),
    ('alice', 3),
    ('alice', 4),
    ('alice', 5),
    ('bill', NULL),
    ('bill', NULL),
    ('bill', 5),
    ('charlie', NULL),
    ('charlie', 1),
    ('charlie', NULL),
    ('elliot', 5);

select do_rollup('test_rollup1');
select do_rollup('test_rollup2');

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup2');

insert into test (name,num) values
    ('alice', 1),
    ('alice', 2),
    ('alice', 3),
    ('elliot', 5);

select do_rollup('test_rollup1');
select do_rollup('test_rollup2');

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup2');

select * from test_rollup1 order by "test.name";
select * from test_rollup2 order by "test.name";

drop table test cascade;

