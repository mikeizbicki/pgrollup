SET client_min_messages TO WARNING;

DROP EVENT TRIGGER pgrollup_from_matview_trigger;

-- this test ensures that hash partitioned rollup tables behave sanely

create table test (
    id serial primary key,
    name text not null, -- FIXME: the "not null" constraint here is needed for hash partitioning but it is not enforced when the rollup is created; nothing breaks if the constraint is missing, but error messages are confusing
    action text not null,
    num int
);

create materialized view test_rollup1 as (
    select name,count(*)
    from test
    group by name
);
select pgrollup_from_matview('test_rollup1', partition_method=>'hash', partition_keys=>ARRAY['"test.name"']);
CALL create_hash_partitions('test_rollup1_raw', 8);

create materialized view test_rollup2 as (
    select name,action,count(*)
    from test
    group by name,action
);
select pgrollup_from_matview('test_rollup2', partition_method=>'hash', partition_keys=>ARRAY['"test.action"']);
CALL create_hash_partitions('test_rollup2_raw', 8);

create materialized view test_rollup3 as (
    select name,action,sum(num)
    from test
    group by name,action
);
select pgrollup_from_matview('test_rollup3', partition_method=>'hash', partition_keys=>ARRAY['"test.name"','"test.action"']);
CALL create_hash_partitions('test_rollup3_raw', 8);

insert into test (name,action,num) values
    ('alice', 'run', 1),
    ('alice', 'run', 2),
    ('alice', 'run', 3),
    ('alice', 'run', 4),
    ('alice', 'run', 5),
    ('bill', 'run', 5),
    ('bill', 'walk', 5),
    ('bill', 'run', NULL),
    ('charlie', 'run', 1),
    ('charlie', 'run', 1),
    ('charlie', 'walk', NULL),
    ('charlie', 'stand', 3),
    ('charlie', 'run', NULL),
    ('dave', 'run', 4),
    ('elliot', 'walk', 5);

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup2');
select assert_rollup('test_rollup3');

insert into test (name,action,num) values
    ('alice', 'run', 1),
    ('alice', 'run', 2),
    ('alice', 'run', 3),
    ('alice', 'run', 4),
    ('alice', 'run', 5),
    ('bill', 'run', 5),
    ('bill', 'walk', 5),
    ('bill', 'run', NULL),
    ('charlie', 'run', 1),
    ('charlie', 'run', 1),
    ('charlie', 'walk', NULL),
    ('charlie', 'stand', 3),
    ('charlie', 'run', NULL),
    ('dave', 'run', 4),
    ('elliot', 'walk', 5);

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup2');
select assert_rollup('test_rollup3');

insert into test (name,action,num) values
    ('alice', 'run', 1),
    ('alice', 'run', 2),
    ('alice', 'run', 3),
    ('alice', 'run', 4),
    ('alice', 'run', 5),
    ('bill', 'run', 5),
    ('bill', 'walk', 5),
    ('bill', 'run', NULL),
    ('charlie', 'run', 1),
    ('charlie', 'run', 1),
    ('charlie', 'walk', NULL),
    ('charlie', 'stand', 3),
    ('charlie', 'run', NULL),
    ('dave', 'run', 4),
    ('elliot', 'walk', 5);

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup2');
select assert_rollup('test_rollup3');

CREATE EVENT TRIGGER pgrollup_from_matview_trigger ON ddl_command_end WHEN TAG IN ('CREATE MATERIALIZED VIEW') EXECUTE PROCEDURE pgrollup_from_matview_event();
DROP table test CASCADE;
