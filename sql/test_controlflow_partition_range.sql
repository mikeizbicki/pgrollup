SET client_min_messages TO WARNING;

DROP EVENT TRIGGER pgrollup_from_matview_trigger;

CREATE SCHEMA partman;
CREATE EXTENSION pg_partman WITH SCHEMA partman;

-- this test ensures that range-partitioned rollup tables behave sanely

create table test (
    id serial primary key,
    timestamp_published timestamptz not null,
    num int
);

create materialized view test_rollup1 as (
    select timestamp_published,sum(num)
    from test
    group by timestamp_published
);

select pgrollup_from_matview('test_rollup1', partition_method=>'range', partition_keys=>ARRAY['"test.timestamp_published"']);
SELECT partman.create_parent('public.test_rollup1_raw', 'test.timestamp_published', 'native', 'daily');

SELECT
    nmsp_parent.nspname AS parent_schema,
    parent.relname      AS parent,
    nmsp_child.nspname  AS child_schema,
    child.relname       AS child
FROM pg_inherits
    JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
    JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
    JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
    JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
WHERE parent.relname='test_rollup1_raw';


insert into test (timestamp_published,num) values
    ('2020-01-01', 2),
    ('2020-01-02', 4),
    ('2020-01-03', null),
    ('2020-01-04', 8),
    ('2020-01-05', 0),
    ('2020-01-06', 1),
    ('2020-01-07', 3),
    ('2020-01-08', 5),
    ('2020-01-09', 7),
    ('2020-01-10', 9),
    ('2020-01-01', 2),
    ('2020-01-02', 4),
    ('2020-01-03', null),
    ('2020-01-04', 8),
    ('2020-01-05', 0),
    ('2020-01-06', 1),
    ('2020-01-07', 3),
    ('2020-01-08', 5),
    ('2020-01-09', null),
    ('2020-01-10', 9);

select assert_rollup('test_rollup1');

insert into test (timestamp_published,num) values
    ('2020-01-01', 2),
    ('2020-01-02', 4),
    ('2020-01-03', null),
    ('2020-01-04', 8),
    ('2020-01-05', 0),
    ('2020-01-06', 1),
    ('2020-01-07', 3),
    ('2020-01-08', 5),
    ('2020-01-09', 7),
    ('2020-01-10', 9),
    ('2020-01-01', 2),
    ('2020-01-02', 4),
    ('2020-01-03', null),
    ('2020-01-04', 8),
    ('2020-01-05', 0),
    ('2020-01-06', 1),
    ('2020-01-07', 3),
    ('2020-01-08', 5),
    ('2020-01-09', null),
    ('2020-01-10', 9);

select assert_rollup('test_rollup1');

insert into test (timestamp_published,num) values
    ('2020-01-01', 2),
    ('2020-01-02', 4),
    ('2020-01-03', null),
    ('2020-01-04', 8),
    ('2020-01-05', 0),
    ('2020-01-06', 1),
    ('2020-01-07', 3),
    ('2020-01-08', 5),
    ('2020-01-09', 7),
    ('2020-01-10', 9),
    ('2020-01-01', 2),
    ('2020-01-02', 4),
    ('2020-01-03', null),
    ('2020-01-04', 8),
    ('2020-01-05', 0),
    ('2020-01-06', 1),
    ('2020-01-07', 3),
    ('2020-01-08', 5),
    ('2020-01-09', null),
    ('2020-01-10', 9);

select assert_rollup('test_rollup1');

insert into test (timestamp_published,num) values
    ('2020-01-01', 2),
    ('2020-01-02', 4),
    ('2020-01-03', null),
    ('2020-01-04', 8),
    ('2020-01-05', 0),
    ('2020-01-06', 1),
    ('2020-01-07', 3),
    ('2020-01-08', 5),
    ('2020-01-09', 7),
    ('2020-01-10', 9),
    ('2020-01-01', 2),
    ('2020-01-02', 4),
    ('2020-01-03', null),
    ('2020-01-04', 8),
    ('2020-01-05', 0),
    ('2020-01-06', 1),
    ('2020-01-07', 3),
    ('2020-01-08', 5),
    ('2020-01-09', null),
    ('2020-01-10', 9);

select assert_rollup('test_rollup1');

CREATE EVENT TRIGGER pgrollup_from_matview_trigger ON ddl_command_end WHEN TAG IN ('CREATE MATERIALIZED VIEW') EXECUTE PROCEDURE pgrollup_from_matview_event();
DROP table test CASCADE;

