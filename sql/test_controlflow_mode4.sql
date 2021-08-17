SET client_min_messages TO WARNING;

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

select rollup_mode('test_rollup1','manual');

call update_rollup('test_rollup1');
call update_rollup('test_rollup1', block_size => 10);

select assert_rollup('test_rollup1');

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

call update_rollup('test_rollup1', block_size => 10);

select assert_rollup('test_rollup1');

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

call update_rollup('test_rollup1', block_size => 10);

select assert_rollup('test_rollup1');

do $$
begin
    for counter in 1..50 loop
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
    end loop;
end;
$$ LANGUAGE plpgsql;

call update_rollup('test_rollup1', block_size => 10);

select assert_rollup('test_rollup1');

drop table test cascade;

