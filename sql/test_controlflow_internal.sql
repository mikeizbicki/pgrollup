SET client_min_messages TO WARNING;

create temporary table testinternal (
    id serial primary key,
    name text,
    num int
);


select create_rollup_internal(
    'testinternal_internal1',
    joininfos => '[{"table_name":"testinternal","table_alias":"testinternal","condition":"","join_type":"FROM"}]',
    columns => '{{avg(num),avg}}',
    groups => '{{name,name},{num,num}}',
    having_clause => 'num = 1 OR num >3',
    dry_run => False
);

select create_rollup_internal(
    'testinternal_internal2',
    joininfos => '[{"table_name":"testinternal","table_alias":"testinternal","condition":"","join_type":"FROM"}]',
    columns => '{{sum(num),sum},{max(num),max}}',
    groups => '{{name,name}}',
    where_clause => 'num = 1 OR num >3',
    dry_run => False
);

insert into testinternal (name,num) values
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

select * from testinternal_internal1;
select assert_rollup('testinternal_internal1');
select assert_rollup('testinternal_internal2');

drop table testinternal cascade;
