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
    wheres => 'name',
    key => 'id',
    mode => 'manual'
);

select create_rollup(
    'test',
    'test_rollup2',
    wheres => 'name,num',
    key => 'id',
    mode => 'manual'
);

select create_rollup(
    'test',
    'test_rollup3',
    wheres => 'name',
    rollups => 'count(num)',
    key => 'id',
    mode => 'manual'
);

select create_rollup(
    'test',
    'test_rollup4',
    rollups => 'count(name),count(num)',
    key => 'id',
    mode => 'manual'
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

select do_rollup('test_rollup1');
select do_rollup('test_rollup2');
select do_rollup('test_rollup3');
select do_rollup('test_rollup4');

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup2');
select assert_rollup('test_rollup3');
select assert_rollup('test_rollup4');

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
select do_rollup('test_rollup2');
select do_rollup('test_rollup3');
select do_rollup('test_rollup4');

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup2');
select assert_rollup('test_rollup3');
select assert_rollup('test_rollup4');

select do_rollup('test_rollup1');
select do_rollup('test_rollup2');
select do_rollup('test_rollup3');
select do_rollup('test_rollup4');

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup2');
select assert_rollup('test_rollup3');
select assert_rollup('test_rollup4');

select do_rollup('test_rollup1');
select do_rollup('test_rollup2');
select do_rollup('test_rollup3');
select do_rollup('test_rollup4');

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup2');
select assert_rollup('test_rollup3');
select assert_rollup('test_rollup4');


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
select do_rollup('test_rollup2');
select do_rollup('test_rollup3');
select do_rollup('test_rollup4');

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup2');
select assert_rollup('test_rollup3');
select assert_rollup('test_rollup4');

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
select rollup_mode('test_rollup2','trigger');
select rollup_mode('test_rollup3','trigger');
select rollup_mode('test_rollup4','trigger');

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

select rollup_mode('test_rollup1','manual');
select rollup_mode('test_rollup2','manual');
select rollup_mode('test_rollup3','manual');
select rollup_mode('test_rollup4','manual');

select rollup_mode('test_rollup1','trigger');
select rollup_mode('test_rollup2','trigger');
select rollup_mode('test_rollup3','trigger');
select rollup_mode('test_rollup4','trigger');

select rollup_mode('test_rollup1','trigger');
select rollup_mode('test_rollup2','trigger');
select rollup_mode('test_rollup3','trigger');
select rollup_mode('test_rollup4','trigger');

select rollup_mode('test_rollup1','manual');
select rollup_mode('test_rollup2','manual');
select rollup_mode('test_rollup3','manual');
select rollup_mode('test_rollup4','manual');

select rollup_mode('test_rollup1','manual');
select rollup_mode('test_rollup2','manual');
select rollup_mode('test_rollup3','manual');
select rollup_mode('test_rollup4','manual');

select rollup_mode('test_rollup1','manual');
select rollup_mode('test_rollup2','manual');
select rollup_mode('test_rollup3','trigger');
select rollup_mode('test_rollup4','trigger');

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
select do_rollup('test_rollup2');
select do_rollup('test_rollup3');
select do_rollup('test_rollup4');

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup2');
select assert_rollup('test_rollup3');
select assert_rollup('test_rollup4');

select rollup_mode('test_rollup1','manual');
select rollup_mode('test_rollup2','manual');
select rollup_mode('test_rollup3','manual');
select rollup_mode('test_rollup4','manual');

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

select do_rollup('test_rollup1',5);
select do_rollup('test_rollup2',5);
select do_rollup('test_rollup3',5);
select do_rollup('test_rollup4',5);

select do_rollup('test_rollup1',5);
select do_rollup('test_rollup2',5);
select do_rollup('test_rollup3',5);
select do_rollup('test_rollup4',5);

select do_rollup('test_rollup1',5);
select do_rollup('test_rollup2',5);
select do_rollup('test_rollup3',5);
select do_rollup('test_rollup4',5);

select do_rollup('test_rollup1',5);
select do_rollup('test_rollup2',5);
select do_rollup('test_rollup3',5);
select do_rollup('test_rollup4',5);

select do_rollup('test_rollup1',5);
select do_rollup('test_rollup2',5);
select do_rollup('test_rollup3',5);
select do_rollup('test_rollup4',5);

select do_rollup('test_rollup1',5);
select do_rollup('test_rollup2',5);
select do_rollup('test_rollup3',5);
select do_rollup('test_rollup4',5);

select do_rollup('test_rollup1',5);
select do_rollup('test_rollup2',5);
select do_rollup('test_rollup3',5);
select do_rollup('test_rollup4',5);

select do_rollup('test_rollup1',5);
select do_rollup('test_rollup2',5);
select do_rollup('test_rollup3',5);
select do_rollup('test_rollup4',5);

select assert_rollup('test_rollup1');
select assert_rollup('test_rollup2');
select assert_rollup('test_rollup3');
select assert_rollup('test_rollup4');

drop table test cascade;
