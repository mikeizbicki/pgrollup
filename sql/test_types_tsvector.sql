create or replace language plpython3u;
create extension if not exists pg_rollup;

create temporary table messages (
    id serial primary key,
    id_user integer,
    text text
);


insert into messages (id_user, text) values
    (0, 'president obama'),
    (0, 'obama was president'),
    (1, 'president trump'),
    (1, 'president bush'),
    (2, 'george bush senior'),
    (2, 'obama obama obama obama obama obama'),
    (3, ''),
    (2, ''),
    (3, NULL),
    (2, NULL),
    (4, NULL),
    (NULL, ''),
    (NULL, 'obama president'),
    (NULL, 'president trump');
    
select create_rollup(
    'messages',
    'messages_rollup1',
    wheres => $$
        unnest(tsvector_to_array(to_tsvector(text))) AS tokens
    $$
);

select create_rollup(
    'messages',
    'messages_rollup2',
    wheres => $$
        unnest(tsvector_to_array(to_tsvector(text))) AS tokens
    $$,
    rollups => $$
        hll(id_user)
    $$
);

select create_rollup(
    'messages',
    'messages_rollup3',
    rollups => $$
        hll(id_user)
    $$
);


select assert_rollup('messages_rollup1');
select assert_rollup('messages_rollup2');
select assert_rollup('messages_rollup3');


insert into messages (id_user, text) values
    (0, 'president obama'),
    (0, 'obama was president'),
    (1, 'president trump'),
    (1, 'president bush'),
    (2, 'george bush senior'),
    (2, 'obama obama obama obama obama obama'),
    (3, ''),
    (2, ''),
    (3, NULL),
    (2, NULL),
    (4, NULL),
    (NULL, ''),
    (NULL, 'obama president'),
    (NULL, 'president trump');

select assert_rollup('messages_rollup1');
select assert_rollup('messages_rollup2');
select assert_rollup('messages_rollup3');
