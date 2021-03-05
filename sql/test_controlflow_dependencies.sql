/*
 * The purpose of this test file is a bit different than other test files;
 * we are not checking anywhere that the rollups created are correct;
 * instead, we are only checking that there are no errors when generating/using the rollup;
 * this will ensure that all the column dependencies are satisfied.
 * We are not trying trying to check that the dependencies for a particular type are satisfied,
 * but rather that the dependency checker is working;
 * therefore, there is no need to modify this file when adding a new algebra.
 */
create or replace language plpython3u;
create extension if not exists pg_rollup;

create temporary table testdeps (
    id serial primary key,
    a int,
    b int
);

insert into testdeps (a,b) values
    (0, 1),
    (0, 2),
    (0, 3),
    (0, 4),
    (0, 5),
    (1, 5),
    (1, 5),
    (1, 5),
    (2, 1),
    (2, 1),
    (2, 1),
    (2, 3),
    (2, NULL),
    (3, 4),
    (4, 5),
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
    'testdeps',
    'testdeps_rollup1',
    wheres => 'a',
    rollups => $$
        avg(b)
    $$
);

select create_rollup(
    'testdeps',
    'testdeps_rollup2',
    wheres => 'a',
    rollups => $$
        var_pop(b)
    $$
);

select create_rollup(
    'testdeps',
    'testdeps_rollup3',
    wheres => 'a',
    rollups => $$
        count(b),
        var_pop(b)
    $$
);

select create_rollup(
    'testdeps',
    'testdeps_rollup4',
    wheres => 'a',
    rollups => $$
        count(*),
        var_pop(b)
    $$
);

select create_rollup(
    'testdeps',
    'testdeps_rollup5',
    rollups => $$
        avg(a),
        var_pop(b)
    $$
);

select create_rollup(
    'testdeps',
    'testdeps_rollup6',
    rollups => $$
        var_pop(a),
        var_pop(b)
    $$
);

insert into testdeps (a,b) values
    (0, 1),
    (0, 2),
    (0, 3),
    (0, 4),
    (0, 5),
    (1, 5),
    (1, 5),
    (1, 5),
    (2, 1),
    (2, 1),
    (2, 1),
    (2, 3),
    (2, NULL),
    (3, 4),
    (4, 5),
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