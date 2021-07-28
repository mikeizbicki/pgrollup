SET client_min_messages TO WARNING;

create table vector_test (
    id serial primary key,
    a vector(3),
    b integer
);

insert into vector_test (a,b) VALUES
    ('[1,2,3]',0),
    ('[1,2,3]',0),
    ('[1,2,3]',1),
    ('[1,2,3]',NULL),
    ('[2,3,4]',0),
    ('[2,3,4]',0),
    ('[2,3,4]',1),
    ('[2,3,4]',NULL),
    (NULL,0),
    (NULL,1),
    (NULL,NULL);

CREATE MATERIALIZED VIEW vector_test_rollup1 AS (
    SELECT
        count(a),
        vector_sum(a),
        vector_avg(a),
        vector_sum(a)/count(a)
    FROM vector_test
);

CREATE MATERIALIZED VIEW vector_test_rollup2 AS (
    SELECT
        count(a),
        vector_sum(a),
        vector_avg(a),
        vector_sum(a)/count(a),
        b
    FROM vector_test
    GROUP BY b
);

select assert_rollup('vector_test_rollup1');
select assert_rollup('vector_test_rollup2');

insert into vector_test (a,b) VALUES
    ('[1,2,3]',0),
    ('[1,2,3]',0),
    ('[1,2,3]',1),
    ('[1,2,3]',NULL),
    ('[2,3,4]',0),
    ('[2,3,4]',0),
    ('[2,3,4]',1),
    ('[2,3,4]',NULL),
    (NULL,0),
    (NULL,1),
    (NULL,NULL);

select assert_rollup('vector_test_rollup1');
select assert_rollup('vector_test_rollup2');

select * from vector_test_rollup1;
select * from vector_test_rollup2;

drop table vector_test cascade;
