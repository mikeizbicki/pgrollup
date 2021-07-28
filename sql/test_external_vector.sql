SET client_min_messages TO WARNING;

create table vector_test (
    id serial primary key,
    a vector(3)
);

insert into vector_test (a) VALUES ('[1,2,3]'), ('[2,3,4]'), (NULL);

CREATE MATERIALIZED VIEW vector_test_rollup1 AS (
    SELECT
        count(a),
        vector_sum(a),
        vector_avg(a),
        vector_sum(a)/count(a)
    FROM vector_test
);

insert into vector_test (a) VALUES ('[1,2,3]'), ('[2,3,4]'), (NULL);

select assert_rollup('vector_test_rollup1');

select * from vector_test_rollup1;

drop table vector_test cascade;
