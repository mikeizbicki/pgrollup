SET client_min_messages TO WARNING;

-- if we call pg_rollup functinos on a table that doesn't exist, they should throw an informative error

select do_rollup('table_does_not_exist');

select rollup_mode('table_does_not_exist', 'trigger');

