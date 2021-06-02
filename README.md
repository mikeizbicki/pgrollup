# pg\_rollup

[![Build Status](https://github.com/mikeizbicki/pg_rollup/workflows/tests/badge.svg)](https://github.com/mikeizbicki/pg_rollup/actions)

## TODO~

easy:

1. sorting with a btree index only scan

1. use `count(*)` to implement aggregateless tables

medium:

1. better parsing https://github.com/pganalyze/queryparser/tree/master/extension

    1. filter syntax on aggregates

1. multiparameter aggregate functions (will affect tdigest, datasketches, allows implementing lots of built-in aggs)

1. use aggregate combining functions by default

1. better tracking of dependencies using postgres built-in features

1. benchmarks

1. good interface for settings
    1. allowing deletes for non-groups

hard

1. outer joins

1. subqueries
