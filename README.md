# pg\_rollup [![Build Status](https://github.com/mikeizbicki/pg_rollup/workflows/tests/badge.svg)](https://github.com/mikeizbicki/pg_rollup/actions)

**tl;dr**
This extension uses [monoids]() to solve the [incrementally refresh materialized view problem]() in Postgres.
The extension can be used to compute aggregate functions (e.g. `count(*)`, `count(distinct *)`, `avg`, `median`, etc.) in only O(1) time during the `SELECT` statement by precomputing these values in a "rollup" table.
The rollup table is precomputed automatically and the use of monoids ensures that these computations are asymptotically efficient.
(All previous implementations, including the system in OracleDB, do not use monoids, and are therefore not asymptotically efficient.)
The extension also introduces a new syntax for semantically defining these rollup tables trivially.

Outline:
1. The Problem
    1. Existing Incomplete Solution 1: Native Oracle/Postgres Implementations
    1. Existing Incomplete Solution 1: Triggers
1. The pg\_rollup Solution
1. Supported Monoids/Groups
    1. Native Postgres Monoids/Groups
    1. Monoids/Groups provided by other extensions
        1. HyperLogLog
        1. t-digest
    1. Adding Support for a new Monoid/Group
1. Library Admin
    1. Modes of Operation
    1. Runtime Correctness Guarantees
1. Examples
    1. 
    1. Text
    1. More examples
1. Installation / Configuration / Docker
1. Known Limitations

## The Problem

```
CREATE TABLE access_logs (
    id                  BIGSERIAL PRIMARY KEY,
    access_time         TIMESTAMPTZ,
    ipaddr              INET,
    bytes_transferred   INT,
    status_code         SMALLINT
);
```

```
SELECT count(*) FROM access_logs WHERE date(access_time) = '2021-04-01';
```

```
CREATE MATERIALIZED VIEW access_logs_stats AS (
    SELECT
        date(access_time)           AS date
        count(*)                    AS num_connections,
        count(distinct ipaddr)      AS unique_ips,
        avg(bytes_transferred)      AS avg_bytes,
        stddev(bytes_transferred)   AS stddev_bytes,
        percentile_cont(0.5) WITHIN GROUP(ORDER BY bytes_transferred) AS median_bytes,
    FROM transactions
    GROUP BY date(access_time)
);
CREATE INDEX access_logs_stats_idx ON access_logs_stats(idx);
```

Now we can get the repeat our `SELECT` statement above as follows
```
SELECT num_connections FROM access_logs_stats WHERE date = '2021-04-01';
```
This select statement takes time only O(1) instead of O(k).
The problem, however, is what happens when we get new data to add to the table?
In current versions of Postgres (<= version 13),
we must fully recompute view with the following command:
```
REFRESH MATERIALIZED VIEW access_logs_stats;
```
This is an expensive operation when the underlying `access_logs` table is large---if the underlying table is 10s of terabytes,
refreshing the view could take hours or even days!

**The Goal:**
What we need is a way to "incrementally" update only those parts of the materialized view that have changed.
These incremental updates are a widely requested feature among users (e.g.
[1](https://stackoverflow.com/questions/47211576/refresh-only-part-of-a-materialized-view),
[2](https://stackoverflow.com/questions/29437650/how-can-i-ensure-that-a-materialized-view-is-always-up-to-date),
[3](https://dba.stackexchange.com/questions/86779/refresh-materalized-view-incrementally-in-postgresql),
[4](https://dba.stackexchange.com/questions/165948/refresh-a-postgresql-materialized-view-automatically-without-using-triggers),
[5](https://stackoverflow.com/questions/59864339/best-way-to-pre-aggregate-time-series-data-in-postgres)).
A large number of attempts have been made to partially solve this problem,
but none of the existing attempts are optimal.
We now review these existing attempts and their shortcomings:

### Existing Incomplete Solution 1: Native Oracle/Postgres support

The Oracle database has built-in support for incremental refreshes (see [the docs](https://docs.oracle.com/database/121/DWHSG/refresh.htm#DWHSG-GUID-64068234-BDB0-4C12-AE70-75571046A586)) and there's currently a patch for Postgres to implement this feature in a similar manner (see [the postgres wiki page](https://wiki.postgresql.org/wiki/Incremental_View_Maintenance), [author's blog post](https://pgsqlpgpool.blogspot.com/2019/08/automatically-updating-materialized.html), [mailinglist conversation](https://www.postgresql.org/message-id/flat/20181227215726.4d166b4874f8983a641123f5%40sraoss.co.jp), and [the github repo](https://github.com/sraoss/pgsql-ivm/issues)).
It's possible this patch will make it into the standard Postgres as early as version 14.

The fundamental limit of both of these implementations, however, is that they do not take advantage of any monoid/group structure inherent in aggregate functions.
Monoids and groups are widely known in functional programming community for their ability to speed up parallel programming tasks,
but AFAIK they have never been used to speed up incremental refreshes.
See the [implementation details]() below for a full description of exactly what monoids and groups are,
and how the pg\_rollup extension uses these algebraic structures for faster updates.

### Existing Incomplete Solution 2: Triggers

The standard solution to this problem in existing postgres installations is to create [rollup tables](https://www.citusdata.com/blog/2018/10/31/materialized-views-vs-rollup-tables/) manually.
These rollup tables typically use triggers to automatically update the rollup table whenever an update happens in the underlying table.

These trigger-based solutions, however, also have significant limitations:

1. Writing the appropriate triggers is a complex process,
   and there are currently no standard best practices.
   A quick google search will reveal many possible trigger architectures (e.g. 
[1](https://hashrocket.com/blog/posts/materialized-view-strategies-using-postgresql),
[2](http://www.varlena.com/GeneralBits/Tidbits/matviews.html),
[3](https://stefan-poeltl.medium.com/views-v-s-materialized-views-v-s-rollup-tables-with-postgresql-2b3824b45330),
[4](https://dzone.com/articles/scalable-incremental-data-aggregation-on-postgres),
[5](https://www.xaprb.com/blog/2006/07/19/3-ways-to-maintain-rollup-tables-in-sql/)
   ).
   No matter which architecture you pick, the resulting code can be hundreds of lines long, and bugs are both common, subtle, and extremely difficult to detect.
   For example, a common class of bugs is to not handle floating point arithemetic properly and have [catestrophic cancellation](https://docs.oracle.com/cd/E19957-01/806-3568/ncg_goldberg.html) occur.

   The pg\_rollup extension fully automates this entire process.
   Rather than writing hundreds of lines of complex, error-prone trigger logic,
   you only need to write a short 3-4 line description of your rollup table.
   The library automatically does the rest for you.
   The [Solution]() Section below shows the syntax.

1. Perhaps surprisingly, many existing hand-written rollup table solutions are significantly faster than the native Oracle/Postgres solutions because they use monoids to make their code asymptotically efficient.
   For example, you can find advanced monoid solutions based on the [HyperLogLog](https://www.citusdata.com/blog/2017/06/30/efficient-rollup-with-hyperloglog-on-postgres/) and the [t-digest](https://stackify.com/sql-percentile-aggregates-and-rollups-with-postgresql-and-t-digest/) at the preceding links.

   This library automates the process of implementing these monoids in your database rollup tables,
   and also provides a centeralized references of all the existing monoids currently implemented in postgres.
   See the [Monoids]() section below for a list of currently implemented monoids and instructions on how to use them with the pg\_rollup library.

1. Triggers can impose significant overhead on the `INSERT`/`UPDATE`/`DELETE` operations of the database.
   A statement that invokes a trigger is not done executing until all of the triggers are done executing,
   and triggers cannot execute in parallel.
   This implies that a table with many rollups will suffer significant performance degredation when modifying its contents.

   The pg\_rollup extension solves this problem by removing the need for these expensive triggers.
   The updates to the rollup tables can happen fully in parallel in background cron processes,
   so `INSERT`/`UPDATE`/`DELETE` performance remains exactly as it did before.
   The [Modes]() Section below describes the implementation details for how this works internally.
  

## The (Complete) Solution



Limitations:
1. only supports a single table, and not views/joins
