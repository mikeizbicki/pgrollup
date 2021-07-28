SET client_min_messages TO WARNING;

-- FIXME:
-- we drop the vector extension since it prevents comparisons of INT[]=INT[] for some reason
DROP EXTENSION vector CASCADE;
CREATE EXTENSION IF NOT EXISTS pgrollup;
CREATE EVENT TRIGGER pgrollup_from_matview_trigger ON ddl_command_end WHEN TAG IN ('CREATE MATERIALIZED VIEW') EXECUTE PROCEDURE pgrollup_from_matview_event();

--
-- this function removes duplicates from an array,
-- and can be used to modify how rollups work with arrays
--
CREATE OR REPLACE FUNCTION array_uniq(a anyarray) RETURNS anyarray AS $$
SELECT ARRAY(SELECT DISTINCT unnest(a));
$$ LANGUAGE 'sql' STRICT IMMUTABLE PARALLEL SAFE;
do $$
BEGIN
    assert( array_uniq('{}'::INT[]) = '{}'::INT[]);
    assert( array_uniq('{1,1,1,1}'::INT[]) = '{1}'::INT[]);
    assert( array_uniq('{1,1,2,3}'::INT[]) = '{1,2,3}'::INT[]);
    assert( array_uniq('{1,2,3,1}'::INT[]) = '{1,2,3}'::INT[]);
    assert( array_uniq('{NULL,NULL}'::INT[]) = '{NULL}'::INT[]);
    assert( array_uniq(NULL::INT[]) IS NULL);
END;
$$;

create table arrtest (
    id serial primary key,
    a int,
    b int[],
    c text,
    d text[]
);

insert into arrtest (a,b,c,d) values
    (0, '{              }', 'foo', '{1,2,3,4       }'),
    (0, '{              }', 'foo', '{2,2,2         }'),
    (0, '{1,2,3,4       }', 'bar', '{              }'),
    (0, '{1,2,3,4       }', 'bar', '{NULL,NULL     }'),
    (0, '{NULL          }', 'foo', '{NULL          }'),
    (0, '{NULL          }', 'foo', '{NULL,5,6      }'),
    (0, '{NULL,5,6      }', 'foo', '{1,2,3,4       }'),
    (0, '{NULL,5,6      }', 'foo', NULL              ),
    (0, '{NULL,NULL     }', 'foo', '{              }'),
    (0, '{NULL,NULL     }', 'foo', '{2,2,2         }'),
    (1, '{2,2,2         }', 'foo', '{NULL          }'),
    (1, '{2,2,2         }', 'baz', NULL              ),
    (2, NULL              , 'baz', '{NULL,5,6      }'),
    (2, NULL              , 'foo', '{NULL,NULL     }'),
    (3, NULL              , 'foo', NULL              );

create materialized view arrtest_rollup0 as (
    select
        count(*),
        unnest(array_uniq(b)) as qqq
    from arrtest
    group by qqq
);

select * from arrtest_rollup0;

create materialized view arrtest_rollup1 as (
    select 
        count(*),
        unnest(array_uniq(b)) as b
    from arrtest
    group by b
);

create materialized view arrtest_rollup2 as (
    select
        count(*),
        unnest(array_uniq(d))
    from arrtest
    group by unnest
);

create materialized view arrtest_rollup3 as (
    select 
        count(*),
        unnest(array_uniq(b)) AS b,
        unnest(array_uniq(d)) AS d
    from arrtest
    group by b,d
);

create materialized view arrtest_rollup4 as (
    select 
        a,
        unnest(array_uniq(b)) AS b,
        unnest(array_uniq(d)) AS d,
        c,
        count(*)
    from arrtest
    group by a,b,c,d
);

create materialized view arrtest_rollup5 as (
    select 
        unnest(array_uniq(b)) AS b,
        unnest(array_uniq(d)) AS d,
        count(a) as count_a,
        count(c) as count_b
    from arrtest
    group by b,d
);

insert into arrtest (a,b,c,d) values
    (0, '{              }', 'foo', '{1,2,3,4       }'),
    (0, '{              }', 'foo', '{2,2,2         }'),
    (0, '{1,2,3,4       }', 'bar', '{              }'),
    (0, '{1,2,3,4       }', 'bar', '{NULL,NULL     }'),
    (0, '{NULL          }', 'foo', '{NULL          }'),
    (0, '{NULL          }', 'foo', '{NULL,5,6      }'),
    (0, '{NULL,5,6      }', 'foo', '{1,2,3,4       }'),
    (0, '{NULL,5,6      }', 'foo', NULL              ),
    (0, '{NULL,NULL     }', 'foo', '{              }'),
    (0, '{NULL,NULL     }', 'foo', '{2,2,2         }'),
    (1, '{2,2,2         }', 'foo', '{NULL          }'),
    (1, '{2,2,2         }', 'baz', NULL              ),
    (2, NULL              , 'baz', '{NULL,5,6      }'),
    (2, NULL              , 'foo', '{NULL,NULL     }'),
    (3, NULL              , 'foo', NULL              );


select assert_rollup('arrtest_rollup0');
select assert_rollup('arrtest_rollup1');
select assert_rollup('arrtest_rollup2');
select assert_rollup('arrtest_rollup3');
select assert_rollup('arrtest_rollup4');
select assert_rollup('arrtest_rollup5');

drop table arrtest cascade;
