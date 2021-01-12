create or replace language plpython3u;
create extension if not exists pg_rollup;

create temporary table arrtest (
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

select create_rollup(
    'arrtest',
    'arrtest_rollup0',
    wheres => $$
        unnest(b)
    $$
);

select create_rollup(
    'arrtest',
    'arrtest_rollup1',
    wheres => $$
        unnest(array_uniq(b))
    $$
);

select create_rollup(
    'arrtest',
    'arrtest_rollup2',
    wheres => $$
        unnest(array_uniq(d))
    $$
);

select create_rollup(
    'arrtest',
    'arrtest_rollup3',
    wheres => $$
        unnest(array_uniq(b)) AS b,
        unnest(array_uniq(d)) AS d
    $$
);

select create_rollup(
    'arrtest',
    'arrtest_rollup4',
    wheres => $$
        a,
        unnest(array_uniq(b)) AS b,
        unnest(array_uniq(d)) AS d,
        c
    $$
);

select create_rollup(
    'arrtest',
    'arrtest_rollup5',
    wheres => $$
        unnest(array_uniq(b)) AS b,
        unnest(array_uniq(d)) AS d
    $$,
    distincts => $$
        a,
        c
    $$
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
