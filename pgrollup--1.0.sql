\echo Use "CREATE EXTENSION pgrollup" to load this file. \quit

CREATE OR REPLACE FUNCTION raw_parser(query text)
RETURNS text
AS 'MODULE_PATHNAME','raw_parser_sql'
LANGUAGE C IMMUTABLE STRICT;

--------------------------------------------------------------------------------

CREATE TABLE algebra (
    id                      SERIAL PRIMARY KEY,
    name                    TEXT NOT NULL,
    agg                     TEXT NOT NULL,
    type                    TEXT NOT NULL,
    zero                    TEXT NOT NULL,
    plus                    TEXT NOT NULL,
    negate                  TEXT,
    view                    TEXT
);

CREATE TABLE viewop (
    id                      SERIAL PRIMARY KEY,
    id_algebra              INTEGER REFERENCES algebra(id),
    op                      TEXT NOT NULL,
    value                   TEXT NOT NULL
);

/*
 * postgres-native algebra
 *
 * FIXME: the following aggregate functions could be implemented, but are not
 * bit_and
 * bit_or
 */

INSERT INTO algebra
    (name           ,agg                            ,type       ,zero                       ,plus                           ,negate ,view)
    VALUES
    ('count'        ,'count(x)'                     ,'INTEGER'  ,'0'                        ,'count(x)+count(y)'            ,'-x'   ,'x'),
    ('sum'          ,'sum(x)'                       ,'x'        ,'0'                        ,'sum(x)+sum(y)'                ,'-x'   ,'x'),
    ('min'          ,'min(x)'                       ,'x'        ,'null'                     ,'least(min(x),min(y))'         ,NULL   ,'x'),
    ('max'          ,'max(x)'                       ,'x'        ,'null'                     ,'greatest(max(x),max(y))'      ,NULL   ,'x'),
    ('bool_and'     ,'bool_and(x)'                  ,'BOOL'     ,'TRUE'                     ,'bool_and(x) and bool_and(y)'  ,NULL   ,'x'),
    ('bool_or'      ,'bool_or(x)'                   ,'BOOL'     ,'FALSE'                    ,'bool_or(x) or bool_or(y)'     ,NULL   ,'x');

INSERT INTO algebra
    (name,agg,type,zero,plus,negate,view)
    VALUES
    ( 'avg'
    , 'avg(x)'
    , 'FLOAT'
    , 'null'
    , 'avg(x)*(count(x)/(count(x)+count(y))::FLOAT)+avg(y)*(count(y)/(count(x)+count(y))::FLOAT)'
    , 'x'
    , 'x'
    ),
    ( 'var_pop'
    , 'var_pop(x)'
    , 'FLOAT'
    , 'null'
    , '(count(x)/(count(x)+count(y)::FLOAT))*(var_pop(x)+(avg(x) - count(x)/(count(x)+count(y)::FLOAT)*avg(x) - count(y)/(count(x)+count(y)::FLOAT)*avg(y))^2) + (count(y)/(count(x)+count(y)::FLOAT))*(var_pop(y)+(avg(y) - count(y)/(count(x)+count(y)::FLOAT)*avg(y) - count(x)/(count(x)+count(y)::FLOAT)*avg(x))^2)'
    , 'x'
    , 'x'
    ),
    ( 'var_samp'
    , 'var_samp(x)'
    , 'FLOAT'
    , 'null'
    , 'null'
    , 'x'
    , 'CASE WHEN count(x) > 1 THEN var_pop(x)*count(x)/(count(x)-1) ELSE var_pop(x) END'
    ),
    ( 'variance'
    , 'variance(x)'
    , 'FLOAT'
    , 'null'
    , 'null'
    , 'x'
    , 'CASE WHEN count(x) > 1 THEN var_pop(x)*count(x)/(count(x)-1) ELSE var_pop(x) END'
    ),
    ( 'stddev'
    , 'stddev(x)'
    , 'FLOAT'
    , 'null'
    , 'null'
    , 'x'
    , 'CASE WHEN count(x) > 1 THEN sqrt(var_pop(x)*count(x)/(count(x)-1)) ELSE stddev(x) END'
    ),
    ( 'stddev_samp'
    , 'stddev_samp(x)'
    , 'FLOAT'
    , 'null'
    , 'null'
    , 'x'
    , 'CASE WHEN count(x) > 1 THEN sqrt(var_pop(x)*count(x)/(count(x)-1)) ELSE stddev_samp(x) END'
    ),
    ( 'stddev_pop'
    , 'stddev_pop(x)'
    , 'FLOAT'
    , 'null'
    , 'null'
    , 'x'
    , 'CASE WHEN count(x) > 1 THEN sqrt(var_pop(x)) ELSE stddev_pop(x) END'
    );

--------------------------------------------------------------------------------

/* 
 * Algebras defined in external libraries goes here.
 * For each library, we first check if the extension is installed.
 * Then, we only define the library specific code if the extension actually is installed.
 * This ensures that the pgrollup library can work even when these optional dependencies are not met.
 *
 * NOTE:
 * The following libraries have not been included as dependencies for this project,
 * but they might be included in the future.
 *
 * Apache MADLib: https://madlib.apache.org/docs/master/group__grp__sketches.html
 * Provides sketch datastructures, but they have no union, and I'm not sure how to install it
 *
 * https://github.com/ozturkosu/cms_topn
 * doesn't build on postgres:12,10; appears abandoned
 */

/*
 * https://github.com/citusdata/postgresql-topn
 */
do $do$
DECLARE
    has_extension BOOLEAN;
BEGIN
SELECT true FROM pg_extension INTO has_extension WHERE extname='topn';
IF has_extension THEN

INSERT INTO algebra
    (name,agg,type,zero,plus,negate,view)
    VALUES
    ('topn_add_agg'
    ,'topn_add_agg(x)'
    ,'JSONB'
    ,$$'{}'$$
    ,'topn_union(topn_add_agg(x),topn_add_agg(y))'
    ,NULL
    ,'x'
    );

END IF;
END
$do$ language 'plpgsql';

/*
 * https://github.com/citusdata/postgresql-hll
 */
do $do$
DECLARE
    has_extension BOOLEAN;
BEGIN
SELECT true FROM pg_extension INTO has_extension WHERE extname='hll';
IF has_extension THEN

INSERT INTO algebra
    (name,agg,type,zero,plus,negate,view)
    VALUES
    ('hll_add_agg'
    ,'hll_add_agg(x)'
    ,'hll'
    ,'hll_empty()'
    ,'hll_add_agg(x)||hll_add_agg(y)'
    ,NULL
    ,'x'
    ),
    ('hll_count'
    ,'hll_add_agg(hll_hash_any(x))'
    ,'hll'
    ,'hll_empty()'
    ,'hll_count(x)||hll_count(y)'
    ,NULL
    ,'round(hll_cardinality(hll_count(x)))'
    );


CREATE FUNCTION hll_count_sfunc(a hll, b anyelement) RETURNS hll AS $$ SELECT hll_add(a, hll_hash_any(b)); $$ LANGUAGE SQL;
CREATE FUNCTION hll_count_final(a hll) RETURNS BIGINT AS $$ SELECT round(hll_cardinality(a)); $$ LANGUAGE SQL;
CREATE AGGREGATE hll_count (anyelement)
(
    sfunc = hll_count_sfunc,
    stype = hll,
    finalfunc = hll_cardinality
);

END IF;
END
$do$ language 'plpgsql';

/*
 * https://pgxn.org/dist/datasketches/
 */

do $do$
DECLARE
    has_extension BOOLEAN;
BEGIN
SELECT true FROM pg_extension INTO has_extension WHERE extname='datasketches';
IF has_extension THEN

CREATE OR REPLACE FUNCTION kll_float_sketch_union(a kll_float_sketch, b kll_float_sketch) RETURNS kll_float_sketch AS $$
    select kll_float_sketch_merge(sketch) from (select a as sketch union all select b) t;
$$ LANGUAGE 'sql' IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION req_float_sketch_union(a req_float_sketch, b req_float_sketch) RETURNS req_float_sketch AS $$
    select req_float_sketch_merge(sketch) from (select a as sketch union all select b) t;
$$ LANGUAGE 'sql' IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION frequent_strings_sketch_union(a frequent_strings_sketch, b frequent_strings_sketch) RETURNS frequent_strings_sketch AS $$
    select frequent_strings_sketch_merge(9,sketch) from (select a as sketch union all select b) t;
$$ LANGUAGE 'sql' IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION theta_sketch_empty() RETURNS theta_sketch AS
$$
    select theta_sketch_intersection(theta_sketch_build(0::INTEGER),theta_sketch_build(1::INTEGER));
$$ LANGUAGE 'sql' IMMUTABLE PARALLEL SAFE;


INSERT INTO algebra
    (name,agg,type,zero,plus,negate,view)
    VALUES
    ('kll_float_sketch_build'
    ,'kll_float_sketch_build(x)'
    ,'kll_float_sketch'
    ,'null'
    ,'kll_float_sketch_union(kll_float_sketch_build(x),kll_float_sketch_build(y))'
    ,NULL
    ,'x'
    ),
    ('req_float_sketch_build'
    ,'req_float_sketch_build(x)'
    ,'req_float_sketch'
    ,'null'
    ,'req_float_sketch_union(req_float_sketch_build(x),req_float_sketch_build(y))'
    ,NULL
    ,'x'
    ),
    ('frequent_strings_sketch_build'
    ,'frequent_strings_sketch_build(x)'
    ,'frequent_strings_sketch'
    ,'null'
    ,'frequent_strings_sketch_union(frequent_strings_sketch_build(x),frequent_strings_sketch_build(y))'
    ,NULL
    ,'x'
    ),
    
    -- FIXME: 
    -- the datasketches library implements an intersection function, but no negate function;
    -- this means that deleting from these rollups won't work, but it should
    ('theta_sketch_build'
    ,'theta_sketch_build(x)'
    ,'theta_sketch'
    ,'null'
    ,'theta_sketch_union(theta_sketch_build(x),theta_sketch_build(y))'
    ,NULL
    ,'x'
    ),
    ('theta_sketch_distinct'
    ,'theta_sketch_build(x)'
    ,'theta_sketch'
    ,'null'
    ,'theta_sketch_union(theta_sketch_distinct(x),theta_sketch_distinct(y))'
    ,NULL
    ,'theta_sketch_get_estimate(theta_sketch_distinct(x))'
    ),
 
    ('hll_sketch_build'
    ,'hll_sketch_build(x)'
    ,'hll_sketch'
    ,'null'
    ,'hll_sketch_union(hll_sketch_build(x),hll_sketch_build(y))'
    ,NULL
    ,'x'
    ),
    ('hll_sketch_distinct'
    ,'hll_sketch_build(x)'
    ,'hll_sketch'
    ,'null'
    ,'hll_sketch_union(hll_sketch_distinct(x),hll_sketch_distinct(y))'
    ,NULL
    ,'hll_sketch_get_estimate(hll_sketch_distinct(x))'
    ),

    -- FIXME: cpc generates an error, but the resulting values still seem to be okay
    ('cpc_sketch_build'
    ,'cpc_sketch_build(x)'
    ,'cpc_sketch'
    ,'null'
    ,'cpc_sketch_union(cpc_sketch_build(x),cpc_sketch_build(y))'
    ,NULL
    ,'x'
    ),
    ('cpc_sketch_distinct'
    ,'cpc_sketch_build(x)'
    ,'cpc_sketch'
    ,'null'
    ,'cpc_sketch_union(cpc_sketch_distinct(x),cpc_sketch_distinct(y))'
    ,NULL
    ,'cpc_sketch_get_estimate(cpc_sketch_distinct(x))'
    );

END IF;
END
$do$ language 'plpgsql';

/*
 * https://github.com/tvondra/tdigest
 */  
do $do$
DECLARE
    has_extension BOOLEAN;
BEGIN
SELECT true FROM pg_extension INTO has_extension WHERE extname='tdigest';
IF has_extension THEN

CREATE OR REPLACE FUNCTION tdigest_union(a tdigest, b tdigest) RETURNS tdigest AS $$
    select tdigest(sketch) from (select a as sketch union all select b) t;
$$ LANGUAGE 'sql' IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION tdigest_getpercentile(a tdigest, p float) RETURNS double precision AS $$
    select tdigest_percentile(sketch, p) from (select a as sketch) t;
$$ LANGUAGE 'sql' IMMUTABLE PARALLEL SAFE;

-- FIXME:
-- we should be able to get rid of the tdigest_getpercentile function;
-- but this would require adjusting the algebra code to parse the parameters
INSERT INTO algebra
    (name,agg,type,zero,plus,negate,view)
    VALUES
    ('tdigest'
    ,'tdigest(x)'
    ,'tdigest'
    ,'null'
    ,'tdigest_union(tdigest(x),tdigest(y))'
    ,NULL
    ,'x'
    );

END IF;
END
$do$ language 'plpgsql';

/*
 * https://github.com/mikeizbicki/vector
 * NOTE:
 * forked from https://github.com/ankane/pgvector , but they haven't merged changes upstream
 * FIXME:
 * the aggregate functions require renaming because pgrollup can't handle avg/sum functions with different return types yet
 */  
do $do$
DECLARE
    has_extension BOOLEAN;
BEGIN
SELECT true FROM pg_extension INTO has_extension WHERE extname='vector';
IF has_extension THEN

CREATE AGGREGATE vector_sum (vector)
(
    sfunc = vector_add,
    stype = vector,
    combinefunc = vector_add,
    parallel = safe
);

CREATE AGGREGATE vector_avg (vector)
(
    sfunc = vector_avg_accum,
    stype = vector_avg_accum,
    finalfunc = vector_avg_final,
    combinefunc = vector_avg_combine,
    parallel = safe
);

INSERT INTO algebra
    (name,agg,type,zero,plus,negate,view)
    VALUES
    ('vector_sum'
    ,'vector_sum(x)'
    ,'vector'
    ,'null'
    ,'vector_sum(x)+vector_sum(y)',
    '-x',
    'x'
    ),
    ('vector_avg'
    ,'vector_avg(x)'
    ,'vector'
    ,'null'
    ,'vector_avg(x)*((count(x)/(count(x)+count(y))::REAL)::REAL)+vector_avg(y)*((count(y)/(count(x)+count(y))::REAL)::REAL)'
    ,'-x'
    ,'x'
    );

END IF;
END
$do$ language 'plpgsql';

--------------------------------------------------------------------------------

CREATE TABLE pgrollup_rollups (
    rollup_name TEXT NOT NULL,
    table_alias TEXT NOT NULL,
    table_name TEXT NOT NULL,
    event_id_sequence_name TEXT,
    rollup_column TEXT,
    sql TEXT,
    mode TEXT NOT NULL,
    last_aggregated_id BIGINT DEFAULT 0,
    PRIMARY KEY (rollup_name,table_alias)
);

CREATE TABLE pgrollup_settings (
    name TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
INSERT INTO pgrollup_settings (name,value) VALUES
    ('default_mode','trigger'),
    ('cron_max_rollup_size','100000');

/*
 * Whenever the source table for a rollup is dropped,
 * the rollup should be deleted as well.
 * This trigger ensures the rollup gets dropped.
 *
 * FIXME:
 * This trigger doesn't seem to fire when a temporary table is automatically dropped at the end of a session.
 */
CREATE OR REPLACE FUNCTION pgrollup_event_drop_f()
RETURNS event_trigger AS $$
DECLARE
    obj record;
    rollup record;
BEGIN
    IF tg_tag LIKE 'DROP%'
    THEN
        FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
        LOOP
            FOR rollup IN SELECT * FROM pgrollup_rollups WHERE table_name=obj.object_name
            LOOP
                PERFORM drop_rollup(rollup.rollup_name);
            END LOOP;
        END LOOP;
    END IF;
END;
$$ LANGUAGE plpgsql;
CREATE EVENT TRIGGER pgrollup_drop_trigger ON sql_drop EXECUTE PROCEDURE pgrollup_event_drop_f();


/*
 * Whenever a materialized view is created, this event replaces it with a rollup.
 */
CREATE OR REPLACE FUNCTION pgrollup_from_matview_event()
RETURNS event_trigger AS $$
DECLARE
    obj record;
    rollup record;
BEGIN
    IF tg_tag='CREATE MATERIALIZED VIEW'
    THEN
        FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands()
        LOOP
            PERFORM pgrollup_from_matview(obj.object_identity);
        END LOOP;
    END IF;
END;
$$ LANGUAGE plpgsql;

/*
 * Manual rollup functions modified from
 * https://www.citusdata.com/blog/2018/06/14/scalable-incremental-data-aggregation/
 *
 * The incremental_rollup_window function has been modified so that it doesn't
 * rollup the entire table at once, but in smaller chunks;
 * this is useful for rolling up large tables incrementally that have already been created
 */
CREATE FUNCTION incremental_rollup_window(
    rollup_name text, 
    table_alias text,
    max_rollup_size bigint default 4611686018427387904, -- 2**62
    force_safe boolean default true,
    OUT window_start bigint,
    OUT window_end bigint
)
RETURNS record
LANGUAGE plpgsql
AS $function$
DECLARE
    table_to_lock regclass;
BEGIN
    RAISE DEBUG 'incremental_rollup_window';
    /*
     * Perform aggregation from the last aggregated ID + 1 up to the last committed ID.
     * We do a SELECT .. FOR UPDATE on the row in the rollup table to prevent
     * aggregations from running concurrently.
     */
    -- FIXME:
    -- the COALESCEs here are assuming that the sequence is positive;
    -- that's the default value, but these can be changed;
    -- the *REALLY* correct thing to do here is to extract the minimum value from the sequence and use that
    SELECT table_name, COALESCE(last_aggregated_id,0)+1, LEAST(COALESCE(last_aggregated_id,0)+max_rollup_size+1,pg_sequence_last_value(event_id_sequence_name))
    INTO table_to_lock, window_start, window_end
    FROM pgrollup_rollups
    WHERE pgrollup_rollups.rollup_name = incremental_rollup_window.rollup_name 
      AND pgrollup_rollups.table_alias = incremental_rollup_window.table_alias 
    FOR UPDATE;
    RAISE DEBUG 'incremental_rollup_window 2';

    IF NOT FOUND THEN
        RAISE 'rollup ''%'' is not in pgrollup_rollups', rollup_name;
    END IF;

    IF window_end IS NULL THEN
        /* sequence was never used */
        window_end := 0;
        RETURN;
    END IF;

    /*
     * Play a little trick: We very briefly lock the table for writes in order to
     * wait for all pending writes to finish. That way, we are sure that there are
     * no more uncommitted writes with a identifier lower or equal to window_end.
     * By throwing an exception, we release the lock immediately after obtaining it
     * such that writes can resume.
     */
    IF force_safe THEN
        BEGIN
            EXECUTE format('LOCK %s IN EXCLUSIVE MODE', table_to_lock);
            RAISE 'release table lock';
        EXCEPTION WHEN OTHERS THEN
        END;
    END IF;
    RAISE DEBUG 'incremental_rollup_window 3';

    /*
     * Remember the end of the window to continue from there next time.
     */
    UPDATE pgrollup_rollups SET last_aggregated_id = window_end
    WHERE pgrollup_rollups.rollup_name = incremental_rollup_window.rollup_name
      AND pgrollup_rollups.table_alias = incremental_rollup_window.table_alias;
END;
$function$;


CREATE FUNCTION do_rollup(
    rollup_name text default null,
    table_alias text default null,
    max_rollup_size bigint default 4611686018427387904, -- 2**62
    force_safe boolean default true,
    delay_seconds integer default 0
)
RETURNS TABLE (
    _rollup_name TEXT,
    _table_alias TEXT,
    start_id BIGINT,
    end_id BIGINT
)
LANGUAGE plpgsql
AS $function$
DECLARE
    sql_command text;
    obj record;
    ret record;
    mode text;
    start_id bigint;
    end_id bigint;
    event_id_sequence_name TEXT;
BEGIN
    RAISE DEBUG 'do_rollup';

    -- if no rollup_name is provided,
    -- then we'll do a rollup on all of the tables
    IF rollup_name IS NULL THEN
        FOR obj IN SELECT * FROM pgrollup_rollups WHERE event_id_sequence_name IS NOT NULL
        LOOP
            RETURN QUERY SELECT * FROM do_rollup(
                obj.rollup_name,
                obj.table_alias,
                do_rollup.max_rollup_size,
                do_rollup.force_safe,
                do_rollup.delay_seconds
                );
        END LOOP;
        RETURN;
    END IF;

    -- if no table_alias is provided,
    -- then we'll do a rollup on all of the table_aliases
    IF table_alias IS NULL THEN
        FOR obj IN SELECT * FROM pgrollup_rollups WHERE pgrollup_rollups.rollup_name=do_rollup.rollup_name
        LOOP
            RETURN QUERY SELECT * FROM do_rollup(
                do_rollup.rollup_name,
                obj.table_alias,
                do_rollup.max_rollup_size,
                do_rollup.force_safe,
                do_rollup.delay_seconds
                );
        END LOOP;
        RETURN;
    END IF;

    -- return null if do_rollup was called on a rollup without a sequence
    SELECT pgrollup_rollups.event_id_sequence_name
    INTO event_id_sequence_name
    FROM pgrollup_rollups
    WHERE pgrollup_rollups.rollup_name=do_rollup.rollup_name
      AND pgrollup_rollups.table_alias=do_rollup.table_alias;
    IF event_id_sequence_name IS NULL THEN
        RAISE WARNING 'event_id_sequence_name is null';
        RETURN; 
    ELSE
        RAISE DEBUG 'do_rollup: event_id_sequence_name is %', event_id_sequence_name;
    END IF;

    /* sleeping is how cron ensures that the jobs are staggered in time */
    PERFORM pg_sleep(delay_seconds);

    /* determine which page views we can safely aggregate */
    SELECT window_start, window_end INTO start_id, end_id
    FROM incremental_rollup_window(rollup_name,table_alias,max_rollup_size,force_safe);
    RAISE DEBUG 'do_rollup: incremental_rollup_window done; start_id=%, end_id=%', start_id, end_id;

    /* exit early if there are no new rows to aggregate */
    IF start_id > end_id OR start_id IS NULL OR end_id IS NULL THEN 
        RAISE DEBUG 'no new rows to aggregate';
        RETURN QUERY SELECT rollup_name,table_alias,start_id,end_id;
        RETURN;
    END IF;

    /* this is the new code that gets the rollup command from the table
     * and executes it */
    RAISE DEBUG 'do_rollup: execute sql';
    SELECT pgrollup_rollups.sql 
    INTO sql_command
    FROM pgrollup_rollups 
    WHERE pgrollup_rollups.rollup_name = do_rollup.rollup_name;

    RAISE DEBUG 'do_rollup; sql_command=%', sql_command;
    EXECUTE 'select '||sql_command||'($1,$2)' USING start_id,end_id;

    -- return
    RETURN QUERY SELECT rollup_name,table_alias,start_id,end_id;
END;
$function$;


CREATE OR REPLACE FUNCTION pgrollup_from_matview(
    view_name REGCLASS,
    mode TEXT DEFAULT NULL,
    partition_clause TEXT DEFAULT NULL,
    dry_run BOOLEAN DEFAULT FALSE
)
RETURNS VOID AS $$
    sql="""
    SELECT definition,tablespace
    FROM pg_matviews
    WHERE matviewname='"""+view_name+"""';
    """
    rows = plpy.execute(sql)
    view_definition = rows[0]['definition']
    tablespace = rows[0]['tablespace']

    if not dry_run:
        sql = """
        DROP MATERIALIZED VIEW """+view_name+""";
        """
        plpy.execute(sql)

    query = """
    CREATE INCREMENTAL MATERIALIZED VIEW """+view_name+""" AS (
    """+view_definition[:-1]+"""
    );
    """
    sql = ("""
    SELECT pgrollup_parse("""
        +"""$pgrollup_parse$"""+query+"""$pgrollup_parse$,"""
        +str(dry_run)+","
        +('NULL' if not mode else "'"+mode+"'")+","
        +('NULL' if not tablespace else "'"+tablespace+"'")+','
        +('NULL' if not partition_clause else "'"+partition_clause+"'")
    +""")""")
    plpy.execute(sql)
$$
LANGUAGE plpython3u;


CREATE OR REPLACE FUNCTION pgrollup_parse(
    text TEXT,
    dry_run BOOLEAN DEFAULT FALSE,
    mode TEXT DEFAULT NULL,
    tablespace TEXT DEFAULT NULL,
    partition_clause TEXT DEFAULT NULL
)
RETURNS VOID AS $$
    import pgrollup.parsing
    cmds = pgrollup.parsing.parse_create(text)
    for cmd in cmds:
        sql = f'''
        SELECT create_rollup_internal(
            $1,
            columns => $2,
            joininfos => $3,
            groups => $4,
            where_clause => $5,
            having_clause => $6,
            dry_run => $7,
            mode => $8,
            tablespace => $9,
            partition_clause => $10
        ) as result;'''
        plan = plpy.prepare(sql,[
            'text',
            'text[]',
            'json',
            'text[]',
            'text',
            'text',
            'boolean',
            'text',
            'text',
            'text',
            ])
        result = plpy.execute(plan,[
            cmd['rollup_name'],
            cmd['columns'],
            cmd['joininfos'],
            cmd['groups'],
            cmd['where_clause'],
            cmd['having_clause'],
            dry_run,
            mode,
            tablespace,
            partition_clause
            ])
$$
LANGUAGE plpython3u;


CREATE OR REPLACE FUNCTION create_rollup_internal(
    rollup_name TEXT,
    columns TEXT[],
    joininfos JSON DEFAULT '[]',
    groups TEXT[] DEFAULT NULL,
    where_clause TEXT DEFAULT NULL,
    having_clause TEXT DEFAULT NULL,
    tablespace TEXT DEFAULT NULL,
    partition_clause TEXT DEFAULT NULL,
    mode TEXT DEFAULT NULL,
    dry_run BOOLEAN DEFAULT FALSE
    )
RETURNS TEXT AS $$
    import pgrollup
    import pgrollup.parsing_functions
    import re
    import collections
    import json

    global joininfos
    joininfos = json.loads(joininfos)

    def get_type(expr):
        '''
        helper funcions that returns the type of expr
        '''
        sql = (f'''
            select {expr}
            '''
            +
            ''.join([
            '''
            ''' + joininfo['join_type'] + ' ' + joininfo['table_name'] + ' AS ' + joininfo['table_alias'] + ' ' + joininfo['condition']
            for joininfo in joininfos
            ])
            +
            '''
            limit 1;
            ''')
        res = plpy.execute(sql)
        t_oid = res.coltypes()[0]
        sql = f'select typname,typlen from pg_type where oid={t_oid} limit 1;'
        row = plpy.execute(sql)[0]
        return row

    def get_nullable(expr):
        if "'" in expr:
            return True
        expr = re.sub(r'^[a-zA-Z0-9_]+\.', '', expr)
        plpy.debug(f'expr={expr}')
        for joininfo in joininfos:
            sql=f'''
            SELECT not pg_attribute.attnotnull AS nullable
            FROM pg_class
            JOIN pg_attribute ON pg_attribute.attrelid = pg_class.oid
            WHERE pg_class.relname = '{joininfo['table_name']}'
              AND pg_attribute.attname ilike '{expr}';
            '''
            attrs = list(plpy.execute(sql))
            if len(attrs) > 0:
                return attrs[0]['nullable']
        return True


    # get a list of all algebras
    sql = f'select * from algebra;'
    rows = plpy.execute(sql)
    all_algebras = list(rows)

    # if no mode provided, calculate the default mode
    global mode
    if mode is None:
        mode = plpy.execute("select value from pgrollup_settings where name='default_mode';")[0]['value'];
        if mode is None:
            mode = 'trigger'

    # if no tablespace provided, calculate the default tablespace
    global tablespace
    if tablespace is None:
        # FIXME: the default tablespace is not guaranteed to be "pg_default"
        tablespace_name = 'pg_default'
    else:
        tablespace_name = tablespace

    # extract a list of wheres and rollups from the input parameters
    global groups
    if groups is None:
        groups=[]

    groups_list = []
    for value,name in groups:
        groups_list.append(pgrollup.Key(value,get_type(value),name,None,get_nullable(value)))

    # columns_view_list contains the columns that will be included in the created view
    columns_view_list = []
    raw_columns = []
    columns_minus_groups = []
    new_groups = []
    for value,name in columns:
        value_groups = [ value for value,name in groups]
        name_groups = [ name for value,name in groups]
        name_groups += [ joininfos[0]['table_name']+'.'+name for value,name in groups ]
        if '('+value+')' not in value_groups and value not in value_groups and name not in name_groups and joininfos[0]['table_name']+'.'+name not in value_groups and joininfos[0]['table_name']+'.'+name not in name_groups:
            columns_minus_groups.append((value,name))
        else:
            new_groups.append((value,name))

    groups_list = []
    for value,name in new_groups:
        groups_list.append(pgrollup.Key(value,get_type(value),name,None,get_nullable(value)))
    for value,name in groups:
        value_groups = [ value for value,name in new_groups]
        valuep_groups = [ '('+value+')' for value,name in new_groups]
        name_groups = [ name for value,name in new_groups]
        if '('+value+')' not in value_groups and value not in valuep_groups and value not in value_groups and name not in name_groups and joininfos[0]['table_name']+'.'+name not in value_groups and joininfos[0]['table_name']+'.'+name not in name_groups:
            groups_list.append(pgrollup.Key(value,get_type(value),name,None,get_nullable(value)))

    for value,name in columns_minus_groups:
        value_substitute_views = pgrollup.parsing_functions.substitute_views(value, all_algebras)
        deps, value_view = pgrollup.parsing_functions.extract_algebras(value_substitute_views, all_algebras)
        columns_view_list.append(pgrollup.ViewKey(value_view,name))
        for dep in deps:
            raw_columns.append(dep)

    # columns_raw_list contains the columns that will be included in the raw table
    columns_raw_list = []
    raw_columns = sorted(list(set(raw_columns)))
    for value in raw_columns:
        # extract key info
        name = '"'+value+'"'
        algebra = value[:value.find("(")]
        expr = value[value.find("(")+1:value.rfind(")")]
        type = get_type(expr)
        nullable = get_nullable(value)

        plpy.debug(f'nullable={nullable}')
        plpy.debug(f'len(raw_columns)={len(raw_columns)}')

        # get the algebra dictionary and key
        sql = f"select * from algebra where name='{algebra}';"
        res = list(plpy.execute(sql))
        if len(res)==1:
            algebra_dictionary = res[0]
            key = pgrollup.Key(expr,type,name,algebra_dictionary,nullable)
        else:
            plpy.error(f'algbera {algebra} not found in the algebra table')

        # add column info
        columns_raw_list.append(key)

        # add dependencies to raw_columns if they are not present
        def extract_functions(text):
            functions = []
            for match in re.finditer(r'\b([a-zA-Z0-9_]+)\([xy]\)', text):
                functions.append(match.group(1))
            return functions

        deps = extract_functions(algebra_dictionary['plus'])
        if algebra_dictionary['plus'].strip().lower() == 'null':
            deps += extract_functions(algebra_dictionary['view'])

        for dep in deps:
            matched = False
            if f'{dep}({expr})' not in raw_columns:
                raw_columns.append(f'{dep}({expr})')

    # if there are any duplicate names in columns_raw_list, throw an error;
    # this should never happen, and is simply a consistency check
    names = [k.name for k in columns_raw_list]
    duplicate_names = [item for item, count in collections.Counter(names).items() if count > 1]
    if len(duplicate_names) > 0:
        plpy.warning('names='+str(names))
        plpy.error(f'duplicate names in columns: '+str(duplicate_names))

    # check if the table is temporary
    is_temp = False
    for joininfo in joininfos:
        sql = f"SELECT relpersistence='t' as is_temp FROM pg_class where relname='{joininfo['table_name']}'"
        is_temp = is_temp or plpy.execute(sql)[0]['is_temp']

    # compute the information needed for manual/cron rollups
    for joininfo in joininfos:
        rollup_column = joininfo.get('rollup_column')
        table_name = joininfo['table_name']
        if rollup_column:
            event_id_sequence_name = f"{table_name}_{rollup_column}_seq"
        else:
            # no rollup_column was given, so we try to use the primary key
            sql=f'''
            SELECT ind_column.attname AS pk
            FROM pg_class tbl
            JOIN pg_index ind ON ind.indrelid = tbl.oid
            JOIN pg_class ind_table ON ind_table.oid = ind.indexrelid
            JOIN pg_attribute ind_column ON ind_column.attrelid = ind_table.oid
            WHERE tbl.relname = '{table_name}'
              AND ind.indisprimary;
            '''
            pks = list(plpy.execute(sql))

            event_id_sequence_name = None
            rollup_column = None
            if len(pks) == 0:
                plpy.notice(f'no primary key in table {table_name}')
            else:
                found_seq = False
                for pk in pks:
                    event_id_sequence_name = f"{table_name}_{pk['pk']}_seq"
                    rollup_column = pk['pk']
                    sql = f"SELECT relname FROM pg_class WHERE relkind = 'S' and relname='{event_id_sequence_name}';";
                    matches = list(plpy.execute(sql))
                    if len(matches) > 0:
                        found_seq = True
                        break
                if found_seq:
                    plpy.debug(f'rollup_column={rollup_column}, event_id_sequence_name={event_id_sequence_name }')
        joininfo['rollup_column'] = rollup_column
        joininfo['event_id_sequence_name'] = event_id_sequence_name

        # verify that the computed sequence exists in the db
        sql = f"SELECT relname FROM pg_class WHERE relkind = 'S' and relname='{event_id_sequence_name}';";
        matches = list(plpy.execute(sql))
        if len(matches) == 0:
            plpy.notice(f'sequence "{event_id_sequence_name}" not found in table')
            event_id_sequence_name = None
            rollup_column = None

        # display warning messages
        if joininfo.get('rollup_column') is None:
            plpy.notice(f'event_id_sequence_name={event_id_sequence_name}')
            plpy.notice('no valid sequence found for manual/cron rollups; the only available rollup type is trigger')

    # verify that there are no subqueries
    if where_clause and re.search(r'\(\s*select', where_clause, re.IGNORECASE):
        plpy.error('subqueries not allowed in the WHERE clause')
    if having_clause and re.search(r'\(\s*select', having_clause, re.IGNORECASE):
        plpy.error('subqueries not allowed in the HAVING clause')

    # constuct the sql statements for generating the rollup, and execute them
    # the error checking above should guarantee that there are no SQL errors below
    sqls = pgrollup.Rollup(
        joininfos,
        is_temp,
        tablespace_name,
        rollup_name,
        groups_list,
        columns_raw_list,
        columns_view_list,
        where_clause,
        having_clause,
        partition_clause,
    ).create()

    # set the rollup mode
    sqls += f"""
    select rollup_mode('{rollup_name}','{mode}');
    """

    # insert values into the rollup
    sqls += f"""
    select {rollup_name}_raw_reset();
    """

    if not dry_run:
        plpy.execute(sqls)
    else:
        plpy.notice('the given command would execute the following SQL code:\n\n'+sqls)
$$
LANGUAGE plpython3u;


CREATE OR REPLACE FUNCTION rollup_mode(
    rollup_name REGCLASS,
    mode TEXT
)
RETURNS VOID AS $func$
    
    plpy.debug('rollup_mode')
    sql = (f"select * from pgrollup_rollups where rollup_name='{rollup_name}'")
    rows = list(plpy.execute(sql))

    for i,pgrollup in enumerate(rows):
        plpy.debug('rollup_mode: row i='+str(i))
        if mode != 'trigger' and pgrollup['event_id_sequence_name'] is None:
            plpy.error(f'''"mode" must be 'trigger' when "event_id_sequence_name" is NULL''')

        ########################################    
        # turn off the old mode
        # NOTE:
        # we should maintain the invariant that whenever we disable the old mode,
        # the rollup tables are consistent with the underlying table;
        # this requires calling the do_rollup function for all non-trigger options,
        # which is potentially an expensive operation.
        ########################################    
        if pgrollup['mode'] == 'trigger':
            plpy.execute(f'''
                SELECT pgrollup_unsafedroptriggers__{rollup_name}__{pgrollup['table_alias']}();
                ''')

        if pgrollup['mode'] == 'cron':
            plpy.execute(f'''
                SELECT cron.unschedule('pgrollup.{rollup_name}');
                ''')
            plpy.execute(f"""
                select do_rollup('{rollup_name}','{pgrollup['table_alias']}');
                """)

        if pgrollup['mode'] == 'manual':
            plpy.execute(f"""
                select do_rollup('{rollup_name}','{pgrollup['table_alias']}');
                """)

        ########################################    
        # enter the new mode
        ########################################    
        if mode=='cron':
            sql = "SELECT value FROM pgrollup_settings WHERE name='cron_max_rollup_size';"
            cron_max_rollup_size = int(plpy.execute(sql)[0]['value'])

            # we use a "random" delay on the cron job to ensure that all of the jobs
            # do not happen at the same time, overloading the database
            sql = (f"""
                SELECT count(*) AS count
                FROM cron.job
                WHERE jobname ILIKE 'pgrollup.%';
                """)
            num_jobs = plpy.execute(sql)[0]['count']
            # delay = 13*num_jobs%60
            delay = 0
            plpy.execute(f'''
                SELECT cron.schedule(
                    'pgrollup.{rollup_name}',
                    '* * * * *',
                    $$SELECT do_rollup('{rollup_name}',max_rollup_size=>{cron_max_rollup_size},delay_seconds=>{delay});$$
                );
                ''')
            # FIXME:
            # we specify the max_rollup_size for cron-based rollups because:
            # for very expensive rollups, a full rollup can take longer than a minute to complete;
            # in heavy-insert operations, this result in a backlog of rows that need to be rolled up;
            # subsequent calls to do_rollup will have more work to do, and therefore take longer, exacerbating the problem;
            # by limiting the max_rollup_size, we prevent a quadratic growth in the backlog size;
            # if the rollup is very fast, however, we prevent it from rolling up properly;
            # the optimal number depends on many factors, such as the speed of the rollup and config params like work_mem;
            # ideally, it should be set automatically for each rollup and not hard coded.

        if mode=='trigger':
            plpy.debug('rollup_mode: trigger')

            # first we do a manual rollup to ensure that the rollup table is up to date
            if pgrollup['event_id_sequence_name'] is not None:
                plpy.debug("rollup_mode: pgrollup['event_id_sequence_name']="+str(pgrollup['event_id_sequence_name']))

                plpy.execute(f"""
                    select do_rollup('{rollup_name}','{pgrollup['table_alias']}');
                    """)

            # next we create triggers
            plpy.debug('rollup_mode: create_trigger')
            sql = 'select pgrollup_unsafecreatetriggers__'+rollup_name+'__'+pgrollup['table_alias']+'();'
            plpy.execute(sql)

    plpy.execute(f"UPDATE pgrollup_rollups SET mode='{mode}' WHERE rollup_name='{rollup_name}';")
$func$
LANGUAGE plpython3u;


CREATE OR REPLACE FUNCTION drop_rollup(rollup_name REGCLASS)
RETURNS VOID AS $$
    import pgrollup
    sql = 'select pgrollup_drop__'+rollup_name+'();'
    plpy.execute(sql)
$$
LANGUAGE plpython3u
RETURNS NULL ON NULL INPUT;



------------------------------------------------------------------------------------------------------------------------
-- the following functions are used to verify the correctness of rollup tables;
-- they are primarily intended for use in the test cases;
-- these functions are potentially slow, and so production use should be careful;
--
-- the function assert_rollup checks for exact equality between the rollup and the groundtruth;
-- it should be used on rollups with only discrete entries that are deterministically generated
--
-- the function assert_rollup_relative_error checks for approximate equality between the rollup and the groundtruth;
-- it should be used on rollups that either use floating point calculations or have internal randomness;
-- the relative_error parameter must be tuned to the accuracy guarantee provided by the rollup algebra

CREATE OR REPLACE FUNCTION assert_rollup(rollup_name REGCLASS)
RETURNS VOID AS $$
    sql = f'select * from {rollup_name}_groundtruth except select * from {rollup_name};';
    res1 = plpy.execute(sql)
    sql = f'select * from {rollup_name} except select * from {rollup_name}_groundtruth;';
    res2 = plpy.execute(sql)

    for row in res1:
        plpy.warning(f'result only in {rollup_name}_groundtruth: {str(row)}')
    for row in res2:
        plpy.warning(f'result only in {rollup_name}: {str(row)}')

    assert len(res1)==0
    assert len(res2)==0
$$ LANGUAGE plpython3u STRICT IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION relative_error(a double precision, b double precision) RETURNS DOUBLE PRECISION AS $$
    select greatest(abs(a),abs(b))/least(abs(a),abs(b))-1;
$$ LANGUAGE 'sql' STRICT IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION rollup_column_relative_error(rollup_name REGCLASS, column_name TEXT) RETURNS DOUBLE PRECISION AS $$
    sql = f'select "{column_name}" from {rollup_name};';
    res = plpy.execute(sql)
    assert len(res)==1
    val1 = res[0][column_name]

    sql = f'select "{column_name}" from {rollup_name}_groundtruth;';
    res = plpy.execute(sql)
    assert len(res)==1
    val2 = res[0][column_name]

    sql = f'select relative_error({val1},{val2}) as relative_error;';
    res = plpy.execute(sql)
    return res[0]['relative_error']
$$ LANGUAGE plpython3u STRICT IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION assert_rollup_column_relative_error(rollup_name REGCLASS, column_name TEXT, relative_error DOUBLE PRECISION) RETURNS VOID AS $$
    sql = f"select rollup_column_relative_error('{rollup_name}','{column_name}') as relative_error;";
    res = plpy.execute(sql)
    if not res[0]['relative_error'] < relative_error:
        plpy.error(f"relative_error={res[0]['relative_error']} > {relative_error}")
$$ LANGUAGE plpython3u STRICT IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION assert_rollup_relative_error(rollup_name REGCLASS, relative_error DOUBLE PRECISION) RETURNS VOID AS $$
    sql = f"select * from {rollup_name} where true limit 1;"
    res = plpy.execute(sql)
    columns = res[0].keys()
    plpy.error('columns={str(columns)}')

    sql = f"select rollup_column_relative_error('{rollup_name}','{column_name}') as relative_error;";
    res = plpy.execute(sql)
    if not res[0]['relative_error'] < relative_error:
        plpy.error(f"relative_error={res[0]['relative_error']} > {relative_error}")
$$ LANGUAGE plpython3u STRICT IMMUTABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION assert_rollup_relative_error(rollup_name REGCLASS, relative_error DOUBLE PRECISION) RETURNS VOID AS $$
    # get a list of the columns in the rollup
    sql = f"select * from {rollup_name} where true limit 1;"
    res = plpy.execute(sql)
    columns = res[0].keys()

    # count the number of columns that do not satisfy the relative_error condition
    num_bad_columns = 0
    for column_name in columns:
        sql = f"select rollup_column_relative_error('{rollup_name}','{column_name}') as relative_error;";
        res = plpy.execute(sql)
        if not res[0]['relative_error'] < relative_error:
            plpy.warning(f"column {column_name} has relative_error={res[0]['relative_error']} > {relative_error}")
            num_bad_columns+=1

    # the test case
    assert num_bad_columns==0

$$ LANGUAGE plpython3u STRICT IMMUTABLE PARALLEL SAFE;
