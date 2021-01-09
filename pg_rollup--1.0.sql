\echo Use "CREATE EXTENSION pg_rollup" to load this file. \quit


CREATE OR REPLACE FUNCTION assert_rollup(rollup_name REGCLASS)
RETURNS VOID AS $$
    sql = f'select * from {rollup_name}_view except select * from {rollup_name};';
    res = plpy.execute(sql)
    assert len(res)==0
    sql = f'select * from {rollup_name} except select * from {rollup_name}_view;';
    res = plpy.execute(sql)
    assert len(res)==0
$$
LANGUAGE plpython3u
RETURNS NULL ON NULL INPUT;


CREATE OR REPLACE FUNCTION create_rollup(
    table_name  REGCLASS,
    rollup_name TEXT,
    wheres TEXT DEFAULT '',
    distincts TEXT DEFAULT ''
    )
RETURNS VOID AS $$
    import pg_rollup
    import re
    import collections

    def process_list(ks, error_str):
        '''
        converts postgresql strings of either of the following forms
            value
            value AS name
        into python pg_rollup.Key values with a value, type, and name;
        this function is responsible for the vast majority of error handling,
        and the error messages could still probably be improved considerably
        '''
        ret = []
        for k in ks:
            # extract the value from the input,
            # and if the 'AS' syntax is used, also extract the name
            l,_,r = k.rpartition('AS ')
 
            # case when AS does not apper in the input string
            if l=='': 
                value = k
                name = None

            # when AS appears in the input string,
            # but the contents to the right of AS are not a valid column name;
            # we treat the input as not using the AS syntax
            elif not re.match(r'^\w+$', r.strip()): 
                value = k
                name = None

            # the AS syntax was used correctly
            else:
                value = l.strip()
                name = r.strip()

            # extract the type and a default name from the value
            sql = f'select {value} from {table_name} limit 1;'
            res = plpy.execute(sql)
            t_oid = res.coltypes()[0]
            name = name or res.colnames()[0]

            sql = f'select typname from pg_type where oid={t_oid} limit 1;'
            type = plpy.execute(sql)[0]['typname']

            # if the name has a ? inside of it, it will not be a valid name, so we through an error;
            # this occurs when no name is specified, and postgresql cannot infer a good name for the column
            if '?' in name:
                plpy.error(f'invalid name for {error_str}: {k}, consider using the syntax: {k} AS column_name')

            # FIXME:
            unnest = 'unnest' in value

            # the value/type/name have been successfully extracted,
            # and so we add them to the ret variable
            ret.append(pg_rollup.Key(value,type,name,unnest))

        # if there are any duplicate names, throw an error
        names = [k.name for k in ret]
        duplicate_names = [item for item, count in collections.Counter(names).items() if count > 1]
        if len(duplicate_names) > 0:
            plpy.error(f'duplicate names in {error_str}: '+duplicate_names)

        # everything worked without error, so return
        return ret

    wheres_list = re.split(r',\s*(?![^()]*\))', wheres)
    distincts_list = re.split(r',\s*(?![^()]*\))', distincts)

    if len(wheres_list)==1 and wheres_list[0].strip()=='':
        wheres_list=[]
    if len(distincts_list)==1 and distincts_list[0].strip()=='':
        distincts_list=[]

    # constuct the sql statements for generating the rollup, and execute them
    # the error checking above should guarantee that there are no SQL errors below
    sqls = pg_rollup.Rollup(
        table_name,
        [],
        rollup_name,
        process_list(wheres_list, 'key'),
        process_list(distincts_list, 'distinct'),
        #use_hll,
        #use_raw,
        #null_support = True
    ).create()
    for s in sqls:
        plpy.execute(s)
$$
LANGUAGE plpython3u
RETURNS NULL ON NULL INPUT;


/*
 * FIXME:
 * automatically deleting rollups is not currently working
 *
 
CREATE OR REPLACE FUNCTION drop_rollup(rollup_name REGCLASS)
RETURNS VOID AS $$
    import pg_rollup
    sql = pg_rollup.drop_rollup_str(rollup_name)
    plpy.execute(sql)
$$
LANGUAGE plpython3u
RETURNS NULL ON NULL INPUT;
*/

