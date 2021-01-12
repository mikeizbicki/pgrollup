import collections
import re

Key = collections.namedtuple('Key', ['value','type','name','unnest'])


def _extract_arguments(s):
    '''
    >>> _extract_arguments('a,b,c')
    ['a', 'b', 'c']
    >>> _extract_arguments('a,(b,c)')
    ['a', '(b,c)']
    >>> _extract_arguments("a,((b,c),d)")
    ['a', '((b,c),d)']
    >>> _extract_arguments("date_trunc('day', accessed_at) AS access_day, date_trunc('day',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published")
    ["date_trunc('day', accessed_at) AS access_day", " date_trunc('day',(jsonb->'timestamp.published'->'best'->'value'->>'lo')::timestamptz) AS timestamp_published"]
    '''
    #return re.split(r',\s*(?![^()]*\))', s)

    ret = []
    last_match_index = 0
    num_paren = 0
    for i,x in enumerate(s):
        if x in '([{':
            num_paren+=1
        if x in ')]}':
            num_paren-=1
        if num_paren==0 and x==',':
            ret.append(s[last_match_index:i].strip())
            last_match_index=i+1

    if s[last_match_index:].strip() != '':
        ret.append(s[last_match_index:])
    return ret


def _add_namespace(s, namespace):
    '''
    Identify all column names in the input string s and replaces them with the specified namespace.

    FIXME:
    currently, the functions uses a heuristic formula for parsing the input SQL statement;
    this should be adjusted so that it does proper parsing

    >>> _add_namespace('url', 'new')
    'new.url'
    >>> _add_namespace('simplify_url(url)', 'new')
    'simplify_url(new.url)'
    >>> _add_namespace('metahtml.simplify_url(url)', 'new')
    'metahtml.simplify_url(new.url)'
    >>> _add_namespace("date_trunc('day',accessed_at)", 'new')
    "date_trunc('day',new.accessed_at)"
    >>> _add_namespace("date_trunc('day',accessed_at::timestamptz)", 'new')
    "date_trunc('day',new.accessed_at::timestamptz)"
    >>> _add_namespace("f('a','b',c,'d',e)", 'new')
    "f('a','b',new.c,'d',new.e)"
    '''
    # NOTE:
    # the regex below is responsible for doing the actual namespacing of names;
    # unfortunately, some name-like strings will appear within quotation marks,
    # but these are string literals and so shouldn't be namespaced;
    # string literals cannot be matched with a regular expression,
    # and so the for loop/if statement combination ensures that we only apply
    # the namespacing to strings outside the if statements
    chunks = s.split("'")
    chunks_namespaced = []
    for i,chunk in enumerate(chunks):
        if i%2==1:
            chunks_namespaced.append(chunk)
        else:
            chunks_namespaced.append(re.sub(r'([^:]|^)\s*\b([\w_]+)\b([^.(]|$)', r'\g<1>'+namespace+r'.\g<2>\g<3>', chunk))
    return "'".join(chunks_namespaced)


class Rollup:

    def __init__(self, table, columns, rollup, wheres, distincts):
        self.table = table
        self.columns = columns
        self.rollup = rollup
        self.distincts = distincts
        self.use_hll = False
        self.use_num = True
        self.null_support = True

        self.wheres = [Key(k.value, k.type, 'where_'+k.name,k.unnest) for k in wheres]

        if '.' in table:
            self.schema_name, self.table_name = table.split('.')
        else:
            self.schema_name = ''
            self.table_name = table

        if '.' in rollup:
            self.rollup_schema, self.rollup_name = rollup.split('.')
        else:
            self.rollup_schema = ''
            self.rollup_name = rollup
        self.rollup_table_name = rollup + '_raw'

        if len(distincts) == 0:
            self.use_hll = False
            self.use_num = False

        def generate_binary(wheres, ls):
            if len(wheres)==0:
                return [ l[1:] for l in ls ]
            else:
                if wheres[0].unnest:
                    return generate_binary(wheres[1:], [l + '1' for l in ls])
                else:
                    return generate_binary(wheres[1:], [l + '0' for l in ls] + [l + '1' for l in ls])
        self.binaries = generate_binary(wheres,' ')


    def create_table(self):
        '''
        >>> print(t.create_table())
        CREATE TABLE metahtml.metahtml_rollup_host_raw (
            distinct_name INTEGER NOT NULL,
            distinct_userid INTEGER NOT NULL,
            count INTEGER NOT NULL,
            where_country text,
            where_language text
        );
        '''
        return (
'''CREATE TABLE '''+self.rollup_table_name+''' ('''+
    (
    '''
    '''.join([distinct.name + '_hll hll NOT NULL,' for distinct in self.distincts])+'''
    '''
    if self.use_hll else ''
    )+
    (
    ''.join(['''
    '''+'distinct_'+distinct.name + ' INTEGER NOT NULL,' for distinct in self.distincts])+'''
    '''
    if self.use_num else ''
    )+
    '''count INTEGER NOT NULL,
    '''
    +
    (
    ''',
    '''.join([key.name + ' ' + key.type for key in self.wheres])
    if len(self.wheres)>0 else '''
    raw_true BOOLEAN DEFAULT TRUE UNIQUE NOT NULL''' )+
    '''
);''')

    
    def create_indexes_notnull(self):
        '''
        Creates indexes that enforce that the self.rollup_table_name cannot contain multiple NULLs 
        See: https://www.enterprisedb.com/postgres-tutorials/postgresql-distinct-constraint-null-allowing-only-one-null
        '''
        if len(self.wheres)>0:
            return ('\n'.join(['CREATE UNIQUE INDEX '''+self.rollup_name+'_index_'+binary+'_notnull ON '+self.rollup_table_name+' (' + ','.join(['('+key.name+' IS NULL)' if i=='0' else key.name for i,key in zip(binary,self.wheres)])+') WHERE TRUE '+' '.join(['and '+key.name+' IS NULL' for i,key in zip(binary,self.wheres) if i=='0' ])+';' for binary in self.binaries]))
        else:
            return ''


    def create_indexes_hll(self):
        '''
        -- indexes ensure fast calculation of the max on each column
        CREATE INDEX metahtml_rollup_host_index_hll ON metahtml.metahtml_rollup_host_raw (hll_cardinality(hll));
        CREATE INDEX metahtml_rollup_host_index_num ON metahtml.metahtml_rollup_host_raw (count);

        '''
        return (''.join(['''
        CREATE INDEX '''+self.rollup_name+'_index_'+distinct.name+'_hll ON '+self.rollup_table_name+' (hll_cardinality('+distinct.name+'_hll));' for distinct in self.distincts
        ]) if self.use_hll else '')


    def create_indexes_num(self):
        return (''.join(['''
        CREATE INDEX '''+self.rollup_name+'_index_'+'distinct_'+distinct.name+' ON '+self.rollup_table_name+' ('+'distinct_'+distinct.name+');' for distinct in self.distincts
        ]) if self.use_num else '')


    def create_indexes_int(self):
        if len(self.wheres)>0:
            return '''
            CREATE INDEX '''+self.rollup_name+'_index_num ON '+self.rollup_table_name+' ('+','.join([key.name for key in self.wheres])+''');
            '''
        else:
            return ''

    def create_index(self):
        return ''.join(['''
        CREATE INDEX IF NOT EXISTS ''' + self.table_name + '''_index_distinct_'''+distinct.name+'''
        ON ''' + self.table + ' ( ' + distinct.value + ' ); '
        for distinct in self.distincts
        ]) if self.use_num else ''



    def create_view(self):
        '''
        -- the view simplifies presentation of the hll columns
        CREATE VIEW metahtml.metahtml_rollup_host AS
        SELECT
            hll_cardinality(hll) AS num_distinct_url,
            count,
            host
        FROM metahtml.metahtml_rollup_host_raw;
        '''
        return ('''
        CREATE VIEW '''+self.rollup+''' AS
        SELECT'''+
            (
            '''
            '''+
            '''
            '''.join(['floor(hll_cardinality('+distinct.name+'_hll)) AS '+distinct.name+'_distinct,' for distinct in self.distincts])
            if self.use_hll else ''
            )+
            (
            '''
            '''+
            '''
            '''.join(['distinct_'+distinct.name+',' for distinct in self.distincts])
            if self.use_num else ''
            )+
            '''
            count'''+
            (
            ''',
            '''+
            ''',
            '''.join([key.name for key in self.wheres])
            if len(self.wheres)>0 else '' )+
            '''
        FROM '''+self.rollup_table_name+';')


    def create_trigger_insert(self):
        '''
        -- an insert trigger ensures that all future rows get rolled up
        CREATE OR REPLACE FUNCTION metahtml.metahtml_rollup_host_insert_f()
        RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
        BEGIN
            IF metahtml.url_host(new.url) IS NOT NULL THEN
                INSERT INTO metahtml.metahtml_rollup_host_raw (
                    hll,
                    count,
                    host
                    )
                VALUES (
                    hll_add(hll_empty(),hll_hash_text(new.url)),
                    1,
                    metahtml.url_host(new.url)
                    )
                ON CONFLICT (host)
                DO UPDATE SET
                    hll = metahtml.metahtml_rollup_host_raw.hll || excluded.hll,
                    count = metahtml.metahtml_rollup_host_raw.count +  excluded.count;
            END IF;
        RETURN NEW;
        END;
        $$;

        CREATE TRIGGER metahtml_rollup_host_insert_t
            BEFORE INSERT 
            ON metahtml.metahtml
            FOR EACH ROW
            EXECUTE PROCEDURE metahtml.metahtml_rollup_host_insert_f();
        '''
        return ('''
        CREATE OR REPLACE FUNCTION '''+self.rollup_table_name+'''_insert_f()
        RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
        BEGIN'''+'''
        '''.join([
            (('''
            IF TRUE ''' + ' '.join(['AND ' + _add_namespace(key.value,'new') + ' IS ' + ('' if i=='0' else 'NOT ') + 'NULL' for i,key in zip(binary,self.wheres) if not key.unnest]) + ''' THEN'''
            ) if self.null_support else '')
            +'''
                INSERT INTO '''+self.rollup_table_name+''' ('''+
                    (
                    '''
                    '''.join([distinct.name+'_hll,' for distinct in self.distincts])
                    if self.use_hll else ''
                    )+
                    (
                    '''
                    '''.join(['distinct_'+distinct.name+',' for distinct in self.distincts])
                    if self.use_num else ''
                    )+
                    '''
                    count'''+
                    (
                    ''',
                    '''+
                    ''',
                    '''.join([key.name for key in self.wheres])
                    if len(self.wheres)>0 else '' )+ '''
                    )
                VALUES ('''+
                    (
                    '''
                    '''+
                    '''
                    '''.join(['hll_add(hll_empty(), hll_hash_'+distinct.type+'('+_add_namespace(distinct.value,'new')+')),' for distinct in self.distincts])
                    if self.use_hll else ''
                    )+
                    (
                    '''
                    '''+
                    '''
                    '''.join(['1,' for distinct in self.distincts])
                    if self.use_num else ''
                    )+
                    '''
                    1'''+
                    (
                    ''',
                    '''+
                    ''',
                    '''.join([_add_namespace(key.value,'new') for key in self.wheres])
                    if len(self.wheres)>0 else '') + '''
                    )
                ON CONFLICT '''
                ' (' + 
                (','.join(['('+key.name+' IS NULL)' if i=='0' else key.name for i,key in zip(binary,self.wheres)]) if len(self.wheres)>0 else 'raw_true'
                )+') WHERE TRUE '+' '.join(['and '+key.name+' IS NULL' for i,key in zip(binary,self.wheres) if i=='0' ])
                +
                '''
                DO UPDATE SET'''+
                    (
                    '''
                    '''+
                    '''
                    '''.join([distinct.name+'_hll = '+self.rollup_table_name+'.'+distinct.name+'_hll || excluded.'+distinct.name+'_hll,' for distinct in self.distincts])
                    if self.use_hll else ''
                    )+
                    (
                    '''
                    '''+
                    '''
                    '''.join(['distinct_'+distinct.name+' = '+self.rollup_table_name+'.'+'distinct_'+distinct.name+''' + 
                        CASE WHEN exists(SELECT 1 FROM '''+self.table+' WHERE '+distinct.value+'='+_add_namespace(distinct.value,'new')+''' OR ('''+distinct.value+' is null and '+_add_namespace(distinct.value,'new')+' is null)'+'''LIMIT 1)
                            THEN 0
                            ELSE 1
                        END,''' for distinct in self.distincts])
                    if self.use_num else ''
                    )+
                    '''
                    count = '''+self.rollup_table_name+'''.count + excluded.count;
            '''+
            ('''END IF;
            '''
            if self.null_support else ''
            )
            for binary in self.binaries])+
            '''
            RETURN NEW;
        END;
        $$;

        CREATE TRIGGER '''+self.rollup_name+'''_insert_t_
            BEFORE INSERT 
            ON ''' + self.table + '''
            FOR EACH ROW
            EXECUTE PROCEDURE ''' + self.rollup_table_name+'''_insert_f();
        ''')
            

    def create_trigger_update(self):
        '''
        -- an update trigger ensures that updates do not affect the distinct columns
        CREATE OR REPLACE FUNCTION metahtml.metahtml_rollup_host_update_f()
        RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
        BEGIN
            IF new.url != metahtml.url THEN
                RAISE EXCEPTION 'cannot update the "url" column due to distinct rollup';
            END IF;
        RETURN NEW;
        END;
        $$;

        CREATE TRIGGER metahtml_rollup_host_update_t
            BEFORE UPDATE
            ON metahtml.metahtml
            FOR EACH ROW
            EXECUTE PROCEDURE metahtml.metahtml_rollup_host_update_f();
        '''
        return ('''
        CREATE OR REPLACE FUNCTION ''' + self.rollup_table_name + '''_update_f()
        RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
        BEGIN'''+
            '''
            '''.join(['''
            IF '''+_add_namespace(distinct.value,'new')+''' != '''+_add_namespace(distinct.value,self.table_name)+''' THEN
                RAISE EXCEPTION 'update would cause the value of "'''+distinct.value+'''" to change, but it is a distinct constraint on a rollup table';
            END IF;'''
            for distinct in self.distincts
            ])+'''
        RETURN NEW;
        END;
        $$;

        CREATE TRIGGER '''+self.rollup_name+'''_update_t
            BEFORE UPDATE
            ON ''' + self.table + '''
            FOR EACH ROW
            EXECUTE PROCEDURE ''' + self.rollup_table_name+'''_update_f();
        ''')


    def create_trigger_delete(self):
        '''
        -- a delete trigger ensures that deletes never occur
        CREATE OR REPLACE FUNCTION metahtml.metahtml_rollup_host_delete_f()
        RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
        BEGIN
            RAISE EXCEPTION 'cannot delete from metahtml.metahtml due to distinct rollup';
        RETURN NEW;
        END;
        $$;

        CREATE TRIGGER metahtml_rollup_host_delete_t
            BEFORE DELETE
            ON metahtml.metahtml
            FOR EACH ROW
            EXECUTE PROCEDURE metahtml.metahtml_rollup_host_delete_f();
        '''
        return ('''
        CREATE OR REPLACE FUNCTION ''' + self.rollup_table_name + '''_delete_f()
        RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
        BEGIN
            RAISE EXCEPTION 'cannot delete from tables with distinct rollup constraints';
        RETURN NEW;
        END;
        $$;

        CREATE TRIGGER '''+self.rollup_name+'''_delete_t
            BEFORE DELETE
            ON ''' + self.table + '''
            FOR EACH ROW
            EXECUTE PROCEDURE ''' + self.rollup_table_name+'''_delete_f();
        ''')


    def create_view_groundtruth(self):
        '''
        CREATE VIEW metahtml.metahtml_rollop1_view AS (
            SELECT
                url_num,
                count,
                from_t.host,
                from_t.access_day
            FROM (
                SELECT
                    count(1) as count,
                    metahtml.url_host(url) AS host,
                    date_trunc('day',accessed_at) AS access_day
                FROM metahtml.metahtml
                WHERE
                    metahtml.url_host(url) IS NOT NULL AND
                    date_trunc('day',accessed_at) IS NOT NULL
                    --metahtml.url_host(url) IS NOT NULL AND
                    --date_trunc('day',accessed_at) IS NOT NULL
                GROUP BY host,access_day
            ) as from_t
            INNER JOIN (
                SELECT 
                    count(1) as url_num,
                    host,
                    access_day
                FROM (
                    SELECT --DISTINCT ON (metahtml.url)
                        metahtml.url,
                        metahtml.url_host(url) as host,
                        date_trunc('day',accessed_at) AS access_day
                    FROM metahtml.metahtml
                    GROUP BY metahtml.url, host, access_day
                ) as t1
                GROUP BY host, access_day
            ) as inner_join1 ON 
                from_t.host=inner_join1.host AND
                from_t.access_day=inner_join1.access_day
        );
        '''
        return ('''
        CREATE VIEW ''' + self.rollup + '''_groundtruth AS (
        SELECT'''
            +
            (
            '''
            '''+
            '''
            '''.join([distinct.name+'_hll,' for distinct in self.distincts])
            if self.use_hll else ''
            )+
            (
            '''
            '''+
            '''
            '''.join(['distinct_'+distinct.name+',' for distinct in self.distincts])
            if self.use_num else ''
            )+
            '''
            count'''+
            (
            ''',
            '''+
            ''',
            '''.join(['from_t.'+key.name for key in self.wheres])
            if len(self.wheres)>0 else '') + '''
        FROM (
            SELECT'''+
                (
                '''
                '''+
                '''
                '''.join(['hll_add_agg(hll_hash_'+distinct.type+'('+distinct.value+')) AS '+distinct.name+'_hll,' for distinct in self.distincts])
                if self.use_hll else ''
                )+
                '''
                count(1) as count'''+
                (
                ''',
                '''+
                ''',
                '''.join([key.value + ' AS ' + key.name for key in self.wheres])
                if len(self.wheres)>0 else '') +
                '''
            FROM ''' + self.table +
            (
                '''
            GROUP BY ''' + ','.join([key.name for key in self.wheres])
            if len(self.wheres)>0 else '')+'''
        ) AS from_t'''+
        (
        ''.join([
        '''
        INNER JOIN (
            SELECT 
                count(1) AS ''' + 'distinct_'+distinct.name + 
                (
                ''',
                '''+
                ''',
                '''.join([key.name for key in self.wheres])
                if len(self.wheres)>0 else '')+
            '''
            FROM (
                SELECT 
                    '''+
                    distinct.value + ' AS distinct_' + distinct.name +
                    (
                    ''',
                    '''+
                    ''',
                    '''.join([key.value + ' AS ' + key.name for key in self.wheres])
                    if len(self.wheres)>0 else '')+
                '''
                FROM ''' + self.table + '''
                WHERE  '''+distinct.value+' IS NOT NULL' +
                #WHERE  distinct_'''+distinct.name+' IS NOT NULL' +
                #WHERE TRUE ''' + ' '.join([' AND distinct_'+distinct.name+' IS NOT NULL' for distinct in self.distincts]) +
                ' GROUP BY ' + distinct.value +(
                ',' + ','.join([key.name for key in self.wheres])
                if len(self.wheres)>0 else '')+'''
            ) AS t1
            '''+
            (
            '''GROUP BY ''' + ','.join([key.name for key in self.wheres])
            if len(self.wheres)>0 else '')+'''
        ) AS inner_join_''' + distinct.name + ''' ON 
            '''+
            (
            ''' AND
            '''.join(['( from_t.' + key.name + ' = ' + 'inner_join_' + distinct.name + '.' + key.name +' or ( from_t.' + key.name + ' is null and inner_join_' + distinct.name + '.' + key.name + ' is null))' for key in self.wheres])
            if len(self.wheres)>0 else 'TRUE')
        for distinct in self.distincts])
        if self.use_num else ''
        )+
        '''
        );'''
        )


    def create_insert(self):
        '''
        -- ensure that all rows already in the table get rolled up
        INSERT INTO metahtml.metahtml_rollup_host_raw (
            hll,
            count,
            host
            )
        SELECT
            hll_add_agg(hll_hash_text(url)),
            count(1),
            metahtml.url_host(url) AS host
        FROM metahtml.metahtml
        WHERE
            metahtml.url_host(url) IS NOT NULL
        GROUP BY host;
        '''
        return ('''
        INSERT INTO '''+self.rollup_table_name+''' ('''+
            (
            '''
            '''.join([distinct.name+'_hll,' for distinct in self.distincts])
            if self.use_hll else ''
            )+
            (
            '''
            '''.join(['distinct_'+distinct.name+',' for distinct in self.distincts])
            if self.use_num else ''
            )+
            '''
            count'''+
            (
            ''',
            '''+
            ''',
            '''.join([key.name for key in self.wheres])
            if len(self.wheres)>0 else '') + '''
            )
        SELECT *
        FROM ''' + self.rollup + '_groundtruth;')


    def create(self):
        return [
            self.create_table(),
            self.create_indexes_notnull(),
            self.create_indexes_hll(),
            self.create_indexes_num(),
            self.create_indexes_int(),
            self.create_index(),
            self.create_view(),
            self.create_trigger_insert(),
            self.create_trigger_update(),
            self.create_trigger_delete(),
            self.create_view_groundtruth(),
            self.create_insert(),
            ]



def drop_rollup_str(rollup):
    rollup_table_name = rollup+'_raw'
    return ('''
    DROP TABLE '''+rollup_table_name+''' CASCADE;
    DROP VIEW '''+rollup+'''_groundtruth CASCADE;
    DROP FUNCTION '''+rollup_table_name+'''_insert_f CASCADE;
    DROP FUNCTION '''+rollup_table_name+'''_update_f CASCADE;
    DROP FUNCTION '''+rollup_table_name+'''_delete_f CASCADE;
''')


if __name__ == '__main__':
    import doctest
    t = Rollup(
            table='metahtml.metahtml',
            columns=['country','language','person_name','age'],
            rollup='metahtml.metahtml_rollup_host',
            wheres=[
                Key('lower(country)','text','country',False),
                Key('language','text','language',False),
                ],
            distincts=[
                Key('name','text','name',False),
                Key('userid','int','userid',False),
                ]
            )
    doctest.testmod(extraglobs={'t': t})
    t.create()


