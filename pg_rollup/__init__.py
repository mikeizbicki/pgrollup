import collections
import re

Key = collections.namedtuple('Key', ['value','type','name','algebra'])

ViewKey = collections.namedtuple('ViewKey', ['value','name'])


Algebra = collections.namedtuple('Algebra', ['view','agg','hom','type','zero','plus','negate'])

algebras = {
    'count' : { 
        'name':'count',
        'agg':'count(x)',
        'type':'INTEGER',
        'zero':'0',
        'plus':'count(x)+count(y)',
        'negate':'-x',
        'view':'x',
        }
    }

def _algsub(text, x='', y=''):
    '''
    >>> _algsub('hll_add_agg(x)', 'test')
    'hll_add_agg(test)'
    >>> _algsub('x||y', 'left', 'right')
    'left||right'
    >>> _algsub('xy(x,y)', 'left', 'right')
    'xy(left,right)'
    '''
    return re.sub(r'\by\b',y,re.sub(r'\bx\b',x,text))


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

    def __init__(
            self,
            table,
            temporary,
            tablespace,
            rollup,
            groups,
            columns_raw,
            columns_view,
            where_clause,
            having_clause,
            rollup_column
            ):
        self.temporary = temporary
        self.table = table
        self.tablespace = tablespace
        self.rollup = rollup
        self.null_support = True
        self.rollup_column = rollup_column

        self.groups = [Key(k.value, k.type, 'group_'+k.name if k.name[0]!='"' else k.name,None) for k in groups]
        self.columns_raw = sorted(columns_raw, key=lambda column: column.type['typlen'], reverse=True)
        self.columns_view = columns_view
        self.where_clause = where_clause
        self.having_clause = having_clause

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

        def generate_binary(groups, ls):
            if len(groups)==0:
                return [ l[1:] for l in ls ]
            else:
                #if groups[0].unnest:
                    #return generate_binary(groups[1:], [l + '1' for l in ls])
                #else:
                return generate_binary(groups[1:], [l + '0' for l in ls] + [l + '1' for l in ls])
        self.binaries = generate_binary(groups,' ')


    def create_table(self):
        temp_str = 'TEMPORARY ' if self.temporary else ''
        return (
f'''CREATE {temp_str}TABLE '''+self.rollup_table_name+''' ('''+
    (
    '''
    '''.join([''+column.name + ' '+_algsub(column.algebra['type'],column.type['typname']) +' /*NOT NULL*/,' for column in self.columns_raw])+'''
    '''
    )+
    (
    ''',
    '''.join([key.name + ' ' + key.type['typname'] for key in self.groups])
    if len(self.groups)>0 else '''
    raw_true BOOLEAN DEFAULT TRUE UNIQUE NOT NULL''' )+
    '''
    ) TABLESPACE '''+self.tablespace+';\n\n')

    
    def create_indexes_notnull(self):
        '''
        Creates indexes that enforce that the self.rollup_table_name cannot contain multiple NULLs 
        See: https://www.enterprisedb.com/postgres-tutorials/postgresql-column-constraint-null-allowing-only-one-null
        '''
        if len(self.groups)>0:
            return ('\n'.join(['CREATE UNIQUE INDEX '''+self.rollup_name+'_index_'+binary+'_notnull ON '+self.rollup_table_name+' (' + ','.join(['('+key.name+' IS NULL)' if i=='0' else key.name for i,key in zip(binary,self.groups)])+') TABLESPACE '+self.tablespace+' WHERE TRUE '+' '.join(['and '+key.name+' IS NULL' for i,key in zip(binary,self.groups) if i=='0' ])+';' for binary in self.binaries]))
        else:
            return ''


    def create_indexes_groups(self):
        if len(self.groups)>1:
            return '''
            CREATE INDEX '''+self.rollup_name+'_index_num ON '+self.rollup_table_name+' ('+','.join([key.name for key in self.groups])+') TABLESPACE '+self.tablespace+';'
        else:
            return ''


    def _insert_statement(self, inverse, query):
        if inverse and not all([column.algebra['negate'] for column in self.columns_raw]):
            return '''
            RAISE EXCEPTION $exception$ cannot % on table '''+self.rollup_table_name+''' with a rollup using op ''' + str([column.algebra['agg'] for column in self.columns_raw if not column.algebra['negate']])+ '''$exception$, TG_OP;
            '''
        else:
            return '''
        '''.join([
            '''
            INSERT INTO '''+self.rollup_table_name+''' (
                '''+
                (
                ''',
                '''.join([''+column.name for column in self.columns_raw])
                )+
                (
                ''',
                '''+
                ''',
                '''.join([key.name for key in self.groups])
                if len(self.groups)>0 else '' )+ '''
                )
            SELECT 
                '''
            + ''',
            '''.join([
                (_algsub(column.algebra['negate'],' '+''+column.name) if inverse else ' '+''+column.name)
                for column in self.columns_raw
            ])
            + ''.join([''',
                '''+key.name for key in self.groups])+'''
            FROM ( ''' + self.create_groundtruth(query) + ''' ) t
                WHERE TRUE '''+
                ((' '.join(['AND t.' + key.name + ' IS ' + ('' if i=='0' else 'NOT ') + 'NULL' for i,key in zip(binary,self.groups)])
                ) if self.null_support else '')+'''
            ON CONFLICT '''
            ' (' + 
            (','.join(['('+key.name+' IS NULL)' if i=='0' else key.name for i,key in zip(binary,self.groups)]) if len(self.groups)>0 else 'raw_true'
            )+') WHERE TRUE '+' '.join(['and '+key.name+' IS NULL' for i,key in zip(binary,self.groups) if i=='0' ])
            +
            '''
            DO UPDATE SET'''+
                (
                '''
                '''+
                ''',
                '''.join([
                    ''+column.name+self._joinsub(f''' = CASE
                        WHEN {self.rollup_table_name}."{column.algebra['name']}({column.value})" IS NOT NULL AND excluded."{column.algebra['name']}({column.value})" IS NOT NULL THEN {column.algebra['plus']}
                        WHEN {self.rollup_table_name}."{column.algebra['name']}({column.value})" IS NOT NULL AND excluded."{column.algebra['name']}({column.value})" IS     NULL THEN {self.rollup_table_name}."{column.algebra['name']}({column.value})" 
                        WHEN {self.rollup_table_name}."{column.algebra['name']}({column.value})" IS     NULL AND excluded."{column.algebra['name']}({column.value})" IS NOT NULL THEN excluded."{column.algebra['name']}({column.value})"
                        WHEN {self.rollup_table_name}."{column.algebra['name']}({column.value})" IS     NULL AND excluded."{column.algebra['name']}({column.value})" IS     NULL THEN {column.algebra['zero']}
                        END
                    ''',
                        self.rollup_table_name,
                        ''+column.value,
                        column.algebra['zero'],
                        'excluded',
                        ''+column.value,
                        column.algebra['zero'],
                        self.columns_raw
                        )
                    for column in self.columns_raw
                    ])
                )+
                ''';
        '''+
        (''
        if self.null_support else ''
        )
        for binary in self.binaries])


    def _sub_columns(self, text, columns):
        for column in columns:
            text = text.replace('"'+column.algebra['name']+'('+column.value+')"', column.name)
        return text 


    def _joinsub(self, text, xtable, xval, xzero, ytable, yval, yzero,columns):
        '''
        >>> _joinsub('hll(x)||hll(y)', 'xtable', 'xval', 'ytable', 'yval')
        'xtable."hll(xval)"||ytable."hll(yval)"'
        >>> _joinsub('xy(f(x),f(y))', 'xtable', 'xval', 'ytable', 'yval')
        'xy(xtable."f(xval)",ytable."f(yval)")'
        >>> _joinsub('avg(x)*(count(x)/(count(x)+count(y)))+avg(y)*(count(y)/(count(x)+count(y)))', 'xtable', 'xval', 'ytable', 'yval')
        'xtable."avg(xval)"*(xtable."count(xval)"/(xtable."count(xval)"+ytable."count(yval)"))+ytable."avg(yval)"*(ytable."count(yval)"/(xtable."count(xval)"+ytable."count(yval)"))'
        '''
        subx = re.sub(r'\b([a-zA-Z0-9_]+)\(x\)','COALESCE('+xtable+r'."\1('+xval+')",'+xzero+')',text)
        suby = re.sub(r'\b([a-zA-Z0-9_]+)\(y\)','COALESCE('+ytable+r'."\1('+yval+')",'+yzero+')',subx)
        return self._sub_columns(suby,columns)


    def create_manualrollup(self):
        if self.rollup_column is not None:
            return ('''
            CREATE OR REPLACE FUNCTION '''+self.rollup_table_name+'''_manualrollup(
                start_id bigint,
                end_id bigint
                )
            RETURNS VOID LANGUAGE PLPGSQL AS $$
            BEGIN
            '''+self._insert_statement(False, '(SELECT * FROM '+self.table+' WHERE '+self.rollup_column+'>=start_id AND '+self.rollup_column+'<=end_id) AS __table_alias')+'''
            END;
            $$;
            ''')
        else:
            return ''


    def create_triggerfunc(self):
        return ('''
        CREATE OR REPLACE FUNCTION '''+self.rollup_table_name+'''_triggerfunc()
        RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
        BEGIN
        IF TG_OP='UPDATE' OR TG_OP='INSERT' THEN'''+self._insert_statement(False, 'new_table')+'''
        END IF;
        IF TG_OP='UPDATE' OR TG_OP='DELETE' THEN'''+self._insert_statement(True, 'old_table')+'''
        END IF;
        RETURN NULL;
        END;
        $$;
        ''')


    def create_triggers(self):
        if not self.create_triggers:
            return ''
        else:
            return ('''
        CREATE TRIGGER '''+self.rollup_name+'''_insert
            AFTER INSERT
            ON ''' + self.table + '''
            REFERENCING NEW TABLE AS new_table
            FOR EACH STATEMENT
            EXECUTE PROCEDURE ''' + self.rollup_table_name+'''_triggerfunc();

        CREATE TRIGGER '''+self.rollup_name+'''_update
            AFTER UPDATE
            ON ''' + self.table + '''
            REFERENCING NEW TABLE AS new_table
                        OLD TABLE AS old_table
            FOR EACH STATEMENT
            EXECUTE PROCEDURE ''' + self.rollup_table_name+'''_triggerfunc();

        CREATE TRIGGER '''+self.rollup_name+'''_delete
            AFTER DELETE
            ON ''' + self.table + '''
            REFERENCING OLD TABLE AS old_table
            FOR EACH STATEMENT
            EXECUTE PROCEDURE ''' + self.rollup_table_name+'''_triggerfunc();
            ''')


    def create_groundtruth(self, source):
        return (
            'SELECT'+
                (
                '''
                '''+
                ''',
                '''.join([
                    'COALESCE('+
                    _algsub(
                        column.algebra['agg'],
                        column.value
                        )
                    +','+column.algebra['zero']+')'
                    +' AS '+''+column.name
                    for column in self.columns_raw
                    ])
                )+
                (
                ''','''+
                ''',
                '''.join([key.value + ' AS ' + key.name for key in self.groups])
                if len(self.groups)>0 else '') +
                '''
            FROM ''' + source +
            (
            f'''
            WHERE {self.where_clause}
            '''
            if self.where_clause else ''
            ) +
            (
                '''
            GROUP BY ''' + ','.join([key.name for key in self.groups])
            if len(self.groups)>0 else ''
            ) +
            (
            f'''
            HAVING {self.having_clause}
            '''
            if self.having_clause else ''
            )
        )


    def create_view_groundtruth(self):
        '''
        '''
        return ('''
        CREATE VIEW ''' + self.rollup + '''_groundtruth_raw AS (
        '''+self.create_groundtruth(self.table_name)+'''
        );'''
        )


    def create_view_pretty(self,source,target):
        '''
        -- the view simplifies presentation of the hll columns
        CREATE VIEW metahtml.metahtml_rollup_host AS
        SELECT
            hll_cardinality(hll) AS num_rollup_url,
            count,
            host
        FROM metahtml.metahtml_rollup_host_raw;
        '''
        return ('''
        CREATE VIEW '''+target+''' AS
        SELECT'''+
            (
            '''
            '''+
            ''',
            '''.join([
                (column.value
                    if True
                    else
                (_algsub(column.algebra['view'],f''' "{column.algebra['name']}({column.value})" ''' + '/*'+column.value+'*/')
                if column.algebra['plus'].lower().strip() != 'null'
                else 
                 #_algsub(column.algebra['view'],f''' "{column.algebra['name']}({column.value})" ''' + '/*'+column.value+'*/')
                 #+ '/*' +
                self._joinsub(
                        column.algebra['view'],
                        source,
                        ''+column.value,
                        column.algebra['zero'],
                        'excluded',
                        ''+column.value,
                        column.algebra['zero'],
                        self.columns_view
                        )
                #+ '*/'
                ))
                +' AS '+''+column.name for column in self.columns_view
                ])
            )+
            (
            ''',
            '''+
            ''',
            '''.join([key.name for key in self.groups])
            if len(self.groups)>0 else '' )+
            '''
        FROM '''+source+';'
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
        CREATE OR REPLACE FUNCTION '''+self.rollup_table_name+'''_reset()
        RETURNS VOID LANGUAGE PLPGSQL AS $$
        BEGIN
        TRUNCATE TABLE '''+self.rollup_table_name+''';
        INSERT INTO '''+self.rollup_table_name+''' ('''+
            (
            ''',
            '''.join([''+column.name for column in self.columns_raw])
            )+
            (
            ''',
            '''+
            ''',
            '''.join([key.name for key in self.groups])
            if len(self.groups)>0 else '') + '''
            )
        SELECT *
        FROM ''' + self.rollup + '''_groundtruth_raw;

        '''+(
        '''
        UPDATE pg_rollup SET last_aggregated_id=(select max('''+self.rollup_column+''') from '''+self.table_name+") WHERE rollup_name='"+self.rollup_name+"""';
        """ if self.rollup_column else '')+'''
        END;
        $$;
        ''')


    def create(self):
        return ' '.join([
            self.create_table(),
            self.create_indexes_notnull(),
            self.create_manualrollup(),
            self.create_triggerfunc(),
            self.create_view_groundtruth(),
            self.create_view_pretty(self.rollup_table_name,self.rollup),
            self.create_view_pretty(self.rollup+'_groundtruth_raw',self.rollup+'_groundtruth'),
            self.create_insert(),
            ])



def drop_rollup_str(rollup):
    rollup_table_name = rollup+'_raw'
    return (f'''
    DROP TABLE {rollup_table_name} CASCADE;
    DROP VIEW IF EXISTS {rollup}_groundtruth CASCADE;
    DROP VIEW IF EXISTS {rollup}_groundtruth_raw CASCADE;
    DROP FUNCTION {rollup_table_name}_triggerfunc CASCADE;
    DELETE FROM pg_rollup WHERE rollup_name='{rollup}';
''')


if __name__ == '__main__':
    import doctest
    t = Rollup(
            table='tablename',
            temporary=True,
            tablespace='tablespacename',
            rollup='rollupname',
            groups=[
                Key('lower(country)','text','country',None),
                Key('language','text','language',None),
                ],
            columns_view=[
                Key('name','text','name',algebras['count']),
                Key('userid','int','userid',algebras['count']),
                ],
            columns_raw=[
                Key('name','text','name',algebras['count']),
                Key('userid','int','userid',algebras['count']),
                ],
            where_clause = None,
            having_clause = None,
            rollup_column = None
            )
    #doctest.testmod(extraglobs={'t': t})
    print(t.create())
    #print('\n'.join(t.create()))


