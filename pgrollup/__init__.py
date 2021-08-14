import collections
import copy
import re

Key = collections.namedtuple('Key', ['value','type','name','algebra','nullable'])

ViewKey = collections.namedtuple('ViewKey', ['value','name'])

Algebra = collections.namedtuple('Algebra', ['view','agg','hom','type','zero','plus','negate'])



def _null_wrap_str(s):
    '''
    This helper function simplifies some code that can be a string or NULL.
    '''
    if s:
        return "'"+s+"'"
    else:
        return 'NULL'
    

def _alias_joininfos(joininfos, aliases):
    '''
    FIXME:
    This seems to make outer joins work, but I'm not 100% sure why.
    Also, can we get cross joins to work?
    '''
    if len(aliases) != 1:
        return joininfos
    else:
        alias=aliases[0]
    new_joininfos = []
    for i,joininfo in enumerate(joininfos):
        new_joininfo = copy.deepcopy(joininfo)
        if joininfo['table_alias']==alias:
            if joininfo['join_type']=='LEFT JOIN':
                new_joininfo['join_type']='INNER JOIN'
            if joininfo['join_type']=='FULL JOIN':
                new_joininfo['join_type']='RIGHT JOIN'
            if joininfo['join_type']=='RIGHT JOIN':
                new_joininfo['join_type']='RIGHT JOIN'
        if i==1 and joininfos[0]['table_alias']==alias:
            if joininfo['join_type']=='LEFT JOIN':
                new_joininfo['join_type']='LEFT JOIN'
            if joininfo['join_type']=='FULL JOIN':
                new_joininfo['join_type']='LEFT JOIN'
            if joininfo['join_type']=='RIGHT JOIN':
                new_joininfo['join_type']='INNER JOIN'
        new_joininfos.append(new_joininfo)
    return new_joininfos

def _getjoinvar(text):
    '''
    >>> _getjoinvar('using (id)')
    'id'
    '''
    match = re.search('\(([a-zA-Z0-0_]+)\)',text)
    if match:
        return match.group(1)

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
            joininfos,
            temporary,
            tablespace,
            rollup,
            groups,
            columns_raw,
            columns_view,
            where_clause,
            having_clause,
            partition_clause,
            ):
        self.temporary = temporary
        self.tablespace = tablespace
        self.rollup = rollup
        self.null_support = True
        self.joininfos = joininfos
        self.table = table = joininfos[0]['table_name']
        self.rollup_column = joininfos[0]['rollup_column']
        self.partition_clause = partition_clause

        self.invertable = all([column.algebra['negate'] for column in columns_raw])

        self.joininfos_merged = {}
        for joininfo in joininfos:
            self.joininfos_merged.get(joininfo['table_name'],[]).append(joininfo)

        self.groups = [Key(k.value, k.type, ''+k.name if k.name[0]!='"' else k.name,None,k.nullable) for k in groups]
        self.columns_raw = sorted(columns_raw, key=lambda column: column.type['typlen'], reverse=True)
        self.columns_view = columns_view
        self.where_clause = where_clause if where_clause else 'TRUE'
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
                if groups[0].nullable:
                    return generate_binary(groups[1:], [l + '0' for l in ls] + [l + '1' for l in ls])
                else:
                    return generate_binary(groups[1:],                         [l + '1' for l in ls])
        self.binaries = generate_binary(groups,' ')


    def create_table(self):
        temp_str = 'TEMPORARY ' if self.temporary else ''
        return (
f'''CREATE {temp_str}TABLE '''+self.rollup_table_name+''' (
    '''+
    (
    '''
    '''.join([''+column.name + ' '+_algsub(column.algebra['type'],column.type['typname']) +',' for column in self.columns_raw])
    +
    '''
    '''
    )+
    (
    ''',
    '''.join([key.name + ' ' + key.type['typname'] + (' NULL' if key.nullable else ' NOT NULL') for key in self.groups])
    if len(self.groups)>0
    else '''raw_true BOOLEAN DEFAULT TRUE UNIQUE NOT NULL''' )+
    '''
    ) '''+ (self.partition_clause if self.partition_clause else '''TABLESPACE '''+self.tablespace) +';\n\n')

    
    def create_indexes_notnull(self):
        '''
        Creates indexes that enforce that the self.rollup_table_name cannot contain multiple NULLs 
        See: https://www.enterprisedb.com/postgres-tutorials/postgresql-column-constraint-null-allowing-only-one-null
        '''
        if len(self.groups)>0:
            return ('\n'.join(['CREATE UNIQUE INDEX '''+self.rollup_name+'_index_'+binary+'_notnull ON '+self.rollup_table_name+' (' + ','.join(['('+key.name+' IS NULL)'+(','+key.name if self.partition_clause else '') if i=='0' else key.name for i,key in zip(binary,self.groups)])+') ' + ('' if self.partition_clause else 'TABLESPACE '+self.tablespace)+' WHERE TRUE '+' '.join(['and '+key.name+' IS NULL' for i,key in zip(binary,self.groups) if i=='0' ])+';' for binary in self.binaries]))
        else:
            return ''


    def create_indexes_groups(self):
        if len(self.groups)>1:
            return '''
            CREATE INDEX '''+self.rollup_name+'_index_num ON '+self.rollup_table_name+' ('+','.join([key.name for key in self.groups])+') ' + ('' if self.partition_clause else 'TABLESPACE '+self.tablespace+';')
        else:
            return ''

    def _insert_table(self, table, use_temporary_table=False):
            return (
            (
            '''
            CREATE TEMPORARY TABLE pgrollup_insert_tmp AS
            '''
            + table + ''';
            '''
            if use_temporary_table
            else ''
            )
            +
            '''
            '''.join([
            '''
            INSERT INTO '''+self.rollup_table_name+''' (
                '''+
                (
                ''',
                '''.join([column.name for column in self.columns_raw])
                )+
                (
                ''',
                '''+
                ''',
                '''.join([key.name for key in self.groups])
                if len(self.groups)>0 else '' )+ '''
                ) 
            SELECT * FROM (
            '''
            +
            table
            +
            '''
            ) t
            WHERE TRUE '''+
                ((' '.join(['AND t.' + key.name + ' IS ' + ('' if i=='0' else 'NOT ') + 'NULL' for i,key in zip(binary,self.groups)])
                ) if self.null_support else '')
            +
            '''
            ON CONFLICT '''
            ' (' + 
            (','.join(['('+key.name+' IS NULL)'+(','+key.name if self.partition_clause else '') if i=='0' else key.name for i,key in zip(binary,self.groups)]) if len(self.groups)>0 else 'raw_true'
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
                '''
                ;
        '''+
        (''
        if self.null_support else ''
        )
        for binary in self.binaries])
        +
        (
        '''
        DROP TABLE pgrollup_insert_tmp;
        '''
        if use_temporary_table
        else ''
        )
        )


    def _insert_statement(self, inverse, source, aliases, temporary_table=False):
        if inverse and not self.invertable:
            return '''
            RAISE EXCEPTION $exception$ cannot % on table '''+self.rollup_table_name+''' with a rollup using op ''' + str([column.algebra['agg'] for column in self.columns_raw if not column.algebra['negate']])+ '''$exception$, TG_OP;
            '''
        result = self._insert_table(self.create_groundtruth(source, aliases, inverse))

        # FIXME: fix this for outer joins
        if False and self.invertable and len(self.joininfos)>1 and self.joininfos[0]['table_alias']==aliases[0]:
            result += self._insert_table(
            '''
            SELECT * FROM (
                ''' + self.create_joinsubtractor(source, aliases, inverse) + '''
            ) t2 '''
            if self.invertable
            else ''
            )
        return result


    def _sub_columns(self, text, columns):
        for column in columns:
            text = text.replace('"'+column.algebra['name']+'('+column.value+')"', column.name)
        return text 


    def _joinsub(self, text, xtable, xval, xzero, ytable, yval, yzero,columns):
        '''
        _joinsub('hll(x)||hll(y)', 'xtable', 'xval', 'ytable', 'yval')
        'xtable."hll(xval)"||ytable."hll(yval)"'
        _joinsub('xy(f(x),f(y))', 'xtable', 'xval', 'ytable', 'yval')
        'xy(xtable."f(xval)",ytable."f(yval)")'
        _joinsub('avg(x)*(count(x)/(count(x)+count(y)))+avg(y)*(count(y)/(count(x)+count(y)))', 'xtable', 'xval', 'ytable', 'yval')
        'xtable."avg(xval)"*(xtable."count(xval)"/(xtable."count(xval)"+ytable."count(yval)"))+ytable."avg(yval)"*(ytable."count(yval)"/(xtable."count(xval)"+ytable."count(yval)"))'
        '''
        subx = re.sub(r'\b([a-zA-Z0-9_]+)\(x\)',xtable+r'."\1('+xval+')"',text)
        suby = re.sub(r'\b([a-zA-Z0-9_]+)\(y\)',ytable+r'."\1('+yval+')"',subx)
        return self._sub_columns(suby,columns)

    def create_manualrollup(self):
        manualrollups = []
        for joininfo in self.joininfos:
            if joininfo.get('rollup_column') is not None:
                function_name = 'pgrollup_manual_'''+self.rollup_table_name+'__'+joininfo['table_alias']
                manualrollups.append('''
                CREATE OR REPLACE FUNCTION '''+function_name+'''(
                    start_id bigint,
                    end_id bigint
                    )
                RETURNS VOID LANGUAGE PLPGSQL AS $$
                BEGIN
                '''+
                self._insert_statement(
                    False,
                    '(SELECT * FROM '+self.table+' WHERE '+self.rollup_column+'>=start_id AND '+self.rollup_column+'<=end_id)',
                    [joininfo['table_alias']]
                    )
                +'''
                END;
                $$;
                ''')
            else:
                function_name = None
            manualrollups.append(
                f'''INSERT INTO pgrollup_rollups 
                    ( rollup_name
                    , table_alias
                    , table_name
                    , rollup_column
                    , event_id_sequence_name
                    , sql
                    , mode
                    )
                    values 
                    ( '{self.rollup_name}'
                    , '{joininfo['table_alias']}'
                    , '{joininfo['table_name']}'
                    , {_null_wrap_str(joininfo['rollup_column'])}
                    , {_null_wrap_str(joininfo['event_id_sequence_name'])}
                    , {_null_wrap_str(function_name)}
                    , 'init'
                    );
                ''')
        return '\n'.join(manualrollups)


    def create_triggerfunc(self):
        return ('''
            '''.join(['''
            CREATE OR REPLACE FUNCTION '''+self.rollup_table_name+'__'+joininfo['table_alias']+'''_triggerfunc()
            RETURNS TRIGGER LANGUAGE PLPGSQL AS $$
            BEGIN
                IF TG_OP='UPDATE' OR TG_OP='INSERT' THEN'''+self._insert_statement(False, 'new_table',[joininfo['table_alias']])+'''
                    UPDATE pgrollup_rollups
                    SET last_aggregated_id=(
                        SELECT pg_sequence_last_value(event_id_sequence_name) AS lastval
                        FROM pgrollup_rollups
                        WHERE rollup_name=\''''+self.rollup_name+'''\'
                          AND table_alias=\''''+joininfo['table_alias']+'''\'
                        )
                    WHERE rollup_name=\''''+self.rollup_name+'''\'
                      AND table_alias=\''''+joininfo['table_alias']+'''\';
                END IF;
                IF TG_OP='UPDATE' OR TG_OP='DELETE' THEN'''+self._insert_statement(True, 'old_table',[joininfo['table_alias']])+'''
                END IF;
                RETURN NULL;
            END;
            $$;

            CREATE OR REPLACE FUNCTION pgr1__'''+self.rollup_name+'__'+joininfo['table_alias']+'''()
            RETURNS VOID LANGUAGE PLPGSQL AS $$
            BEGIN
                CREATE TRIGGER '''+self.rollup_name+'__'+joininfo['table_alias']+'''_insert
                    AFTER INSERT
                    ON ''' + joininfo['table_name'] + '''
                    REFERENCING NEW TABLE AS new_table
                    FOR EACH STATEMENT
                    EXECUTE PROCEDURE ''' + self.rollup_table_name+'__'+joininfo['table_alias']+'''_triggerfunc();
                CREATE TRIGGER '''+self.rollup_name+'__'+joininfo['table_alias']+'''_update
                    AFTER UPDATE
                    ON ''' + joininfo['table_name'] + '''
                    REFERENCING NEW TABLE AS new_table
                                OLD TABLE AS old_table
                    FOR EACH STATEMENT
                    EXECUTE PROCEDURE ''' + self.rollup_table_name+'__'+joininfo['table_alias']+'''_triggerfunc();
                CREATE TRIGGER '''+self.rollup_name+'__'+joininfo['table_alias']+'''_delete
                    AFTER DELETE
                    ON ''' + joininfo['table_name'] + '''
                    REFERENCING OLD TABLE AS old_table
                    FOR EACH STATEMENT
                    EXECUTE PROCEDURE ''' + self.rollup_table_name+'__'+joininfo['table_alias']+'''_triggerfunc();
            END;
            $$;

            CREATE OR REPLACE FUNCTION pgr2__'''+self.rollup_name+'__'+joininfo['table_alias']+'''()
            RETURNS VOID LANGUAGE PLPGSQL AS $$
            BEGIN
                '''
                +
                '''DROP TRIGGER '''+self.rollup_name+'__'+joininfo['table_alias']+'''_insert ON '''+joininfo['table_name']+''';
                '''+
                '''DROP TRIGGER '''+self.rollup_name+'__'+joininfo['table_alias']+'''_update ON '''+joininfo['table_name']+''';
                '''+
                '''DROP TRIGGER '''+self.rollup_name+'__'+joininfo['table_alias']+'''_delete ON '''+joininfo['table_name']+''';
                '''
                +
                '''
            END;
            $$;
            '''
            for joininfo in self.joininfos
            ]))


    def create_joinsubtractor(self, source, aliases=[], negate=False):
        new_where = 'num1 in (SELECT num1 FROM ' + source + ' t )'
        return (
                'SELECT'+
                    (
                    '''
                    '''+
                    ''',
                    '''.join([
                        'COALESCE('+
                        (
                        _algsub(
                            column.algebra['agg'],
                            column.value
                            )
                        if not not negate else
                        _algsub(
                            column.algebra['negate'],
                            _algsub(
                                column.algebra['agg'],
                                column.value
                                )
                            )
                        )
                        +','+column.algebra['zero']+')'
                        +' AS '+''+column.name
                        for column in self.columns_raw
                        ])
                    )+
                    (
                    ''',
                    '''+
                    ''',
                    '''.join([key.value + ' AS ' + key.name for key in self.groups])
                    if len(self.groups)>0 else '') +
                (
                ''.join([
                '''
                ''' + (joininfo['join_type'] if joininfo['join_type'] != 'RIGHT JOIN' else 'FULL JOIN') + ' ' 
                    + (joininfo['table_name'] 
                       if joininfo['table_alias'] not in aliases
                       else '( SELECT * FROM '+joininfo['table_name']+' LIMIT 0)'
                      )
                    + ' AS ' + joininfo['table_alias']
                    + ' ' + joininfo['condition']
                for joininfo in self.joininfos
                ])
                )
                +
                (
                f'''
                WHERE ({self.where_clause})
                  AND ''' + new_where 
                +
                (
                '''
                '''.join(['''
                  AND (''' + joininfo['table_alias'] + '.' + joininfo['rollup_column'] + " IS NULL OR "
                           + joininfo['table_alias'] + '.' + joininfo['rollup_column'] + " <= (SELECT last_aggregated_id FROM pgrollup_rollups WHERE rollup_name='"+self.rollup_name+"' AND table_alias='"+joininfo['table_alias']+"'))"
                  for joininfo in self.joininfos if joininfo['table_alias'] not in aliases
                    ])
                if len(self.joininfos)>1
                else ''
                )
                )
                +
                (
                '''
                GROUP BY ''' + ','.join([key.name for key in self.groups])
                if len(self.groups)>0 else ''
                ) +
                (
                f'''
                HAVING ({self.having_clause})
                '''
                if self.having_clause else ''
                )
            )

    def create_groundtruth(self, source=None, aliases=[], negate=False):
        return (
                'SELECT'+
                    (
                    '''
                    '''+
                    ''',
                    '''.join([
                        'COALESCE('+
                        (
                        _algsub(
                            column.algebra['agg'],
                            column.value
                            )
                        if not negate else
                        _algsub(
                            column.algebra['negate'],
                            _algsub(
                                column.algebra['agg'],
                                column.value
                                )
                            )
                        )
                        +','+column.algebra['zero']+')'
                        +' AS '+''+column.name
                        for column in self.columns_raw
                        ])
                    )+
                    (
                    ''',
                    '''+
                    ''',
                    '''.join([key.value + ' AS ' + key.name for key in self.groups])
                    if len(self.groups)>0 else '') +
                (
                ''.join([
                '''
                ''' + (joininfo['join_type']) + ' ' 
                    + (joininfo['table_name'] if joininfo['table_alias'] not in aliases else source)
                    + ' AS ' + joininfo['table_alias']
                    + ' ' + joininfo['condition']
                for joininfo in _alias_joininfos(self.joininfos,aliases)
                ])
                )
                +
                (
                f'''
                WHERE ({self.where_clause})'''
                +
                (
                '''
                '''.join(['''
                  AND (''' + joininfo['table_alias'] + '.' + joininfo['rollup_column'] + " IS NULL OR "
                           + joininfo['table_alias'] + '.' + joininfo['rollup_column'] + " <= (SELECT last_aggregated_id FROM pgrollup_rollups WHERE rollup_name='"+self.rollup_name+"' AND table_alias='"+joininfo['table_alias']+"'))"
                  for joininfo in self.joininfos if joininfo['table_alias'] not in aliases
                    ])
                if len(self.joininfos)>1
                else ''
                )
                )
                +
                (
                '''
                GROUP BY ''' + ','.join([key.name for key in self.groups])
                if len(self.groups)>0 else ''
                ) +
                (
                f'''
                HAVING ({self.having_clause})
                '''
                if self.having_clause else ''
                )
            )


    def create_view_groundtruth(self):
        '''
        '''
        return ('''
        CREATE VIEW ''' + self.rollup + '''_groundtruth_raw AS (
        '''+self.create_groundtruth()+'''
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
            '''.join([column.value+' AS '+''+column.name for column in self.columns_view])
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
        return ('''
        CREATE OR REPLACE FUNCTION '''+self.rollup_table_name+'''_reset()
        RETURNS VOID LANGUAGE PLPGSQL AS $$
        BEGIN
            TRUNCATE TABLE '''+self.rollup_table_name+''';
            INSERT INTO '''+self.rollup_table_name+''' SELECT * FROM ''' + self.rollup + '''_groundtruth_raw;'''
            +
            (
            '''
            UPDATE pgrollup_rollups SET last_aggregated_id=(select max('''+self.rollup_column+''') from '''+self.table_name+") WHERE rollup_name='"+self.rollup_name+"';"
            if self.rollup_column else ''
            )+'''
        END;
        $$;
        ''')

    def create_drop(self):
        return (f'''
        CREATE OR REPLACE FUNCTION pgrollup_drop__'''+self.rollup_name+f'''()
        RETURNS VOID LANGUAGE PLPGSQL AS $$
            BEGIN
            DROP TABLE {self.rollup_table_name} CASCADE;
            DROP VIEW IF EXISTS {self.rollup}_groundtruth CASCADE;
            DROP VIEW IF EXISTS {self.rollup}_groundtruth_raw CASCADE;
            '''
            +
            '''
            '''.join([
                'DROP FUNCTION '+self.rollup_table_name+'__'+joininfo['table_alias']+'''_triggerfunc CASCADE;
                DROP FUNCTION pgr1__'''+self.rollup_name+'__'+joininfo['table_alias']+''' CASCADE;'''
                for joininfo in self.joininfos])
            +
            f'''
            DELETE FROM pgrollup_rollups WHERE rollup_name='{self.rollup}';
        END;
        $$;
        ''')

    def create(self):
        return '\n\n'.join([
            self.create_table(),
            self.create_indexes_notnull(),
            self.create_manualrollup(),
            self.create_triggerfunc(),
            self.create_view_groundtruth(),
            self.create_view_pretty(self.rollup_table_name,self.rollup),
            self.create_view_pretty(self.rollup+'_groundtruth_raw',self.rollup+'_groundtruth'),
            self.create_insert(),
            self.create_drop(),
            ])



if __name__ == '__main__':
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

    import doctest
    t = Rollup(
            temporary=True,
            tablespace='tablespacename',
            rollup='rollupname',
            groups=[
                Key('lower(country)',{'typname':'text','typlen':-1},'country',None,None),
                Key('language',{'typname':'text','typlen':-1},'language',None,None),
                ],
            columns_view=[
                ViewKey('name','name'),
                ViewKey('userid','userid'),
                ],
            columns_raw=[
                Key('name',{'typname':'text','typlen':-1},'name',algebras['count'],False),
                Key('userid',{'typname':'int','typlen':4},'userid',algebras['count'],True),
                ],
            joininfos=[{
                    'join_type': 'FROM',
                    'table_name': 'tablename',
                    'table_alias': 'tablename',
                    'condition': '',
                    'rollup_column': 'pk1',
                    'event_id_sequence_name': 'event_id_sequence_name1',
                },{
                    'join_type': 'INNER JOIN',
                    'table_name': 'example1name',
                    'table_alias': 'example1alias',
                    'condition': 'USING (id)',
                    'rollup_column': 'pk2',
                    'event_id_sequence_name': 'event_id_sequence_name2',
                },{
                    'join_type': 'LEFT JOIN',
                    'table_name': 'example2name',
                    'table_alias': 'example2alias',
                    'condition': 'ON example.id=example2.id2',
                    'rollup_column': 'pk3',
                    'event_id_sequence_name': 'event_id_sequence_name3',
                }
                ],
            where_clause = "name='test'",
            having_clause = "usedid=1",
            )
    #doctest.testmod(extraglobs={'t': t})
    print(t.create())
    #print('\n'.join(t.create()))


