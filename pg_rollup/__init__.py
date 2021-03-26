import collections
import re
from lark import Lark

Key = collections.namedtuple('Key', ['value','type','name','algebra'])


Algebra = collections.namedtuple('Algebra', ['view','agg','hom','type','zero','plus','negate'])

algebras = {
    'count' : {
        'agg' : 'count(*)',
        'hom' : '1',
        'type' : 'INTEGER',
        'zero' : '0',
        'plus' : 'x+y',
        'view' : 'x',
        'negate' : '-x',
        },
    'hll' : {
        'agg' : 'hll_add_agg(x)',
        'hom' : 'hll_hash_anynull(x)',
        'type' : 'hll',
        'zero' : 'hll_empty()',
        'plus' :  'x||y',
        'view' : 'floor(hll_cardinality(x))',
        'negate' : None,
        },
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


def unparse(tree):
    if hasattr(tree,'children'):
        if tree.data == 'blurb':
            return '(' + ''.join([unparse(child) for child in tree.children]) + ')'
        elif tree.data == 'as':
            return ' AS ' + ' '.join([unparse(child) for child in tree.children])
        else:
            return ' '.join([unparse(child) for child in tree.children])
    else:
        return str(tree)


def parse_create(text):
    '''
    >>> parse_create('create incremental materialized view test_rollup as (select count(*) from test);')
    {'table': 'test', 'rollup_name': 'test_rollup', 'rollups': ' count(*) ', 'wheres': ''}
    >>> parse_create('create incremental materialized view test_rollup as (select count(*) from test group by date, url );')
    {'table': 'test', 'rollup_name': 'test_rollup', 'rollups': ' count(*) ', 'wheres': 'date, url '}
    '''
    parser = Lark(r"""
    value: create_view ";"?

    create_view: "CREATE"i "INCREMENTAL"i "MATERIALIZED"i "VIEW"i name "AS"i "(" select ")"

    name: /[a-zA-Z_0-9]+/ | ESCAPED_STRING

    select: "SELECT"i columns "FROM"i name ("GROUP"i "BY"i groups)?

    columns: column comma columns | column
    column: name "(" blurb ")" (as name)?

    blurb: /./+
    comma: /,/
    as: "AS"i

    groups: group comma groups | group
    group: name

    %import common.ESCAPED_STRING
    %import common.WS
    %ignore WS
    """, start='value')
    tree = parser.parse(text)

    infos = []
    for child in tree.children:
        rollup_name = unparse(child.children[0].children[0])
        table_name = unparse(child.children[1].children[1].children[0])
        columns = unparse(child.children[1].children[0])
        try:
            groups = unparse(child.children[1].children[2])
        except IndexError:
            groups = ''
        infos.append({
            'table_name' : table_name,
            'rollup_name' : rollup_name,
            'groups' : groups,
            'columns' : columns
            })
    return infos
    # FIXME: implement unparse
    print("x=",x)
    create_regex_str = r'''CREATE\s+INCREMENTAL\s+MATERIALIZED\s+VIEW\s+(\w+)\s+AS\s*\(\s*SELECT(.*)FROM\s+(\w+)(\s+GROUP\s+BY\s+(.*))?\)\s*;?'''
    create_regex = re.compile(create_regex_str, re.IGNORECASE|re.MULTILINE)
    result = create_regex.match(text)
    if result:
        return {
            'table' : result.groups()[2],
            'rollup_name' : result.groups()[0],
            'rollups' : result.groups()[1],
            'wheres' : result.groups()[4] if result.groups()[4] is not None else ''
            }

"""
text = '''
CREATE      INCREMENTAL MATERIALIZED VIEW example AS (SELECT count(*) AS count, hll(ad aIHD P98HDAWLDKJSDE DLKAJSD FN" SDFASD FASD ) FROM table GROUP BY a, b);
CREATE      INCREMENTAL MATERIALIZED VIEW example AS (SELECT count(*) AS count, hll(ad aIHD P98HDAWLDKJSDE DLKAJSD FN" SDFASD FASD ) FROM table GROUP BY a, b);
'''
x = parse_create(text)
import pprint
pprint.pprint(x)
asd
"""


def parse_algebra(text):
    '''
    >>> parse_algebra('count(*) AS sum')
    {'algebra': 'count', 'expr': '*', 'name': 'sum'}
    >>> parse_algebra('count(*) as sum')
    {'algebra': 'count', 'expr': '*', 'name': 'sum'}
    >>> parse_algebra('count(*)')
    {'algebra': 'count', 'expr': '*', 'name': '"count(*)"'}
    >>> parse_algebra('sum(1+max(left,right))')
    {'algebra': 'sum', 'expr': '1+max(left,right)', 'name': 'sum'}
    '''
    # FIXME: this will break columns in "" with capitalization
    text = text.lower()

    parts = text.split('as')
    
    agg = parts[0].strip()
    ret = {
        'algebra': agg[:agg.find("(")],
        'expr': agg[agg.find("(")+1:agg.rfind(")")]
        }
    if len(parts) == 1:
        ret['name'] = '"'+agg+'"' #ret['algebra']
    else:
        ret['name'] = parts[1].strip()
    return ret

class Rollup:

    def __init__(self, table, temporary, tablespace, rollup, wheres, distincts, rollup_column):
        self.temporary = temporary
        self.table = table
        self.tablespace = tablespace
        self.rollup = rollup
        self.use_hll = True
        self.null_support = True
        self.rollup_column = rollup_column

        self.wheres = [Key(k.value, k.type, 'where_'+k.name,None) for k in wheres]
        self.distincts = [Key(k.value, k.type, k.name, k.algebra) for k in distincts]

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

        def generate_binary(wheres, ls):
            if len(wheres)==0:
                return [ l[1:] for l in ls ]
            else:
                #if wheres[0].unnest:
                    #return generate_binary(wheres[1:], [l + '1' for l in ls])
                #else:
                return generate_binary(wheres[1:], [l + '0' for l in ls] + [l + '1' for l in ls])
        self.binaries = generate_binary(wheres,' ')


    def create_table(self):
        temp_str = 'TEMPORARY ' if self.temporary else ''
        return (
f'''CREATE {temp_str}TABLE '''+self.rollup_table_name+''' ('''+
    (
    '''
    '''.join([''+distinct.name + ' '+_algsub(distinct.algebra['type'],distinct.type) +' /*NOT NULL*/,' for distinct in self.distincts])+'''
    '''
    if self.use_hll else ''
    )+
    (
    ''',
    '''.join([key.name + ' ' + key.type for key in self.wheres])
    if len(self.wheres)>0 else '''
    raw_true BOOLEAN DEFAULT TRUE UNIQUE NOT NULL''' )+
    '''
    ) TABLESPACE '''+self.tablespace+';')

    
    def create_indexes_notnull(self):
        '''
        Creates indexes that enforce that the self.rollup_table_name cannot contain multiple NULLs 
        See: https://www.enterprisedb.com/postgres-tutorials/postgresql-distinct-constraint-null-allowing-only-one-null
        '''
        if len(self.wheres)>0:
            return ('\n'.join(['CREATE UNIQUE INDEX '''+self.rollup_name+'_index_'+binary+'_notnull ON '+self.rollup_table_name+' (' + ','.join(['('+key.name+' IS NULL)' if i=='0' else key.name for i,key in zip(binary,self.wheres)])+') TABLESPACE '+self.tablespace+' WHERE TRUE '+' '.join(['and '+key.name+' IS NULL' for i,key in zip(binary,self.wheres) if i=='0' ])+';' for binary in self.binaries]))
        else:
            return ''


    def create_indexes_wheres(self):
        if len(self.wheres)>1:
            return '''
            CREATE INDEX '''+self.rollup_name+'_index_num ON '+self.rollup_table_name+' ('+','.join([key.name for key in self.wheres])+') TABLESPACE '+self.tablespace+';'
        else:
            return ''


    def _insert_statement(self, inverse, query):
        if inverse and not all([distinct.algebra['negate'] for distinct in self.distincts]):
            return '''
            RAISE EXCEPTION $exception$ cannot % on table '''+self.rollup_table_name+''' with a rollup using op ''' + str([distinct.algebra['agg'] for distinct in self.distincts if not distinct.algebra['negate']])+ '''$exception$, TG_OP;
            '''
        else:
            return '''
        '''.join([
            '''
            INSERT INTO '''+self.rollup_table_name+''' (
                '''+
                (
                ''',
                '''.join([''+distinct.name for distinct in self.distincts])
                if self.use_hll else ''
                )+
                (
                ''',
                '''+
                ''',
                '''.join([key.name for key in self.wheres])
                if len(self.wheres)>0 else '' )+ '''
                )
            SELECT 
                '''
            + ''',
            '''.join([
                (_algsub(distinct.algebra['negate'],' '+''+distinct.name) if inverse else ' '+''+distinct.name)
                for distinct in self.distincts
            ])
            + ''.join([''',
                '''+key.name for key in self.wheres])+'''
            FROM ( ''' + self.create_groundtruth(query) + ''' ) t
                WHERE TRUE '''+
                ((' '.join(['AND t.' + key.name + ' IS ' + ('' if i=='0' else 'NOT ') + 'NULL' for i,key in zip(binary,self.wheres)])
                ) if self.null_support else '')+'''
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
                ''',
                '''.join([
                    ''+distinct.name+self._joinsub(f''' = CASE
                        WHEN {self.rollup_table_name}."{distinct.algebra['name']}({distinct.value})" IS NOT NULL AND excluded."{distinct.algebra['name']}({distinct.value})" IS NOT NULL THEN {distinct.algebra['plus']}
                        WHEN {self.rollup_table_name}."{distinct.algebra['name']}({distinct.value})" IS NOT NULL AND excluded."{distinct.algebra['name']}({distinct.value})" IS     NULL THEN {self.rollup_table_name}."{distinct.algebra['name']}({distinct.value})" 
                        WHEN {self.rollup_table_name}."{distinct.algebra['name']}({distinct.value})" IS     NULL AND excluded."{distinct.algebra['name']}({distinct.value})" IS NOT NULL THEN excluded."{distinct.algebra['name']}({distinct.value})"
                        WHEN {self.rollup_table_name}."{distinct.algebra['name']}({distinct.value})" IS     NULL AND excluded."{distinct.algebra['name']}({distinct.value})" IS     NULL THEN {distinct.algebra['zero']}
                        END
                    ''',
                    #''+distinct.name+' = '+self._joinsub(
                        #distinct.algebra['plus'],
                        self.rollup_table_name,
                        ''+distinct.value,
                        distinct.algebra['zero'],
                        'excluded',
                        ''+distinct.value,
                        distinct.algebra['zero'],
                        self.distincts
                        )
                    for distinct in self.distincts
                    ])
                if self.use_hll else ''
                )+
                ''';
        '''+
        (''
        if self.null_support else ''
        )
        for binary in self.binaries])


    def _sub_distincts(self, text):
        for distinct in self.distincts:
            text = text.replace('"'+distinct.algebra['name']+'('+distinct.value+')"', distinct.name)
        return text 


    def _joinsub(self, text, xtable, xval, xzero, ytable, yval, yzero,distincts):
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
        return self._sub_distincts(suby)


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
                        distinct.algebra['agg'],
                        distinct.value
                        )
                    +','+distinct.algebra['zero']+')'
                    +' AS '+''+distinct.name
                    for distinct in self.distincts
                    ])
                if self.use_hll else ''
                )+
                (
                ''','''+
                ''',
                '''.join([key.value + ' AS ' + key.name for key in self.wheres])
                if len(self.wheres)>0 else '') +
                '''
            FROM ''' + source +
            (
                '''
            GROUP BY ''' + ','.join([key.name for key in self.wheres])
            if len(self.wheres)>0 else ''
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

                (_algsub(distinct.algebra['view'],''+distinct.name)
                if distinct.algebra['plus'].lower().strip() != 'null'
                #else '1'
                else self._joinsub(
                        distinct.algebra['view'],
                        source,
                        ''+distinct.value,
                        distinct.algebra['zero'],
                        'excluded',
                        ''+distinct.value,
                        distinct.algebra['zero'],
                        self.distincts
                        )
                )
                +' AS '+''+distinct.name for distinct in self.distincts
                ])
            if self.use_hll else ''
            )+
            (
            ''',
            '''+
            ''',
            '''.join([key.name for key in self.wheres])
            if len(self.wheres)>0 else '' )+
            '''
        FROM '''+source+';'
        #(
        #'''
        #WHERE
        #'''
        #if len(['' for distinct in self.distincts if distinct.algebra['negate'] is not None])>0
        #else ''
        #)
        #+
        #(
        #''' AND 
        #'''.join(['''
                #''' + _algsub(distinct.algebra['view'],''+distinct.name)
                    #+ ' != ' 
                    #+ _algsub(distinct.algebra['view'],distinct.algebra['zero'])
                #for distinct in self.distincts if distinct.algebra['negate'] is not None #and distinct.algebra['plus'].lower().strip() != 'null'
            #])
        #)+'''
        #;'''
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
            '''.join([''+distinct.name for distinct in self.distincts])
            if self.use_hll else ''
            )+
            (
            ''',
            '''+
            ''',
            '''.join([key.name for key in self.wheres])
            if len(self.wheres)>0 else '') + '''
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
        return [
            self.create_table(),
            self.create_indexes_notnull(),
            self.create_manualrollup(),
            self.create_triggerfunc(),
            self.create_view_groundtruth(),
            self.create_view_pretty(self.rollup_table_name,self.rollup),
            self.create_view_pretty(self.rollup+'_groundtruth_raw',self.rollup+'_groundtruth'),
            self.create_insert(),
            ]



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
            table='metahtml.metahtml',
            temporary=True,
            tablespace='pg_default',
            rollup='metahtml.metahtml_rollup_host',
            wheres=[
                Key('lower(country)','text','country',None),
                Key('language','text','language',None),
                ],
            distincts=[
                Key('name','text','name',algebras['count']),
                Key('userid','int','userid',algebras['count']),
                ],
            rollup_column = None
            )
    #doctest.testmod(extraglobs={'t': t})
    print('\n'.join(t.create()))


