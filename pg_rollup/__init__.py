import collections
import re

Key = collections.namedtuple('Key', ['value','type','name','algebra'])


Algebra = collections.namedtuple('Algebra', ['view','agg','hom','type','zero','plus','negate'])

algebras = {
    'count' : Algebra(
        agg= lambda x:'count(1)',
        hom = lambda x: '1',
        type = lambda x: 'INTEGER',
        zero = '0',
        plus = lambda x,y: x+'+'+y,
        view = lambda x: x,
        negate = '-',
        ),
    'sum' : Algebra(
        agg=lambda x: 'sum(x)',
        hom = lambda x: x,
        type = lambda x: x,
        zero = '0',
        plus = lambda x,y: x+'+'+y,
        view = lambda x: x,
        negate = '-',
        ),
    'hll' : Algebra(
        agg=lambda x:'hll_add_agg('+x+')',
        hom=lambda x:'hll_hash_anynull('+x+')',
        type = lambda x: 'hll',
        zero='hll_empty()',
        plus= lambda x,y: x+' || '+y,
        view= lambda x: 'floor(hll_cardinality('+x+'))',
        negate=None,
        ),
    }


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

    def __init__(self, table, temporary, tablespace, rollup, wheres, distincts, rollup_column):
        self.temporary = temporary
        self.table = table
        self.tablespace = tablespace
        self.rollup = rollup
        self.use_hll = True
        self.null_support = True
        self.rollup_column = rollup_column

        self.wheres = [Key(k.value, k.type, 'where_'+k.name,None) for k in wheres]
        self.distincts = [Key(k.value, k.type, k.name,algebras[k.algebra]) for k in distincts]

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
        temp_str = 'TEMPORARY ' if self.temporary else ''
        return (
f'''CREATE {temp_str}TABLE '''+self.rollup_table_name+''' ('''+
    (
    '''
    '''.join(['distinct_'+distinct.name + ' '+distinct.algebra.type(distinct.type) +' NOT NULL,' for distinct in self.distincts])+'''
    '''
    if self.use_hll else ''
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
        if inverse and not all([distinct.algebra.negate for distinct in self.distincts]):
            return '''
            RAISE EXCEPTION 'cannot % on table '''+self.rollup_table_name+''' with a rollup using op ''' + str([distinct.algebra.agg for distinct in self.distincts if not distinct.algebra.negate])+ '''', TG_OP;
            '''
        else:
            return '''
        '''.join([
            '''
            INSERT INTO '''+self.rollup_table_name+''' ('''+
                (
                '''
                '''.join(['distinct_'+distinct.name+',' for distinct in self.distincts])
                if self.use_hll else ''
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
            SELECT '''
            + '''
            '''.join([
                (distinct.algebra.negate if inverse else '')+' distinct_'+distinct.name+','
                for distinct in self.distincts
            ])
            +
            '''
            '''+('-' if inverse else '')+'''count
            '''
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
                '''
                '''.join([
                    'distinct_'+distinct.name+' = '+distinct.algebra.plus(
                        self.rollup_table_name+'.distinct_'+distinct.name,
                        'excluded.distinct_'+distinct.name+',' 
                        )
                    for distinct in self.distincts
                    ])
                if self.use_hll else ''
                )+
                '''
                count = '''+self.rollup_table_name+'''.count + excluded.count;
        '''+
        (''
        if self.null_support else ''
        )
        for binary in self.binaries])


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
                '''
                '''.join([
                    distinct.algebra.agg(
                        distinct.algebra.hom(distinct.value)
                        )+' AS distinct_'+distinct.name+','
                    for distinct in self.distincts
                    ])
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
            hll_cardinality(hll) AS num_distinct_url,
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
            '''
            '''.join([
                distinct.algebra.view('distinct_'+distinct.name)
                +' AS distinct_'+distinct.name+',' for distinct in self.distincts
                ])
            if self.use_hll else ''
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
        FROM '''+source+'''
        WHERE count != 0
        '''
        +
        (
        '''
        '''.join(['''
            AND ''' + distinct.algebra.view('distinct_'+distinct.name) 
                    + ' != ' 
                    + distinct.algebra.view(distinct.algebra.zero)
                for distinct in self.distincts
            ])
        )+'''
        ;''')


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
            '''
            '''.join(['distinct_'+distinct.name+',' for distinct in self.distincts])
            if self.use_hll else ''
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
            #self.create_indexes_wheres(),
            self.create_manualrollup(),
            self.create_triggerfunc(),
            #self.create_triggers(),
            self.create_view_groundtruth(),
            self.create_view_pretty(self.rollup_table_name,self.rollup),
            self.create_view_pretty(self.rollup+'_groundtruth_raw',self.rollup+'_groundtruth'),
            self.create_insert(),
            ]



def drop_rollup_str(rollup):
    rollup_table_name = rollup+'_raw'
    return ('''
    DROP TABLE '''+rollup_table_name+''' CASCADE;
    DROP VIEW '''+rollup+'''_groundtruth CASCADE;
    DROP FUNCTION '''+rollup_table_name+'''_triggerfunc CASCADE;
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
                Key('name','text','name','count'),
                Key('userid','int','userid','count'),
                ],
            rollup_column = None
            )
    #doctest.testmod(extraglobs={'t': t})
    print('\n'.join(t.create()))


