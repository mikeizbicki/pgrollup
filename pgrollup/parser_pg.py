import sys
from lark import Lark, Transformer

def parse_create(sql):
    '''
    This is the main function that will get called from postgresql.
    It converts the output of raw_parser into a list of dictionaries that contain the arguments for the create_view_internal function.
    
    >>> parse_create(sql0)
    [{'joininfos': '[{"table_name": "tablename", "table_alias": "tablename", "condition": "", "join_type": "FROM"}]', 'groups': [['a', '"a"'], ['b', '"b"'], ['c', '"c"']], 'columns': [['count(*)', '"count(*)"'], ['sum(num)', 'sum']], 'where_clause': '(test>=from)', 'having_clause': '(foo=bar)'}]
    >>> parse_create(sql1)
    [{'joininfos': '[{"table_name": "testparsing", "table_alias": "testparsing", "condition": "", "join_type": "FROM"}]', 'groups': [['name', '"name"']], 'columns': [['count(*)', 'count']], 'where_clause': None, 'having_clause': None}]
    >>> parse_create(sql2)
    [{'joininfos': '[{"table_name": "testparsing", "table_alias": "testparsing", "condition": "", "join_type": "FROM"}]', 'groups': [['name', '"name"'], ['num', '"num"']], 'columns': [['count(*)', 'count']], 'where_clause': None, 'having_clause': None}]
    >>> parse_create(sql3)
    [{'joininfos': '[{"table_name": "testparsing", "table_alias": "testparsing", "condition": "", "join_type": "FROM"}]', 'groups': [['name', '"name"']], 'columns': [['sum(num)', 'sum'], ['count(*)', 'count_all'], ['count(num)', '"count(num)"'], ['max(num)', '"max(num)"'], ['min(num)', '"min(num)"']], 'where_clause': None, 'having_clause': None}]
    >>> parse_create(sql4)
    [{'joininfos': '[{"table_name": "testparsing", "table_alias": "testparsing", "condition": "", "join_type": "FROM"}]', 'groups': None, 'columns': [['sum(num)', 'sum'], ['count(*)', 'count_all'], ['count(num)', '"count(num)"'], ['max(num)', '"max(num)"'], ['min(num)', '"min(num)"']], 'where_clause': None, 'having_clause': None}]
    >>> parse_create(sql5)
    [{'joininfos': '[{"table_name": "testparsing", "table_alias": "testparsing", "condition": "", "join_type": "FROM"}]', 'groups': None, 'columns': [['sum(num*num + 2)', '"sum(num*num + 2)"'], ['max(1)', '"max(1)"'], ['(max((1 +(((num))))*2)+ count(num))/count(*)+(max((1 +(((num))))*2)+ count(num))/count(*)', '"(max((1 +(((num))))*2)+ count(num))/count(*)+(max((1 +(((num))))*2)+ count(num))/count(*)"']], 'where_clause': None, 'having_clause': None}]
    >>> parse_create(sql6)
    [{'joininfos': '[{"table_name": "testjoin1", "table_alias": "testjoin1", "condition": "", "join_type": "FROM"}, {"table_name": "testjoin2", "table_alias": "testjoin2", "condition": "using (id)", "join_type": "INNER JOIN"}]', 'groups': [['name', '"name"']], 'columns': [['count(*)', 'count']], 'where_clause': None, 'having_clause': None}]
    >>> parse_create(sql7)
    [{'joininfos': '[{"table_name": "testjoin1", "table_alias": "INNER", "condition": "", "join_type": "FROM"}, {"table_name": "testjoin2", "table_alias": "testjoin2", "condition": "on testjoin1.id=testjoin2.id", "join_type": "INNER JOIN"}]', groups': [['name', '"name"']], 'columns': [['count(*)', 'count']], 'where_clause': None, 'having_clause': None}]
    >>> parse_create(sql8)
    [{'joininfos': '[{"table_name": "testjoin1", "table_alias": "t1", "condition": "", "join_type": "FROM"}, {"table_name": "testjoin2", "table_alias": "t2", "condition": "on testjoin1.id=testjoin2.id", "join_type": "INNER JOIN"}, {"table_name": "testjoin3", "table_alias": "t3", "condition": "on testjoin1.name=testjoin3.name", "join_type": "LEFT JOIN"}]', 'groups': [['name', '"name"']], 'columns': [['count(*)', 'count']], 'where_clause': None, 'having_clause': None}]
    >>> parse_create(sql9)
    [{'joininfos': '[{"table_name": "testjoin1", "table_alias": "testjoin1", "condition": "", "join_type": "FROM"}, {"table_name": "testjoin2", "table_alias": "testjoin2", "condition": "using (id)", "join_type": "FULL JOIN"}]', 'groups': [['name', '"name"']], 'columns': [['sum(num)', '"sum(num)"'], ['sum(foo)', '"sum(foo)"']], 'where_clause': None, 'having_clause': None}]
    >>> parse_create(sql10)
    [{'joininfos': '[{"table_name": "testjoin1", "table_alias": "testjoin1", "condition": "", "join_type": "FROM"}, {"table_name": "testjoin2", "table_alias": "testjoin2", "condition": "using (id)", "join_type": "INNER JOIN"}]', 'groups': [['name', '"name"']], 'columns': [['sum(num)', '"sum(num)"']], 'where_clause': None, 'having_clause': None}]
    >>> parse_create(sql11)
    [{'joininfos': '[{"table_name": "testjoin1", "table_alias": "t1", "condition": "", "join_type": "FROM"}, {"table_name": "testjoin2", "table_alias": "t2", "condition": "using (id)", "join_type": "INNER JOIN"}]', 'groups': [['t1.name', '"t1.name"']], 'columns': [['sum(t1.num)', 'sum_num'], ['sum(t2.foo)', 'sum_foo']], 'where_clause': None, 'having_clause': None}]
    >>> parse_create(sql12)
    [{'joininfos': '[{"table_name": "testjoin1", "table_alias": "t1", "condition": "", "join_type": "FROM"}, {"table_name": "testjoin1", "table_alias": "t2", "condition": "on ((t1.id = t2.num))", "join_type": "INNER JOIN"}, {"table_name": "testjoin1", "table_alias": "t3", "condition": "on ((t2.id = t3.num))", "join_type": "INNER JOIN"}, {"table_name": "testjoin1", "table_alias": "t4", "condition": "on ((t3.id = t4.num))", "join_type": "INNER JOIN"}, {"table_name": "testjoin1", "table_alias": "t5", "condition": "on ((t4.id = t5.num))", "join_type": "INNER JOIN"}, {"table_name": "testjoin1", "table_alias": "t6", "condition": "on ((t5.id = t6.num))", "join_type": "INNER JOIN"}, {"table_name": "testjoin1", "table_alias": "t7", "condition": "on ((t6.id = t7.num))", "join_type": "INNER JOIN"}]', 'groups': [['t1.name', '"t1.name"']], 'columns': [['count(t1.num)', 'count_t1'], ['count(t2.num)', 'count_t2']], 'where_clause': None, 'having_clause': None}]
    '''
    parsed_output = parse(sql)
    infos = []                                              
    for x in parsed_output[0]['stmt']['targetList']:
        if x['name']!= None:
           alias = x['name'][1]
        else:
           alias = ''
    
    infos.append({
        'joininfos' :_getjoins(parsed_output[0]['stmt']['fromClause'][0]),
        'groups' : [[x['fields'][0],str(x['fields'][0])] for x in parsed_output[0]['stmt']['groupClause']] if parsed_output[0]['stmt']['groupClause'] != None else '',
        'columns': [[expr_to_str(x['val']),alias] for x in parsed_output[0]['stmt']['targetList']],
        'where_clause': expr_to_str(parsed_output[0]['stmt']['whereClause']) if parsed_output[0]['stmt']['whereClause'] != None else None,
        'having_clause': expr_to_str(parsed_output[0]['stmt']['havingClause']) if parsed_output[0]['stmt']['havingClause'] != None else None,
        })
    return infos


grammar = r"""
    ?value: dict
          | list
          | ESCAPED_STRING     ->string
          | NAME               -> name   
          | SIGNED_NUMBER      -> number
          | "true"             -> true
          | "false"            -> false
          | "<>"               -> null
    
    list: "(" [value*] ")"
    dict: "{" value [pair*] "}"
    pair : ":"NAME value

    NAME: /[a-zA-Z_.0-9]+/
    

    %import common.ESCAPED_STRING
    %import common.SIGNED_NUMBER
    %import common.WS
    %ignore WS
    """


class Transformer(Transformer):
    '''
    See https://lark-parser.readthedocs.io/en/latest/visitors.html    
    '''
    def string(self, s):
        (s,) = s
        return s[1:-1]
    
    
    def number(self, n):
        (n,) = n
        return int(n)                        
    
    
    def pair(self,key_value):
        k, v = key_value
        return str(k),v
                       
    list = list   
    dict = dict
    name  = lambda self,n: ('TYPE',str(n[0]))
    null = lambda self, _: None
    true = lambda self, _: True
    false = lambda self, _: False

parser = Lark(grammar, start='value', lexer='standard')


def parse(text):
    '''
    This function is used to parse the output of raw_parser.
    
    >>> parse(sql0)
    [{'TYPE': 'RAWSTMT', 'stmt': {'TYPE': 'SELECT', 'distinctClause': None, 'intoClause': None, 'targetList': [{'TYPE': 'RESTARGET', 'name': None, 'indirection': None, 'val': {'TYPE': 'FUNCCALL', 'funcname': ['count'], 'args': None, 'agg_order': None, 'agg_filter': None, 'agg_within_group': False, 'agg_star': True, 'agg_distinct': False, 'func_variadic': False, 'over': None, 'location': 16}, 'location': 16}, {'TYPE': 'RESTARGET', 'name': ('TYPE', 'sum'), 'indirection': None, 'val': {'TYPE': 'FUNCCALL', 'funcname': ['sum'], 'args': [{'TYPE': 'COLUMNREF', 'fields': ['num'], 'location': 38}], 'agg_order': None, 'agg_filter': None, 'agg_within_group': False, 'agg_star': False, 'agg_distinct': False, 'func_variadic': False, 'over': None, 'location': 34}, 'location': 34}], 'fromClause': [{'TYPE': 'RANGEVAR', 'schemaname': None, 'relname': ('TYPE', 'tablename'), 'inh': True, 'relpersistence': ('TYPE', 'p'), 'alias': None, 'location': 59}], 'whereClause': {'TYPE': 'AEXPR', 'name': ['>='], 'lexpr': {'TYPE': 'COLUMNREF', 'fields': ['test'], 'location': 80}, 'rexpr': {'TYPE': 'COLUMNREF', 'fields': ['from'], 'location': 86}, 'location': 84}, 'groupClause': [{'TYPE': 'COLUMNREF', 'fields': ['a'], 'location': 107}, {'TYPE': 'COLUMNREF', 'fields': ['b'], 'location': 109}, {'TYPE': 'COLUMNREF', 'fields': ['c'], 'location': 111}], 'havingClause': {'TYPE': 'AEXPR', 'name': ['='], 'lexpr': {'TYPE': 'COLUMNREF', 'fields': ['foo'], 'location': 124}, 'rexpr': {'TYPE': 'COLUMNREF', 'fields': ['bar'], 'location': 128}, 'location': 127}, 'windowClause': None, 'valuesLists': None, 'sortClause': None, 'limitOffset': None, 'limitCount': None, 'limitOption': 0, 'lockingClause': None, 'withClause': None, 'op': 0, 'all': False, 'larg': None, 'rarg': None}, 'stmt_location': 0, 'stmt_len': 132}]
    '''
    tree = parser.parse(text)
    return Transformer().transform(tree)


def parse_tree(text):
    print(parser.parse(text).pretty())

################################################################################
# internal helper functions
################################################################################

def expr_to_str(expr):
    '''
    This function transforms expressions into strings.
    
    >>> assert expr_to_str(parse(sql0)[0]['stmt']['havingClause'])
    '(foo=bar)' 
    '''
    if expr['TYPE']=='A_CONST':
        return str(expr['val'])
    elif expr['TYPE']=='COLUMNREF':
        return str(expr['fields'][0])
    elif expr['TYPE']=='FUNCCALL':
        if expr['agg_star']==True:
            return expr['funcname'][0]+'(*)'
        else:
            return expr['funcname'][0]+'('+expr_to_str(expr['args'][0])+')'
    elif expr['TYPE']=='AEXPR':
        return '('+expr_to_str(expr['lexpr']) + expr['name'][0] + expr_to_str(expr['rexpr'])+')'


def _getjointype(info):
    '''
    This function returns a query's join type.
    '''
    if info['jointype'] == 0:
        return "INNER JOIN"
    elif info['jointype'] ==1:
        return "OUTER JOIN"
    elif info['jointype'] ==2:
        return "FULL JOIN"


def _getjoins(info):
    '''
    This function generates a list that contains join information.
    
    >>> _getjoins(parse(sql7)[0]['stmt']['fromClause'][0])
    [[{'table_name': 'testjoin1', 'table_alias': 'testjoin1', 'condition': '', 'join_type': 'FROM'}], [{'table_name': 'testjoin2', 'table_alias': 'testjoin2', 'condition': 'on(testjoin1=testjoin2)', 'join_type': 'INNER JOIN'}]]
    '''
    join_infos = []
    if info['TYPE']=='RANGEVAR':
        table_name = str(info['relname'][1])
        if info['alias']!= None:
            table_alias=str(info['alias']['aliasname'][1])
        else:    
            table_alias = str(info['relname'][1])
        join_type= 'FROM'
        condition= ''
        join_info = {
            'table_name': table_name,
            'table_alias': table_alias,
            'condition': condition,
            'join_type': join_type
       }
        join_infos.append(join_info)
        return join_infos
    elif info['TYPE']=='JOINEXPR':
        if info['usingClause']!= None:
            condition = 'using('+str(info['usingClause'])+')'
        else:
            condition = 'on'+expr_to_str(info['quals'])
        larg = _getjoins(info['larg'])
        rarg = _getjoins(info['rarg'])
        rarg[0]['join_type'] = _getjointype(info)
        if info['usingClause']!= None:
            rarg[0]['condition'] = 'using('+str(info['usingClause'][0])+')'
        else:
            rarg[0]['condition'] = 'on'+expr_to_str(info['quals'])
        join_infos.append(larg)
        join_infos.append(rarg)
        return join_infos
              
################################################################################
# postgres's parse tree on example sql expressions
################################################################################

#sql0 = '''
#CREATE INCREMENTAL MATERIALIZED VIEW example AS (
#    SELECT
#        count(*),
#        sum(num) AS sum
#    FROM tablename
#    WHERE (test>=from)
#    GROUP BY a,b,c
#    HAVING foo=bar
#);
#'''
sql0='''
 ({RAWSTMT :stmt {SELECT :distinctClause <> :intoClause <> :targetList ({RESTARGET :name <> :indirection <> :val {FUNCCALL :funcname ("count") :args <> :agg_order <> :agg_filter <> :agg_within_group false :agg_star true :agg_distinct false :func_variadic false :over <> :location 16} :location 16} {RESTARGET :name sum :indirection <> :val {FUNCCALL :funcname ("sum") :args ({COLUMNREF :fields ("num") :location 38}) :agg_order <> :agg_filter <> :agg_within_group false :agg_star false :agg_distinct false :func_variadic false :over <> :location 34} :location 34}) :fromClause ({RANGEVAR :schemaname <> :relname tablename :inh true :relpersistence p :alias <> :location 59}) :whereClause {AEXPR  :name (">=") :lexpr {COLUMNREF :fields ("test") :location 80} :rexpr {COLUMNREF :fields ("from") :location 86} :location 84} :groupClause ({COLUMNREF :fields ("a") :location 107} {COLUMNREF :fields ("b") :location 109} {COLUMNREF :fields ("c") :location 111}) :havingClause {AEXPR  :name ("=") :lexpr {COLUMNREF :fields ("foo") :location 124} :rexpr {COLUMNREF :fields ("bar") :location 128} :location 127} :windowClause <> :valuesLists <> :sortClause <> :limitOffset <> :limitCount <> :limitOption 0 :lockingClause <> :withClause <> :op 0 :all false :larg <> :rarg <>} :stmt_location 0 :stmt_len 132})
'''

sql1='''
 ({RAWSTMT :stmt {SELECT :distinctClause <> :intoClause <> :targetList ({RESTARGET :name count :indirection <> :val {FUNCCALL :funcname ("count") :args <> :agg_order <> :agg_filter <> :agg_within_group false :agg_star true :agg_distinct false :func_variadic false :over <> :location 16} :location 16}) :fromClause ({RANGEVAR :schemaname <> :relname testparsing :inh true :relpersistence p :alias <> :location 43}) :whereClause <> :groupClause ({COLUMNREF :fields ("name") :location 68}) :havingClause <> :windowClause <> :valuesLists <> :sortClause <> :limitOffset <> :limitCount <> :limitOption 0 :lockingClause<> :withClause <> :op 0 :all false :larg <> :rarg <>} :stmt_location 0 :stmt_len 73})
'''

sql2='''
({RAWSTMT :stmt {SELECT :distinctClause <> :intoClause <> :targetList ({RESTARGET :name count :indirection <> :val {FUNCCALL :funcname ("count") :args <> :agg_order <> :agg_filter <> :agg_within_group false :agg_star true :agg_distinct false :func_variadic false :over <> :location 8} :location 8}) :fromClause ({RANGEVAR :schemaname <> :relname testparsing :inh true :relpersistence p :alias <> :location 35}) :whereClause <> :groupClause ({COLUMNREF :fields ("name") :location 60} {COLUMNREF :fields ("num") :location 65}) :havingClause <> :windowClause <> :valuesLists <> :sortClause <> :limitOffset <> :limitCount <> :limitOption 0 :lockingClause <> :withClause <> :op 0 :all false :larg <> :rarg <>} :stmt_location 0 :stmt_len 69})
'''

sql3='''
({RAWSTMT :stmt {SELECT :distinctClause <> :intoClause <> :targetList ({RESTARGET :name sum :indirection <> :val {FUNCCALL :funcname ("sum") :args ({COLUMNREF :fields ("num") :location 21}) :agg_order <> :agg_filter <> :agg_within_group false :agg_star false :agg_distinct false :func_variadic false :over <> :location 17} :location 17} {RESTARGET :name count_all :indirection <> :val {FUNCCALL :funcname ("count") :args <> :agg_order <> :agg_filter <> :agg_within_group false :agg_star true :agg_distinct false :func_variadic false :over <> :location 42} :location 42} {RESTARGET :name <> :indirection <> :val{FUNCCALL :funcname ("count") :args ({COLUMNREF :fields ("num") :location 79}) :agg_order <> :agg_filter <> :agg_within_group false :agg_star false :agg_distinct false :func_variadic false :over <> :location 73} :location 73} {RESTARGET :name <> :indirection <> :val {FUNCCALL :funcname ("max") :args ({COLUMNREF :fields ("num") :location 97}) :agg_order <> :agg_filter <> :agg_within_group false :agg_star false :agg_distinct false :func_variadic false :over <> :location 93} :location 93} {RESTARGET :name <> :indirection <> :val {FUNCCALL :funcname ("min") :args ({COLUMNREF :fields ("num") :location 115}) :agg_order <> :agg_filter <> :agg_within_group false :agg_star false :agg_distinct false :func_variadic false :over <> :location 111} :location 111}) :fromClause ({RANGEVAR :schemaname <> :relname testparsing :inh true :relpersistence p :alias <> :location 129}) :whereClause <> :groupClause ({COLUMNREF :fields ("name") :location 154}) :havingClause <> :windowClause <> :valuesLists <> :sortClause <> :limitOffset <> :limitCount <> :limitOption 0 :lockingClause <> :withClause <> :op 0 :all false :larg <> :rarg <>} :stmt_location 0 :stmt_len 159})
'''

sql4='''
({RAWSTMT :stmt {SELECT :distinctClause <> :intoClause <> :targetList ({RESTARGET :name sum :indirection <> :val {FUNCCALL :funcname ("sum") :args ({COLUMNREF :fields ("num") :location 20}) :agg_order <> :agg_filter <> :agg_within_group false :agg_star false :agg_distinct false :func_variadic false :over <> :location 16} :location 16} {RESTARGET :name count_all :indirection <> :val {FUNCCALL :funcname ("count") :args <> :agg_order <> :agg_filter <> :agg_within_group false :agg_star true :agg_distinct false :func_variadic false :over <> :location 41} :location 41} {RESTARGET :name <> :indirection <> :val{FUNCCALL :funcname ("count") :args ({COLUMNREF :fields ("num") :location 78}) :agg_order <> :agg_filter <> :agg_within_group false :agg_star false :agg_distinct false :func_variadic false :over <> :location 72} :location 72} {RESTARGET :name <> :indirection <> :val {FUNCCALL :funcname ("max") :args ({COLUMNREF :fields ("num") :location 96}) :agg_order <> :agg_filter <> :agg_within_group false :agg_star false :agg_distinct false :func_variadic false :over <> :location 92} :location 92} {RESTARGET :name <> :indirection <> :val {FUNCCALL :funcname ("min") :args ({COLUMNREF :fields ("num") :location 114}) :agg_order <> :agg_filter <> :agg_within_group false :agg_star false :agg_distinct false :func_variadic false :over <> :location 110} :location 110}) :fromClause ({RANGEVAR :schemaname <> :relname testparsing :inh true :relpersistence p :alias <> :location 128}) :whereClause <> :groupClause <> :havingClause <> :windowClause <> :valuesLists <> :sortClause <> :limitOffset <> :limitCount <> :limitOption 0 :lockingClause <> :withClause <> :op 0 :all false :larg <> :rarg <>} :stmt_location 0 :stmt_len 140})
'''

sql5='''
({RAWSTMT :stmt {SELECT :distinctClause <> :intoClause <> :targetList ({RESTARGET :name <> :indirection <> :val {FUNCCALL :funcname ("sum") :args ({AEXPR  :name ("+") :lexpr {AEXPR  :name ("*") :lexpr {COLUMNREF :fields ("num") :location 20} :rexpr {COLUMNREF :fields ("num") :location 24} :location 23} :rexpr {A_CONST :val 2 :location 30} :location 28}) :agg_order <> :agg_filter <> :agg_within_group false :agg_star false :agg_distinct false :func_variadic false :over <> :location 16} :location 16} {RESTARGET :name <> :indirection <> :val {FUNCCALL :funcname ("max") :args ({A_CONST :val 1 :location 46}) :agg_order <> :agg_filter <> :agg_within_group false :agg_star false :agg_distinct false :func_variadic false :over <> :location 42} :location 42} {RESTARGET :name <> :indirection <> :val {AEXPR  :name ("+") :lexpr {AEXPR  :name ("/") :lexpr {AEXPR  :name ("+") :lexpr {FUNCCALL :funcname ("max") :args ({AEXPR  :name ("*") :lexpr {AEXPR  :name ("+") :lexpr {A_CONST :val 1 :location 64} :rexpr {COLUMNREF :fields ("num") :location 71} :location 66} :rexpr {A_CONST :val 2 :location 79} :location 78}) :agg_order <> :agg_filter <> :agg_within_group false :agg_star false :agg_distinct false :func_variadic false :over <> :location 59} :rexpr {FUNCCALL :funcname ("count") :args ({COLUMNREF :fields ("num") :location 90}) :agg_order <> :agg_filter <> :agg_within_group false :agg_star false :agg_distinct false :func_variadic false :over <> :location 84} :location 82} :rexpr {FUNCCALL :funcname ("count") :args <> :agg_order <> :agg_filter <> :agg_within_group false :agg_star true :agg_distinct false :func_variadic false :over <> :location 96} :location 95} :rexpr {AEXPR  :name ("/") :lexpr {AEXPR  :name ("+") :lexpr {FUNCCALL :funcname ("max") :args ({AEXPR  :name ("*") :lexpr {AEXPR  :name ("+") :lexpr {A_CONST :val 1 :location 121} :rexpr {COLUMNREF :fields ("num") :location 128} :location 123} :rexpr {A_CONST :val 2 :location 136} :location 135}) :agg_order <> :agg_filter <> :agg_within_group false :agg_star false :agg_distinct false :func_variadic false :over <> :location 116} :rexpr {FUNCCALL :funcname ("count") :args ({COLUMNREF :fields ("num") :location 147}) :agg_order <> :agg_filter <> :agg_within_group false:agg_star false :agg_distinct false :func_variadic false :over <> :location 141} :location 139} :rexpr {FUNCCALL :funcname ("count") :args <> :agg_order <> :agg_filter <> :agg_within_group false :agg_star true :agg_distinct false :func_variadic false :over <> :location 153} :location 152} :location 113} :location 58}) :fromClause ({RANGEVAR :schemaname <> :relname testparsing :inh true :relpersistence p :alias <> :location 171}) :whereClause <> :groupClause <> :havingClause <> :windowClause <> :valuesLists <> :sortClause <> :limitOffset <> :limitCount <> :limitOption 0 :lockingClause <> :withClause <> :op 0 :all false :larg <> :rarg <>} :stmt_location 0 :stmt_len 184})
 '''

sql6='''
({RAWSTMT :stmt {SELECT :distinctClause <> :intoClause <> :targetList ({RESTARGET :name count :indirection <> :val {FUNCCALL :funcname ("count") :args <> :agg_order <> :agg_filter <> :agg_within_group false :agg_star true :agg_distinct false :func_variadic false :over <> :location 17} :location 17}) :fromClause ({JOINEXPR :jointype 0 :isNatural false :larg {RANGEVAR :schemaname <> :relname testjoin1 :inh true :relpersistence p :alias <> :location 44} :rarg {RANGEVAR :schemaname <> :relname testjoin2 :inh true :relpersistence p :alias <> :location 63} :usingClause ("id") :quals <> :alias <> :rtindex 0}) :whereClause <> :groupClause ({COLUMNREF :fields ("name") :location 97}) :havingClause <> :windowClause <> :valuesLists <> :sortClause <> :limitOffset <> :limitCount <> :limitOption 0 :lockingClause <> :withClause <> :op 0 :all false :larg <> :rarg <>} :stmt_location 0 :stmt_len 102})
'''

sql7='''
({RAWSTMT :stmt {SELECT :distinctClause <> :intoClause <> :targetList ({RESTARGET :name count :indirection <> :val {FUNCCALL :funcname ("count") :args <> :agg_order <> :agg_filter <> :agg_within_group false :agg_star true :agg_distinct false :func_variadic false :over <> :location 16} :location 16}) :fromClause ({JOINEXPR :jointype 0 :isNatural false :larg {RANGEVAR :schemaname <> :relname testjoin1 :inh true :relpersistence p :alias <> :location 43} :rarg {RANGEVAR :schemaname <> :relname testjoin2 :inh true :relpersistence p :alias <> :location 68} :usingClause <> :quals {AEXPR  :name ("=") :lexpr {COLUMNREF :fields ("testjoin1" "id") :location 81} :rexpr {COLUMNREF :fields ("testjoin2" "id") :location 94} :location 93} :alias <> :rtindex 0}) :whereClause <> :groupClause ({COLUMNREF :fields ("name") :location 120}) :havingClause <> :windowClause <> :valuesLists <> :sortClause <> :limitOffset <> :limitCount <> :limitOption 0 :lockingClause <> :withClause <> :op 0 :all false :larg <> :rarg <>} :stmt_location 0 :stmt_len 125})
'''

sql8='''
({RAWSTMT :stmt {SELECT :distinctClause <> :intoClause <> :targetList ({RESTARGET :name count :indirection <> :val {FUNCCALL :funcname ("count") :args <> :agg_order <> :agg_filter <> :agg_within_group false :agg_star true :agg_distinct false :func_variadic false :over <> :location 16} :location 16}) :fromClause ({JOINEXPR :jointype 1 :isNatural false :larg {JOINEXPR :jointype 0 :isNatural false :larg {RANGEVAR :schemaname <> :relname testjoin1 :inh true :relpersistence p :alias {ALIAS :aliasname t1 :colnames <>} :location 43} :rarg {RANGEVAR :schemaname <> :relname testjoin2 :inh true :relpersistence p :alias {ALIAS :aliasname t2 :colnames <>} :location 74} :usingClause <> :quals {AEXPR  :name ("=") :lexpr {COLUMNREF :fields ("testjoin1" "id") :location 93} :rexpr {COLUMNREF :fields ("testjoin2" "id") :location 106} :location 105} :alias <> :rtindex 0} :rarg {RANGEVAR :schemaname <> :relname testjoin3 :inh true :relpersistence p :alias {ALIAS :aliasname t3 :colnames <>} :location 139} :usingClause <> :quals {AEXPR  :name ("=") :lexpr {COLUMNREF :fields ("testjoin1" "name") :location 158} :rexpr {COLUMNREF :fields ("testjoin3" "name") :location 173} :location 172} :alias <> :rtindex 0}) :whereClause <> :groupClause ({COLUMNREF :fields ("name") :location 201}) :havingClause <> :windowClause <> :valuesLists <> :sortClause <> :limitOffset <> :limitCount <> :limitOption 0 :lockingClause <> :withClause <> :op 0 :all false :larg <> :rarg <>} :stmt_location 0 :stmt_len 207})
'''

sql9='''
({RAWSTMT :stmt {SELECT :distinctClause <> :intoClause <> :targetList ({RESTARGET :name <> :indirection <> :val {FUNCCALL :funcname ("sum") :args ({COLUMNREF :fields ("num") :location 21}) :agg_order <>:agg_filter <> :agg_within_group false :agg_star false :agg_distinct false :func_variadic false :over <> :location 17} :location 17} {RESTARGET :name <> :indirection <> :val {FUNCCALL :funcname ("sum") :args ({COLUMNREF :fields ("foo") :location 39}) :agg_order <> :agg_filter <> :agg_within_group false :agg_star false :agg_distinct false :func_variadic false :over <> :location 35} :location 35}) :fromClause ({JOINEXPR :jointype 2 :isNatural false :larg {RANGEVAR :schemaname <> :relname testjoin1 :inh true :relpersistence p :alias <> :location 53} :rarg {RANGEVAR :schemaname <> :relname testjoin2 :inh true :relpersistence p :alias <> :location 77} :usingClause ("id") :quals <> :alias <> :rtindex 0}) :whereClause <> :groupClause ({COLUMNREF :fields ("name") :location 111}) :havingClause <> :windowClause <>:valuesLists <> :sortClause <> :limitOffset <> :limitCount <> :limitOption 0 :lockingClause <> :withClause <> :op 0 :all false :larg <> :rarg <>} :stmt_location 0 :stmt_len 116})
'''

sql10='''
({RAWSTMT :stmt {SELECT :distinctClause <> :intoClause <> :targetList ({RESTARGET :name <> :indirection <> :val {FUNCCALL :funcname ("sum") :args ({COLUMNREF :fields ("num") :location 20}) :agg_order <>:agg_filter <> :agg_within_group false :agg_star false :agg_distinct false :func_variadic false :over <> :location 16} :location 16}) :fromClause ({JOINEXPR :jointype 0 :isNatural false :larg {RANGEVAR :schemaname <> :relname testjoin1 :inh true :relpersistence p :alias <> :location 34} :rarg {RANGEVAR :schemaname <> :relname testjoin2 :inh true :relpersistence p :alias <> :location 53} :usingClause ("id") :quals <> :alias <> :rtindex 0}) :whereClause <> :groupClause ({COLUMNREF :fields ("name") :location 87}) :havingClause <> :windowClause <> :valuesLists <> :sortClause <> :limitOffset <> :limitCount <>:limitOption 0 :lockingClause <> :withClause <> :op 0 :all false :larg <> :rarg <>} :stmt_location 0 :stmt_len 92})
'''

sql11='''
({RAWSTMT :stmt {SELECT :distinctClause <> :intoClause <> :targetList ({RESTARGET :name sum_num :indirection <> :val {FUNCCALL :funcname ("sum") :args ({COLUMNREF :fields ("t1" "num") :location 12}) :agg_order <> :agg_filter <> :agg_within_group false :agg_star false :agg_distinct false :func_variadic false :over <> :location 8} :location 8} {RESTARGET :name sum_foo :indirection <> :val {FUNCCALL :funcname ("sum") :args ({COLUMNREF :fields ("t2" "foo") :location 40}) :agg_order <> :agg_filter <> :agg_within_group false :agg_star false :agg_distinct false :func_variadic false :over <> :location 36} :location 36}) :fromClause ({JOINEXPR :jointype 0 :isNatural false :larg {RANGEVAR :schemaname <> :relname testjoin1 :inh true :relpersistence p :alias {ALIAS :aliasname t1 :colnames <>} :location 68} :rarg {RANGEVAR :schemaname <> :relname testjoin2 :inh true :relpersistence p :alias {ALIAS :aliasname t2 :colnames <>} :location 91} :usingClause ("id") :quals <> :alias <> :rtindex 0}) :whereClause <> :groupClause ({COLUMNREF :fields ("t1" "name") :location 127}) :havingClause <> :windowClause <> :valuesLists <> :sortClause <> :limitOffset <> :limitCount <> :limitOption 0 :lockingClause <> :withClause <> :op 0 :all false :larg <> :rarg <>} :stmt_location 0 :stmt_len 135})
'''

sql12='''
({RAWSTMT :stmt {SELECT :distinctClause <> :intoClause <> :targetList ({RESTARGET :name count_t1 :indirection <> :val {FUNCCALL :funcname ("count") :args ({COLUMNREF :fields ("t1" "num") :location 14}) :agg_order <> :agg_filter <> :agg_within_group false :agg_star false :agg_distinct false :func_variadic false :over <> :location 8} :location 8} {RESTARGET :name count_t2 :indirection <> :val {FUNCCALL :funcname ("count") :args ({COLUMNREF :fields ("t2" "num") :location 45}) :agg_order <> :agg_filter <> :agg_within_group false :agg_star false :agg_distinct false :func_variadic false :over <> :location 39}:location 39}) :fromClause ({JOINEXPR :jointype 0 :isNatural false :larg {JOINEXPR :jointype 0 :isNatural false :larg {JOINEXPR :jointype 0 :isNatural false :larg {JOINEXPR :jointype 0 :isNatural false :larg {JOINEXPR :jointype 0 :isNatural false :larg {JOINEXPR :jointype 0 :isNatural false :larg {RANGEVAR :schemaname <> :relname testjoin1 :inh true :relpersistence p :alias {ALIAS :aliasname t1 :colnames<>} :location 79} :rarg {RANGEVAR :schemaname <> :relname testjoin1 :inh true :relpersistence p :alias {ALIAS :aliasname t2 :colnames <>} :location 102} :usingClause <> :quals {AEXPR  :name ("=") :lexpr {COLUMNREF :fields ("t1" "id") :location 120} :rexpr {COLUMNREF :fields ("t2" "num") :location 128} :location 126} :alias <> :rtindex 0} :rarg {RANGEVAR :schemaname <> :relname testjoin1 :inh true :relpersistence p :alias {ALIAS :aliasname t3 :colnames <>} :location 148} :usingClause <> :quals {AEXPR  :name ("=") :lexpr {COLUMNREF :fields ("t2" "id") :location 166} :rexpr {COLUMNREF :fields ("t3" "num") :location 174} :location 172} :alias <> :rtindex 0} :rarg {RANGEVAR :schemaname <> :relname testjoin1 :inh true :relpersistence p :alias {ALIAS :aliasname t4 :colnames <>} :location 194} :usingClause <> :quals {AEXPR  :name ("=") :lexpr {COLUMNREF :fields ("t3" "id") :location 212} :rexpr {COLUMNREF :fields ("t4" "num") :location 220} :location 218} :alias <> :rtindex 0} :rarg {RANGEVAR :schemaname <> :relname testjoin1 :inh true :relpersistence p :alias {ALIAS :aliasname t5 :colnames <>} :location 240} :usingClause <> :quals {AEXPR  :name ("=") :lexpr {COLUMNREF :fields ("t4" "id") :location 258} :rexpr {COLUMNREF :fields ("t5" "num") :location 266} :location 264} :alias <> :rtindex 0} :rarg {RANGEVAR :schemaname <> :relname testjoin1 :inh true :relpersistence p :alias {ALIAS :aliasname t6 :colnames <>} :location 286} :usingClause <> :quals {AEXPR  :name ("=") :lexpr {COLUMNREF :fields ("t5" "id") :location 304} :rexpr {COLUMNREF :fields ("t6" "num") :location 312} :location 310} :alias <> :rtindex 0} :rarg {RANGEVAR :schemaname <> :relname testjoin1 :inh true :relpersistence p :alias {ALIAS :aliasname t7 :colnames <>} :location 332} :usingClause <> :quals {AEXPR  :name ("=") :lexpr {COLUMNREF :fields ("t6" "id") :location 350} :rexpr {COLUMNREF :fields ("t7" "num") :location 358} :location 356} :alias <> :rtindex 0}) :whereClause <> :groupClause ({COLUMNREF :fields ("t1" "name") :location 379}) :havingClause <> :windowClause <> :valuesLists <> :sortClause <> :limitOffset <> :limitCount <> :limitOption 0 :lockingClause <> :withClause <> :op 0 :all false :larg <> :rarg <>} :stmt_location 0 :stmt_len 388})
'''


