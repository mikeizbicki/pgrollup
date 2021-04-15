'''
FIXME:
We should use postgres's parse tree rather than generate our own.
See https://wiki.postgresql.org/wiki/Query_Parsing
'''

import json
import pprint
from lark import Lark


def parse_create(text):
    '''
    This is the main function that will get called from postgresql.
    It converts input sql commands into a list of dictionaries that contain the arguments for the create_view_internal function.

    >>> assert parse_create(sql0)
    >>> assert parse_create(sql1)
    >>> assert parse_create(sql2)
    >>> assert parse_create(sql3)
    >>> assert parse_create(sql4)
    >>> assert parse_create(sql5)
    >>> assert parse_create(sql6)
    >>> assert parse_create(sql7)
    >>> assert parse_create(sql8)
    >>> assert parse_create(sql9)
    >>> assert parse_create(sql10)
    >>> assert parse_create(sql11)
    >>> assert parse_create(sql12)
    >>> len(parse_create(sql0+sql1+sql2+sql3+sql4))
    5
    '''
    tree = grammar.parse(text)
    infos = []
    for child in tree.find_data('create_view'):
        infos.append({
            'joininfos' : _getjoins(child),
            'rollup_name' : _getvalue(child,'rollup_name'),
            'groups' : _getvalue(child,'group_clause'),
            'columns' : _getvalue(child,'columns'),
            'where_clause' : _getvalue(child,'where_clause'),
            'having_clause' : _getvalue(child,'having_clause'),
            })
    return infos


# this is the simplified grammar we use for parsing;
# it doesn't support all of postgres's syntax;
# in particular, the blurbs field is used to capture arbitrary strings that may or may not be valid sql expressions,
# but implementing a full sql parser seemed like an unrealistic task
grammar = Lark(r"""
    start: cmd ";" start
         | cmd ";"?

    cmd: create_view

    create_view: "CREATE"i "INCREMENTAL"i "MATERIALIZED"i "VIEW"i rollup_name "AS"i "(" select ")"

    select: "SELECT"i columns select_source where_clause? group_clause? having_clause?

    select_source: "FROM"i from joins? | "FROM"i "("+ from joins? 
    from: table_name ("AS"i? table_alias)? 
    joins: join | join joins
    join: join_type table_name ("AS"i? table_alias)? join_condition ")"?
    join_type: join_inner | join_left | join_right | join_full
    join_inner: "INNER"i? "JOIN"i
    join_left: "LEFT"i "OUTER"i? "JOIN"i
    join_right: "RIGHT"i "OUTER"i? "JOIN"i
    join_full: "FULL"i "OUTER"i? "JOIN"i
    join_condition: using_condition | on_condition
    using_condition: "USING"i "(" using_column_name ")"
    on_condition: "ON"i blurb

    columns: blurbs
    where_clause: "WHERE"i blurb
    group_clause: "GROUP"i "BY"i blurbs
    having_clause: "HAVING"i blurb
    
    blurbs: blurbitem COMMA blurbs | blurbitem 
    blurbitem: blurb as_clause?
    as_clause: "AS"i blurb
    blurb: blurb_rec
    blurb_rec: blurb_word blurb_rec | blurb_word
    blurb_word: /((?!(GROUP)|(HAVING)|(FROM)|(LEFT)|(RIGHT)|(FULL)|(INNER)|(OUTER)|(AS)|[(),])(.|\s))+/i
              | LPAREN blurb_inparen_rec RPAREN
    blurb_inparen_rec: blurb_inparen blurb_inparen_rec | blurb_inparen
    blurb_inparen: /[^()]+/
                 | LPAREN blurb_inparen_rec RPAREN

    rollup_name: NAME
    table_name: NAME
    table_alias: NAME
    join_table_name: NAME
    using_column_name: NAME

    NAME: /[a-zA-Z_.0-9]+/ 
    LPAREN: "("
    RPAREN: ")"
    COMMA: ","

    %import common.ESCAPED_STRING
    %import common.SIGNED_NUMBER
    %import common.WS
    %ignore WS
    """)

################################################################################
# internal helper functions
################################################################################


def _getjoins(tree):
    results = list(tree.find_data('from')) + list(reversed(list(tree.find_data('join'))))
    joininfos = []
    for result in results:
        table_name = _getvalue(result,'table_name')

        # extract the alias
        table_alias = _getvalue(result, 'table_alias')
        if table_alias is None:
            table_alias = table_name
        
        # extract the join type
        def has_field(x):
            return len(list(result.find_data(x)))>0
        if has_field('from'):
            join_type='FROM'
        elif has_field('join_inner'):
            join_type='INNER JOIN'
        elif has_field('join_left'):
            join_type='LEFT JOIN'
        elif has_field('join_right'):
            join_type='RIGHT JOIN'
        elif has_field('join_full'):
            join_type='FULL JOIN'

        # extract the join condition
        on_condition = _getvalue(result,'on_condition')
        using_condition = _getvalue(result,'using_condition')
        if on_condition:
            condition = 'on '+on_condition
        elif using_condition:
            condition = 'using ('+using_condition+')'
        else:
            condition = ''

        # add the dictionary
        joininfo = {
            'table_name': table_name,
            'table_alias': table_alias,
            'condition': condition,
            'join_type': join_type
            }
        joininfos.append(joininfo)
    #pprint.pprint(joininfos)
    return json.dumps(joininfos)


def _getvalue(tree, location):
    results = list(tree.find_data(location))
    if len(results)==0:
        return None
    assert len(results)==1
    child = results[0].children[0]
    if hasattr(child,'data') and child.data == 'blurbs':
        return _unparse_blurbs(child)
    else:
        return _unparse(child)


def _unparse(tree):
    if hasattr(tree,'children'):
        return ''.join([_unparse(child) for child in tree.children])
    else:
        return str(tree).strip()


def _unparse_blurbs(tree):
    assert(tree.data) == 'blurbs'
    blurbitem = _unparse_blurbitem(tree.children[0])
    if len(tree.children)==1:
        return [blurbitem]
    else:
        blurbs_child = _unparse_blurbs(tree.children[2])
        return [blurbitem] + blurbs_child


def _unparse_blurbitem(tree):
    if len(tree.children)==1:
        value = _unparse(tree)
        return [value, '"'+value+'"']
    else:
        return [_unparse(tree.children[0]), _unparse(tree.children[1])]


################################################################################
# example sql expressions for doctests
################################################################################

sql0 = '''
CREATE INCREMENTAL MATERIALIZED VIEW example AS (
    SELECT
        count(*),
        sum(num) AS sum
    FROM tablename
    WHERE (test>=from)
    GROUP BY a,b,c
    HAVING foo=bar
);
'''

sql1='''
CREATE INCREMENTAL MATERIALIZED VIEW testparsing_rollup1 AS (
    SELECT
        count(*) AS count
    FROM testparsing
    GROUP BY name
);
'''

sql2='''
CREATE INCREMENTAL MATERIALIZED VIEW testparsing_rollup2 AS (
    select count(*) AS count
    from testparsing
    group by name,num
);
'''

sql3='''
CREATE INCREMENTAL MATERIALIZED VIEW testparsing_rollup3 AS (
    select
        sum(num) AS sum,
        count(*) AS count_all,
        count(num),
        max(num),
        min(num)
    from testparsing
    group by name
);
'''

sql4='''
CREATE INCREMENTAL MATERIALIZED VIEW testparsing_rollup4 AS (
    select
        sum(num) AS sum,
        count(*) AS count_all,
        count(num),
        max(num),
        min(num)
    from testparsing
);
'''

sql5='''
CREATE INCREMENTAL MATERIALIZED VIEW testparsing_rollup7 AS (
    select
        sum(num*num + 2),
        max(1),
        (max((1 + (((num))))*2) + count(num))/count(*)
        + (max((1 + (((num))))*2) + count(num))/count(*)
    from testparsing
);
'''

sql6='''
CREATE INCREMENTAL MATERIALIZED VIEW testparsing_rollup7 AS (
    SELECT
        count(*) AS count
    FROM testjoin1
    JOIN testjoin2 USING (id)
    GROUP BY name
);
'''

sql7='''
CREATE INCREMENTAL MATERIALIZED VIEW testparsing_rollup7 AS (
    SELECT
        count(*) AS count
    FROM testjoin1
    INNER JOIN testjoin2 ON testjoin1.id=testjoin2.id
    GROUP BY name
);
'''

sql8='''
CREATE INCREMENTAL MATERIALIZED VIEW testparsing_rollup7 AS (
    SELECT
        count(*) AS count
    FROM testjoin1 AS t1
    INNER JOIN testjoin2 AS t2 ON testjoin1.id=testjoin2.id
    LEFT OUTER JOIN testjoin3 as t3 ON testjoin1.name=testjoin3.name
    GROUP BY name
);
'''

sql9='''
CREATE INCREMENTAL MATERIALIZED VIEW testparsing_rollup7 AS (
    SELECT
        sum(num),
        sum(foo)
    FROM testjoin1
    FULL JOIN testjoin2 USING (id)
    GROUP BY name
);
'''

sql10='''
CREATE INCREMENTAL MATERIALIZED VIEW testparsing_rollup7 AS (
    SELECT
        sum(num)
    FROM testjoin1
    JOIN testjoin2 USING (id)
    GROUP BY name
);
'''

sql11='''
	    CREATE INCREMENTAL MATERIALIZED VIEW testjoin_rollup1 AS (
	     SELECT sum(t1.num) AS sum_num,
    sum(t2.foo) AS sum_foo
   FROM (testjoin1 t1
     JOIN testjoin2 t2 USING (id))
  GROUP BY t1.name
	    );
'''

sql12='''
	    CREATE INCREMENTAL MATERIALIZED VIEW testjoin_rollup3 AS (
	     SELECT count(t1.num) AS count_t1,
    count(t2.num) AS count_t2
   FROM ((((((testjoin1 t1
     JOIN testjoin1 t2 ON ((t1.id = t2.num)))
     JOIN testjoin1 t3 ON ((t2.id = t3.num)))
     JOIN testjoin1 t4 ON ((t3.id = t4.num)))
     JOIN testjoin1 t5 ON ((t4.id = t5.num)))
     JOIN testjoin1 t6 ON ((t5.id = t6.num)))
     JOIN testjoin1 t7 ON ((t6.id = t7.num)))
  GROUP BY t1.name
	    );
            '''
