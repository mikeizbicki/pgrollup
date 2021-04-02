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
    >>> len(parse_create(sql0+sql1+sql2+sql3+sql4))
    5
    '''
    tree = grammar.parse(text)
    infos = []
    for child in tree.find_data('create_view'):
        infos.append({
            'table_name' : _getvalue(child,'table_name'),
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

    select: "SELECT"i columns "FROM"i table_name where_clause? group_clause? having_clause?

    columns: blurbs
    where_clause: "WHERE"i blurb
    group_clause: "GROUP"i "BY"i blurbs
    having_clause: "HAVING"i blurb
    
    blurbs: blurbitem COMMA blurbs | blurbitem 
    blurbitem: blurb as_clause?
    as_clause: "AS"i blurb
    blurb: blurb_rec
    blurb_rec: blurb_word blurb_rec | blurb_word
    blurb_word: /((?!(GROUP)|(HAVING)|(FROM)|(AS)|[(),])(.|\s))+/i
              | LPAREN blurb_inparen_rec RPAREN
    blurb_inparen_rec: blurb_inparen blurb_inparen_rec | blurb_inparen
    blurb_inparen: /[^()]+/
                 | LPAREN blurb_inparen_rec RPAREN

    rollup_name: NAME
    table_name: NAME

    NAME: /[a-zA-Z_0-9]+/ 
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
