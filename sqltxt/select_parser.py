# select_parser.py
# Copyright 2010, Paul McGuire
#
# a simple SELECT statement parser, taken from SQLite's SELECT statement
# definition at http://www.sqlite.org/lang_select.html
#
from pyparsing import *
ParserElement.enablePackrat()

LPAR,RPAR,COMMA = map(Suppress,"(),")
select_stmt = Forward().setName("select statement")

# keywords
(UNION, ALL, AND, INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER, 
 CROSS, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, DISTINCT, FROM, WHERE, GROUP, BY,
 HAVING, ORDER, BY, LIMIT, OFFSET) =  map(CaselessKeyword, """UNION, ALL, AND, INTERSECT, 
 EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER, CROSS, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, 
 DISTINCT, FROM, WHERE, GROUP, BY, HAVING, ORDER, BY, LIMIT, OFFSET""".replace(",","").split())
(CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, END, CASE, WHEN, THEN, EXISTS,
 COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, CURRENT_TIME, CURRENT_DATE, 
 CURRENT_TIMESTAMP) = map(CaselessKeyword, """CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, 
 END, CASE, WHEN, THEN, EXISTS, COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, 
 CURRENT_TIME, CURRENT_DATE, CURRENT_TIMESTAMP""".replace(",","").split())
keyword = MatchFirst((UNION, ALL, INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER, 
 CROSS, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, DISTINCT, FROM, WHERE, GROUP, BY,
 HAVING, ORDER, BY, LIMIT, OFFSET, CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, END, CASE, WHEN, THEN, EXISTS,
 COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, CURRENT_TIME, CURRENT_DATE, 
 CURRENT_TIMESTAMP))
 
identifier = ~keyword + Word(alphas, alphanums+"_")
column_name = identifier.copy()
column_alias = identifier.copy()
table_name = identifier.copy()
table_alias = identifier.copy()
index_name = identifier.copy()
function_name = identifier.copy()
parameter_name = identifier.copy()
database_name = identifier.copy()

# expression
expr = Forward().setName("expression")

integer = Regex(r"[+-]?\d+")
numeric_literal = Regex(r"\d+(\.\d*)?([eE][+-]?\d+)?")
string_literal = QuotedString("'")
blob_literal = Combine(oneOf("x X") + "'" + Word(hexnums) + "'")
literal_value = ( numeric_literal | string_literal | blob_literal |
    NULL | CURRENT_TIME | CURRENT_DATE | CURRENT_TIMESTAMP )
bind_parameter = (
    Word("?",nums) |
    Combine(oneOf(": @ $") + parameter_name)
    )
type_name = oneOf("TEXT REAL INTEGER BLOB NULL")

expr_term = (
    CAST + LPAR + expr + AS + type_name + RPAR |
    function_name + LPAR + Optional(delimitedList(expr)) + RPAR |
    literal_value |
    bind_parameter |
    identifier
    )

UNARY,BINARY,TERNARY=1,2,3
expr << operatorPrecedence(expr_term,
    [
    ('||', BINARY, opAssoc.LEFT),
    (oneOf('* / %'), BINARY, opAssoc.LEFT),
    (oneOf('+ -'), BINARY, opAssoc.LEFT),
    (oneOf('< <= > >='), BINARY, opAssoc.LEFT),
    (oneOf('= == != <>') | IS | IN | LIKE | GLOB | MATCH | REGEXP, BINARY, opAssoc.LEFT),
    ])
compound_operator = (UNION + Optional(ALL) | INTERSECT | EXCEPT)

ordering_term = expr + Optional(ASC | DESC)

join_constraint = Optional(ON + expr | USING + LPAR + Group(delimitedList(column_name)) + RPAR)

join_op = COMMA | (Optional(NATURAL) + Optional(INNER | CROSS | LEFT + OUTER | LEFT | OUTER) + JOIN)

single_source = Group(database_name("database") + "." + table_name("table") | table_name("table") + 
                    Optional(Optional(AS) + table_alias("table_alias")))

join_source = Group(single_source + ZeroOrMore(join_op.setResultsName('join_op') + single_source + join_constraint)).setResultsName('joins')

result_column = "*" | table_name + "." + "*" | (expr + Optional(Optional(AS) + column_alias))
select_stmt = (SELECT + Optional(DISTINCT | ALL) + Group(delimitedList(result_column))("columns") +
                Optional(FROM + join_source) +
                Optional(WHERE + expr("where_expr")) +
                Optional(GROUP + BY + Group(delimitedList(ordering_term)("group_by_terms")) + 
                        Optional(HAVING + expr("having_expr"))))

