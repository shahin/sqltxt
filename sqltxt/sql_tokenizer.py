"""This module contains the SQL grammar, functions for parsing a string based on the grammar, and
functions for humanizing the parsed result."""

import subprocess
from pyparsing import (
  Forward, 
  Keyword,
  Word,
  Group,
  CaselessKeyword,
  MatchFirst,

  delimitedList,
  oneOf,
  ZeroOrMore,
  OneOrMore,
  Combine,
  Optional,
  StringEnd,
  Suppress,

  CaselessLiteral,
  quotedString,
  alphas,
  alphanums,
  nums,
  printables
  )

# keywords
(UNION, ALL, AND, INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER, 
 CROSS, RIGHT, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, DISTINCT, FROM, WHERE, GROUP, BY,
 HAVING, ORDER, BY, LIMIT, OFFSET, TABLESAMPLE) =  map(CaselessKeyword, """UNION, ALL, AND, INTERSECT, 
 EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER, CROSS, RIGHT, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, 
 DISTINCT, FROM, WHERE, GROUP, BY, HAVING, ORDER, BY, LIMIT, OFFSET, TABLESAMPLE""".replace(",","").split())
(CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, END, CASE, WHEN, THEN, EXISTS,
 COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, CURRENT_TIME, CURRENT_DATE, 
 CURRENT_TIMESTAMP) = map(CaselessKeyword, """CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, 
 END, CASE, WHEN, THEN, EXISTS, COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, 
 CURRENT_TIME, CURRENT_DATE, CURRENT_TIMESTAMP""".replace(",","").split())
keyword = MatchFirst((UNION, ALL, INTERSECT, EXCEPT, COLLATE, ASC, DESC, ON, USING, NATURAL, INNER, 
 CROSS, RIGHT, LEFT, OUTER, JOIN, AS, INDEXED, NOT, SELECT, DISTINCT, FROM, WHERE, GROUP, BY,
 HAVING, ORDER, BY, LIMIT, OFFSET, CAST, ISNULL, NOTNULL, NULL, IS, BETWEEN, ELSE, END, CASE, WHEN, THEN, EXISTS,
 COLLATE, IN, LIKE, GLOB, REGEXP, MATCH, ESCAPE, CURRENT_TIME, CURRENT_DATE, 
 CURRENT_TIMESTAMP, TABLESAMPLE))

select_tok = Keyword('select', caseless=True)
from_tok = Keyword('from', caseless=True) 

# for parsing select-from statements
idr = ~keyword + Word(alphas + '*', alphanums + '_/-.*').setName('identifier')

table_path = Word(''.join([c for c in printables if c not in "?"])).setResultsName('path')
table_alias = idr.setResultsName('alias')
table_idr = table_path + Optional(Optional(Suppress('as')) + table_alias)

column_idr = delimitedList(idr, '.', combine=True)
aggregate_function = Combine(Keyword('count') + '(' + column_idr + ')')
column_list = Group(delimitedList((column_idr ^ aggregate_function.setResultsName('aggregate_functions', listAllMatches=True))))

# for parsing where statements
and_ = Keyword('and', caseless=True)
or_ = Keyword('or', caseless=True)
in_ = Keyword('in', caseless=True)

E = CaselessLiteral('E')
binary_op = oneOf('= != < > >= <= eq ne lt le gt ge', caseless=True)
arith_sign = Word('+-', exact=1)

real_num = Combine( 
    Optional(arith_sign) + 
    ( Word(nums) + '.' + Optional( Word(nums) ) | ( '.' + Word(nums) ) ) + 
    Optional( E + Optional(arith_sign) + Word(nums) ) 
    )

int_num = Combine(
    Optional(arith_sign) + Word(nums) + Optional( E + Optional('+') + Word(nums) )
    )

column_val = real_num | int_num | quotedString | column_idr

select_stmt = Forward()
where_expr = Forward()
where_cond = Group(
  ( column_idr.setResultsName('left_operand') + 
    binary_op.setResultsName('operator') + 
    column_val.setResultsName('right_operand') 
    ) |
  ( column_idr.setResultsName('left_operand') + 
    in_ + 
    Suppress("(") + delimitedList( column_val ).setResultsName('right_operand') + Suppress(")") 
    ).setResultsName('in_list_condition') |
  ( column_idr.setResultsName('left_operand') + 
    in_ + 
    Suppress("(") + select_stmt.setResultsName('right_operand') + Suppress(")")
    ).setResultsName('in_query_condition') |
  ( Suppress("(") + where_expr + Suppress(")") )
  )

group_by_expr = Group(column_idr + ZeroOrMore( "," + column_idr ))

where_expr << where_cond + ZeroOrMore( (and_ | or_) + where_expr )

on_ = Keyword('on', caseless=True)
join = ((oneOf('left right') + 'join' ) | (Optional('inner') + 'join')
        ).setResultsName('join_type')

from_clause = table_idr.setResultsName('relation') + ZeroOrMore(Group(
    join + table_idr.setResultsName('relation') + on_ + where_cond.setResultsName('join_conditions') 
  )).setResultsName('joins', listAllMatches=True)

tablesample_clause = CaselessLiteral("tablesample") + \
        Suppress("(") + \
        Word(nums).setResultsName('sample_size').setParseAction(lambda s, loc, tok: int(tok[0])) + \
        Suppress(")")

select_stmt << (
    select_tok + 
    ( column_list ).setResultsName('column_definitions') +
    from_tok +
    from_clause.setResultsName('from_clause') +
    Optional( CaselessLiteral("where") + where_expr.setResultsName("where_clause") ) +
    Optional( tablesample_clause ).setResultsName("tablesample_clause") +
    StringEnd()
    )

def _normalize_relation(relation_clause):
    return {
        'path': relation_clause['path'],
        'alias': relation_clause.get('alias', [False])[0] or relation_clause['path']
    }

def _normalize_condition(condition_clause):
    normalized_clause = []

    for idx in range(0, len(condition_clause.asList())):

        condition_part = condition_clause[idx]

        if isinstance(condition_part, basestring):
            # this is just a str, so it'd better be 'and' or 'or'
            r = condition_part
        elif len(condition_part.asDict()) > 0:
            # this is a working dictionary, so it's an atomic condition
            r = condition_part.asDict()
        elif len(condition_part.asDict()) == 0:
            # this is dictionaryable but not itself an atomic cond, so normalize it
            r = _normalize_condition(condition_part)

        normalized_clause.append(r)
    return normalized_clause

def _normalize_from_clause(from_clause):
    normalized = [{'relation': _normalize_relation(from_clause['relation'])}]

    if 'joins' in from_clause:
        for join_clause in from_clause['joins'][0]:

            normalized_join = {}
            normalized_join['relation'] = _normalize_relation(join_clause['relation'])

            normalized_join['join_conditions'] = _normalize_condition(join_clause['join_conditions'])
            normalized.append(normalized_join) 
    
    return normalized

def _normalize_where_clause(where_clause):
    return _normalize_condition(where_clause) if where_clause else []

def parse(sql_string):
    """Given a string containing SQL, parse it and return the normalized result."""
    parsed = select_stmt.parseString(sql_string)
    parsed.from_clause = _normalize_from_clause(parsed.from_clause)
    parsed.where_clause = _normalize_where_clause(parsed.where_clause)
    return parsed

def get_relations_and_conditions(parsed_sql):
    """Given normalized parsed SQL, return a list of relations and list of conditions including all
    join- and where-conditions.
    """

    relations = [subclause['relation'] for subclause in parsed_sql.from_clause]
    conditions = [
        subclause['join_conditions'] for subclause in parsed_sql.from_clause
        if 'join_conditions' in subclause
    ]
    conditions.extend(parsed_sql.where_clause)
    conjunctions = ['and'] * len(conditions)
    conditions = [ part for parts in zip(conditions, conjunctions) for part in parts ][:-1]

    return relations, conditions

