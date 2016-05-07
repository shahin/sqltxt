"""This module contains the SQL grammar, functions for parsing a string based on the grammar, and
functions for humanizing the parsed result."""

import subprocess
from pyparsing import (
  Forward, 
  Keyword,
  Word,
  Group,

  delimitedList,
  oneOf,
  ZeroOrMore,
  Optional,
  StringEnd,
  Suppress,

  CaselessLiteral,
  nums,
  )

from tokens import *

column_list = Group(delimitedList((column_idr ^ aggregate_function.setResultsName('aggregate_functions', listAllMatches=True))))

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

where_expr << where_cond + ZeroOrMore( (and_ | or_) + where_expr )

join = ((oneOf('left right') + 'join' ) | (Optional('inner') + 'join')
        ).setResultsName('join_type')

on_ = Keyword('on', caseless=True)
from_clause = table_idr.setResultsName('relation') + ZeroOrMore(Group(
    join + table_idr.setResultsName('relation') + on_ + where_cond.setResultsName('join_conditions') 
  )).setResultsName('joins', listAllMatches=True)

group_by_clause = delimitedList(column_idr)

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
    Optional( CaselessLiteral("group by") + group_by_clause.setResultsName("group_by_clause") ) +
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

