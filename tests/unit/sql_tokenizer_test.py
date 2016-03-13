import unittest
from sqltxt.sql_tokenizer import select_stmt, parse

class SqlTokenizerTest(unittest.TestCase):

    def test_parse_select_list(self):
        parsed = select_stmt.parseString('select col1 from table1')
        self.assertEqual(list(parsed.column_definitions), ['col1'])

        parsed = select_stmt.parseString('select col1, col2 from table1')
        self.assertEqual(list(parsed.column_definitions), ['col1', 'col2'])

        parsed = select_stmt.parseString('select table1.col1 from table1')
        self.assertEqual(list(parsed.column_definitions), ['table1.col1'])

    def test_parse_select_list_with_wildcard(self):
        parsed = select_stmt.parseString('select * from table1')
        self.assertEqual(list(parsed.column_definitions), ['*'])

        parsed = select_stmt.parseString('select *, col1 from table1')
        self.assertEqual(list(parsed.column_definitions), ['*', 'col1'])

    def test_parse_from_list(self):
        parsed = select_stmt.parseString('select col1 from table1')
        relation_path = parsed.from_clause.relation.path
        self.assertEqual(relation_path, 'table1')

    def test_parse_from_list_with_table_alias(self):
        parsed = select_stmt.parseString('select col1 from table1 t1')
        relation_path = parsed.from_clause.relation.path
        relation_alias = parsed.from_clause.relation.alias[0]
        self.assertEqual(relation_path, 'table1')
        self.assertEqual(relation_alias, 't1')

    def test_parse_from_list_with_joins_to_get_join_type(self):
        parsed = select_stmt.parseString('''
            select col1
            from table1 join table2 on (table1.col1 = table2.col1)
        ''')
        self.assertEqual(parsed.from_clause.joins[0][0].join_type, 'join')

        parsed = select_stmt.parseString('''
            select col1
            from table1 inner join table2 on (table1.col1 = table2.col1)
        ''')
        self.assertEqual(parsed.from_clause.joins[0][0].join_type, 'inner')

        parsed = select_stmt.parseString('''
            select col1
            from table1 left join table2 t2 on (table1.col1 = t2.col1)
        ''')
        self.assertEqual(parsed.from_clause.joins[0][0].join_type, 'left')

        parsed = select_stmt.parseString('''
            select col1
            from table1 right join table2 on (table1.col1 = table2.col1)
        ''')
        self.assertEqual(parsed.from_clause.joins[0][0].join_type, 'right')

        parsed = select_stmt.parseString('''
            select col1
            from
                table1
                join table2 on (table1.col1 = table2.col1)
                left join table3 on (table2.col1 = table3.col1)
        ''')
        self.assertEqual(parsed.from_clause.joins[0][0].join_type, 'join')
        self.assertEqual(parsed.from_clause.joins[0][1].join_type, 'left')

    def test_parse_from_list_with_joins_to_get_join_conditions(self):

        parsed = parse('''
            select cola
            from table1 join table2 on (table1.cola = table2.cola)
        ''')
        self.assertEqual(parsed.from_clause[1]['join_conditions'][0], {
            'left_operand': 'table1.cola',
            'operator': '=',
            'right_operand': 'table2.cola',
        })

        parsed = parse('''
            select cola
            from
                table1
                join table2 on (table1.cola = table2.cola)
                join table3 t3 on (table1.colb = t3.colb)
        ''')
        self.assertEqual(parsed.from_clause[2]['join_conditions'][0], {
            'left_operand': 'table1.colb',
            'operator': '=',
            'right_operand': 't3.colb',
        })

        parsed = parse('''
            select cola
            from
                table1
                join table2 on (table1.cola = table2.cola)
                join table3 t3 on (table1.colb = t3.colb and table2.colc = t3.colc)
        ''')
        self.assertEqual(parsed.from_clause[2]['join_conditions'][1], 'and')
        self.assertEqual(parsed.from_clause[2]['join_conditions'][2], {
            'left_operand': 'table2.colc',
            'operator': '=',
            'right_operand': 't3.colc',
        })

    def test_parse_where_list(self):
        parsed = parse('''
            select cola
            from
                table1
                join table2 on (table1.cola = table2.cola)
            where colb = 1 and (colc = 0 or colz = 'a')
        ''')
        self.assertEqual(parsed.where_clause, [
            {'left_operand': 'colb', 'operator': '=', 'right_operand': '1'},
            'and',
            [
                {'left_operand': 'colc', 'operator': '=', 'right_operand': '0'},
                'or',
                {'left_operand': 'colz', 'operator': '=', 'right_operand': "'a'"}
            ],
        ])

    def test_parse_tablesample_clause(self):
        parsed = parse('''
            select cola
            from table1
            tablesample (5)
        ''')
        self.assertEqual(parsed.tablesample_clause.asDict(), {'sample_size': 5})

        parsed = parse('''
            select cola
            from
                table1
                join table2 on (table1.cola = table2.cola)
            where colb = 1 and (colc = 0 or colz = 'a')
            tablesample (50)
        ''')
        self.assertEqual(parsed.tablesample_clause.asDict(), {'sample_size': 50})

