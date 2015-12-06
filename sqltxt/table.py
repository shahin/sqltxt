from column import Column
import logging
import re


def dedupe_with_order(dupes):
    """Given a list, return it without duplicates and order preserved."""

    seen = set()
    deduped = []
    for c in dupes:
        if c not in seen:
            seen.add(c) 
            deduped.append(c)
    return deduped


class Table(object):
    """Translate abstract data-manipulation operations to commands that perform them.

    A Table is a virtual representation of data. Operations on Tables are accumulated and
    optimized, but a Table cannot execute its own commands. In order retrieve the data represented
    by a Table, a second party must execute the Table's commands.
    """

    VALID_IDENTIFIER_REGEX = '^[a-zA-Z_][a-zA-Z0-9_.]*$'
    LOG = logging.getLogger(__name__)

    def __str__(self):
        return self.name

    def __init__(self, 
        name, delimiter=',', cmd=None, columns=None, offset=None, alias=None):

        self.name = name
        self.delimiter = delimiter
        self.cmds = [] if cmd == None else [cmd]
        self.columns = columns
        self.offset = offset
        self.alias = alias

        self.sorted_by = []
        self.outfile_name = "{0}.out".format(name)

        self.column_idxs = self._compute_column_indices()

    @classmethod
    def from_file_path(cls, file_path, columns=None, delimiter=',', alias=None):
        """Given the path to a file, return an instance of a Table representing that file.
        
        :param file_path: a string containing the path to the file
        :param columns: an exhaustive list of column names or Column objects on this table
        :param delimiter: the column delimiter for this table; defaults to ','
        """

        if columns is None:
            columns = cls._parse_column_names(file_path, delimiter)
        
        if alias is None:
            alias = file_path

        column_qualifiers = [file_path.lower(), alias.lower()]
        for idx, col in enumerate(columns):
            if not isinstance(col, Column):
                columns[idx] = Column(col, qualifiers=column_qualifiers)

        # disallow duplicate column names on the same file
        assert len(set([c.name for c in columns])) == len(columns)

        return cls(file_path, delimiter, None, columns, 1, alias)

    @classmethod
    def from_cmd(cls, name, cmd, columns, delimiter=','):
        """Given a command, instantiate a Table representing the output of that command.
        
        :param name: the name of the table
        :param cmd: a string of commands whose execution materializes this table
        :param columns: an exhaustive list of column names or Column objects on this table
        :param delimiter: the column delimiter for this table; defaults to ','
        """

        column_qualifiers = [name.lower()]
        for idx, col in enumerate(columns):
            if not isinstance(col, Column):
                columns[idx] = Column(col, qualifiers=column_qualifiers)

        return cls(name, delimiter, cmd, columns)

    @staticmethod
    def _parse_column_names(file_path, delimiter):
        """Return a list of column headers found in the first line of a file."""

        with open(file_path) as table_file:
            head = table_file.readline().rstrip()

        return head.split(delimiter)

    def order_columns(self, columns_in_order, drop_other_columns=False):
        """Rearrange and subset the columns of this Table."""

        if (columns_in_order == self.columns) or (
            columns_in_order == self.columns[0:len(columns_in_order)] and not drop_other_columns):
            self.LOG.debug('Columns already in order {0}'.format(self.columns))
            return

        self.LOG.debug('Current column order of {0} is {1}'.format(self.name, self.columns))
        self.LOG.debug('Reordering {0} columns to {1}'.format(self.name, columns_in_order))
        
        reordered_col_idxs = [self.column_idxs[col][0] for col in columns_in_order]
        unchanged_col_idxs = [
            self.column_idxs[col][0] for col in self.columns
            if col not in columns_in_order]

        col_idxs = reordered_col_idxs
        if not drop_other_columns:
            col_idxs += unchanged_col_idxs

        reorder_cmd = "awk -F'{0}' 'OFS=\"{0}\" {{ print {1} }}'".format(
            self.delimiter, ','.join('$' + str(idx + 1) for idx in col_idxs))

        self.columns = [self.columns[idx] for idx in col_idxs]
        self.column_idxs = self._compute_column_indices()
        self.cmds.append(reorder_cmd)

    def is_sorted_by(self, sort_order_indices):
        """Return true if this Table's rows are sorted by columns at the given indices."""

        if len(self.sorted_by) < len(sort_order_indices):
            return False

        for sort_idx, column_idx in enumerate(sort_order_indices):
            if self.columns[column_idx] != self.sorted_by[sort_idx]:
                return False

        return True

    def sort(self, columns_to_sort_by):
        """Sort the rows of this Table by the given columns."""

        columns_to_sort_by = dedupe_with_order(columns_to_sort_by)

        # if this table is already sorted by the requested sort order, do nothing
        if columns_to_sort_by == self.sorted_by[0:len(columns_to_sort_by)]:
            return None
        self.LOG.debug('Sorting {0} by {1}'.format(self.name, columns_to_sort_by))

        column_idxs_to_sort_by = [self.column_idxs[col][0] for col in columns_to_sort_by]

        sort_key_params = ' -k '.join(
              ','.join([str(idx + 1),str(idx + 1)]) for idx in column_idxs_to_sort_by)

        sort_cmd = 'sort -t{0} -k {1}'.format(self.delimiter, sort_key_params)
        self.sorted_by = columns_to_sort_by
        self.cmds.append(sort_cmd)
    
    def subset_rows(self, conditions):
        """Subset the rows of this Table to rows that satisfy the given conditions."""

        # translate a list of boolean conditions to awk syntax
        condition_str = ''
        for expr_part in conditions:
            if not isinstance(expr_part, basestring):
                # treat any PostgreSQL-valid identifier as a column
                expr_part = [
                    ('$' + str( self._column_idx(Column(token)) ) 
                        if re.match(self.VALID_IDENTIFIER_REGEX, token) 
                        else token
                    )
                    for token in expr_part]
            condition_str += ''.join(expr_part)

        if condition_str == '':
            self.LOG.debug('Empty condition string so not subsetting columns on {0}'.format(
                self.name))
            return

        columns = ','.join(['$' + str(self.column_idxs[c][0] + 1) for c in self.columns])
        awk_cmd = "awk -F'{0}' 'OFS=\"{0}\" {{ if ({1}) {{ print {2} }} }}'".format(
            self.delimiter, condition_str, columns)
        self.cmds.append(awk_cmd)

    def get_cmd_str(self, output_column_names=False):
        """Return a string of commands whose output is the contents of this Table.""" 

        cmds = self.cmds

        if self.offset:
            cmds = ['tail +{0} {1}'.format(self.offset+1, self.name)] + cmds 

        cmd_str = ' | '.join(cmds)

        # write column names
        if output_column_names:
            cmd_str = 'echo "{0}"; '.format(
                ','.join([str(col) for col in self.columns])
                ) + cmd_str

        return cmd_str

    def _compute_column_indices(self):
        """Return a dictionary of column index lists keyed by Column."""

        idxs = {}
        for i, c in enumerate(self.columns):
            try:
                idxs[c].append(i)
            except KeyError:
                idxs[c] = [i]
        self.LOG.debug('{0} computed column indices {1}'.format(self,idxs))
        return idxs
  
    def _column_idx(self, column):
        return self.column_idxs[column][0] + 1
