"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        self.table = table
        # One index for each table. All are empty initially.
        self.indices = [None] *  table.num_columns
        self.indices[table.key] = {}
        self.index_columns = []
        
    def delete(self, key):
        del self.indices[self.table.key][key]

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        # return None if value not in index else return all RID for the given column/value
        if self.indices[column] == None or value not in self.indices[column]:
            return None
        return self.indices[column][value]
  
    def insert(self, column, value, rid):
        # add the value -> rid mapping to the given column hashmap
        self.indices[column][value] = rid

    def insert_nonprimary_columns(self, rid, *columns):
        for i in self.index_columns:
            self.indices[i][columns[i]] = rid

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        if self.indices[column] == None:
            return None
        result_range = {}
        for i in range(begin, end + 1):
            if i in self.indices[column]:
                result_range.append(self.indices[column][i])
        return result_range

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        self.indices[column_number] = {}
        self.index_columns.append(column_number)

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None
        self.index_columns.remove(column_number)
