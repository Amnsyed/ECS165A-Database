"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        self.table = table
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        self.key_rid_dict = {} 
        # creating a dict object for all columns, but for milestone1 we only need to fill the key column
        for i in range(len(self.indices)):
            self.indices[i] = dict()
        
    def delete(self, key):
        del self.key_rid_dict[key]

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        # return None if value not in index else return all RID for the given column/value
        if value not in self.key_rid_dict:
            return None
        return self.key_rid_dict[value]

    def insert(self, column, value, rid):
        # add the value -> rid mapping to the given column hashmap
        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        # No need to implement this
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
