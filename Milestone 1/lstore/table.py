from lstore.index import Index
from time import time
from lstore.pagerange import PageRange
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.rid_temp = 1
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.pagerange_array = []
        pass

    def insert(self, *columns):
        # add mapping to index
        # create new page range if current full; else call pageRange insert
        # pageRange insert return (basepage# and offset) <- add this to page directory
         
        rid_in = self.generate_unique_rid()
        pagerange_len = len(self.pagerange_array)

        if  pagerange_len > 0 and self.pagerange_array[pagerange_len - 1].hasCapacity():
            page_directory_val = self.pagerange_array[pagerange_len - 1].addNewRecord(rid_in, *columns)
        else:
            page_range_temp = PageRange(self, self.num_columns)
            self.pagerange_array.append(page_range_temp)
            pagerange_len = pagerange_len + 1
            page_directory_val = self.pagerange_array[pagerange_len - 1].addNewRecord(rid_in, *columns)
         
        self.index.key_rid_dict[columns[0]] = rid_in
        self.page_directory[rid_in] = pagerange_len - 1, page_directory_val
        pass

    def update(self, primary_key, *columns):
        # figure out the basepage/offset for this record and call pagerange update w/ this info
        temp_rid = self.index.key_rid_dict.get(primary_key) 
        params = self.page_directory.get(temp_rid)
        basepage = params[1][0]
        offset = params[1][1]
        self.pagerange_array[params[0]].updateRecord(basepage, offset, *columns)
        pass

    def read(self, rid, query_columns):
        # use page directory to find pageRange for this rid and call it's read function

        # break up RID
        pagerange_num, (basepage_num, offset) = self.page_directory[rid]

        page_range = self.pagerange_array[pagerange_num]

        # Get values for specified columns in query_columns
        record_columns = page_range.readRecord(basepage_num, offset, query_columns)

        selected_record = Record(rid, record_columns[self.key], record_columns)
        return [selected_record]

    def delete(self, key):
        # call it's corresponding pageRange deleteRecord function
        # remove RID mapping from page directory
        # call Index delete

        # grabbing info from dictionaries
        temp_rid = self.index.key_rid_dict.get(key)
        params = self.page_directory.get(temp_rid)
        
        # call it's corresponding pageRange deleteRecord function
        self.pagerange_array[params[0]].removeRecord(params[1][0], params[1][1])
        
        # remove RID mapping from page directory
        del self.page_directory[temp_rid]        
        
        # call Index delete
        self.index.delete(key)
        
        pass

    def generate_unique_rid(self):
        self.rid_temp += 1
        return self.rid_temp - 1

    def __merge(self):
        print("merge is happening")
        pass
 
