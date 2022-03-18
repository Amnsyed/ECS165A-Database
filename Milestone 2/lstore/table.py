from lstore.index import Index
from time import time
from lstore.pagerange import PageRange
import copy
import threading
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
        self.bufferpool = None
        self.table_path = name + '/'
        self.locks = []

    def insert(self, *columns):
        # add mapping to index
        # create new page range if current full; else call pageRange insert
        # pageRange insert return (basepage# and offset) <- add this to page directory
         
        rid_in = self.generate_unique_rid()
        pagerange_len = len(self.pagerange_array)

        if  pagerange_len > 0 and self.pagerange_array[pagerange_len - 1].hasCapacity():
            self.locks[pagerange_len - 1].acquire()
            page_directory_val = self.pagerange_array[pagerange_len - 1].addNewRecord(rid_in, *columns)
            self.locks[pagerange_len - 1].release()
        else:
            self.locks.append(threading.Lock())
            self.locks[-1].acquire()
            page_range_temp = PageRange(self, self.num_columns, len(self.pagerange_array))
            self.pagerange_array.append(page_range_temp)
            pagerange_len = pagerange_len + 1
            page_directory_val = self.pagerange_array[pagerange_len - 1].addNewRecord(rid_in, *columns)
            self.locks[-1].release()
        
        self.index.insert(self.key, columns[0], rid_in)
        self.index.insert_nonprimary_columns(rid_in, *columns)

        self.page_directory[rid_in] = pagerange_len - 1, page_directory_val

    def update(self, primary_key, *columns):
        # figure out the basepage/offset for this record and call pagerange update w/ this info
        temp_rid = self.index.locate(self.key, primary_key)
        params = self.page_directory.get(temp_rid)
        self.locks[params[0]].acquire()
        basepage = params[1][0]
        offset = params[1][1]
        self.pagerange_array[params[0]].updateRecord(temp_rid, basepage, offset, *columns)
        self.locks[params[0]].release()

    def read(self, rid, query_columns):
        # use page directory to find pageRange for this rid and call it's read function
        # break up RID
        pagerange_num, (basepage_num, offset) = self.page_directory[rid]
        self.locks[pagerange_num].acquire()
        page_range = self.pagerange_array[pagerange_num]

        # Get values for specified columns in query_columns
        record_columns = page_range.readRecord(basepage_num, offset, query_columns)

        selected_record = Record(rid, record_columns[self.key], record_columns)
        self.locks[pagerange_num].release()
        return [selected_record]

    def delete(self, key):
        # call it's corresponding pageRange deleteRecord function
        # remove RID mapping from page directory
        # call Index delete

        # grabbing info from dictionaries
        temp_rid = self.index.locate(self.key, key)
        params = self.page_directory.get(temp_rid)
        self.locks[params[0]].acquire()
        
        # call it's corresponding pageRange deleteRecord function
        self.pagerange_array[params[0]].removeRecord(params[1][0], params[1][1])
        
        # remove RID mapping from page directory
        del self.page_directory[temp_rid]        
        
        # call Index delete
        self.index.delete(key)
        
        self.locks[params[0]].release()

    def generate_unique_rid(self):
        self.rid_temp += 1
        return self.rid_temp - 1

    def merge(self):
        for i in range(len(self.pagerange_array)):
            self.locks[i].acquire()
            b_array, tail_array, offset = self.pagerange_array[i].get_basepage_copy()
            if len(tail_array) == 0 or len(b_array) == 0:
                self.locks[i].release()
                continue
            base_array = []
            for ii in range(len(b_array)):
                arr = []
                for jj in range(len(b_array[ii])):
                    self.bufferpool.get(b_array[ii][jj], self.table_path, (i, ii, jj, self.num_columns + 3), 'bp')
                    arr.append(copy.deepcopy(b_array[ii][jj]))
                    self.bufferpool.decrementPin(b_array[ii][jj])
                base_array.append(arr)
            self.locks[i].release()
            latest_update = set()
            latest_tail_rid = 0
            found = True
            for tail_page_num in reversed(range(len(tail_array))):  # Iterate through each tail page in tail page array
                tail_page = tail_array[tail_page_num]
                base_rid_col = len(tail_page) - 1   # Last column in tail record should be the base RID column

                # for each record in the tail (determined by offset) page copy all values to the corresponding base page
                for base_rid_col_offset in range(4088, -1, -8):
                    self.bufferpool.get(tail_page[base_rid_col], self.table_path, (i, tail_page_num + offset, base_rid_col, self.num_columns + 3), 'tp')
                    c_base_page_rid = tail_page[base_rid_col].read(base_rid_col_offset)
                    if found:
                        self.bufferpool.get(tail_page[base_rid_col - 2], self.table_path, (i, tail_page_num + offset, base_rid_col - 2, self.num_columns + 3), 'tp')
                        latest_tail_rid = tail_page[base_rid_col - 2].read(base_rid_col_offset)
                        found = False
                        self.bufferpool.decrementPin(tail_page[base_rid_col - 2])
                    self.bufferpool.decrementPin(tail_page[base_rid_col])

                    # Check to see if latest update has already been copied over for that base record
                    if c_base_page_rid in latest_update:
                        continue

                    latest_update.add(c_base_page_rid)

                    params = self.page_directory.get(c_base_page_rid)
                    if params == None:
                        continue
                    basepage_num = params[1][0]
                    if basepage_num >= len(base_array):
                        continue
                    base_rid_offset = params[1][1]

                    # Iterate through all use defined columns in tail page
                    for col in range(len(tail_page) - 3):
                        self.bufferpool.get(tail_page[col], self.table_path, (i, tail_page_num + offset, col, self.num_columns + 3), 'tp')
                        tail_col_val = tail_page[col].read(base_rid_col_offset)
                        self.bufferpool.decrementPin(tail_page[col])
                        if tail_col_val != 0:
                            base_array[basepage_num][col].write(tail_col_val, base_rid_offset)

            # lock page range
            # update tps
            self.locks[i].acquire()
            self.pagerange_array[i].tps = latest_tail_rid
            # update all base pages
            for ii in range(len(base_array)):
                for jj in range(len(base_array[ii])):
                    self.bufferpool.get(b_array[ii][jj], self.table_path, (i, ii, jj, self.num_columns + 3), 'bp')
                    b_array[ii][jj].data = base_array[ii][jj].data
                    self.bufferpool.decrement_and_markDirty(b_array[ii][jj])
            self.locks[i].release()
            # release lock
 
