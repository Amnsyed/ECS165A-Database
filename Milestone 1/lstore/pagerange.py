
from lstore.page import Page

class PageRange:
    def __init__(self, table, num_columns):
        self.table = table
        self.base_page_array = []
        self.tail_page_array = []
        self.num_columns = num_columns
        self.capacity = 8192
        self.num_records = 0
        self.last_base_offset = 0
        self.last_tail_offset = 0
        self.last_base_page = 0
        self.last_tail_page = 0
        self.tail_page_directory = {}   # rid -> (tail page number, offset)
        
        # create empty base page/tail page and add it to their corresponding arrays
        base_page = []
        tail_page = []
        for i in range(self.num_columns + 2):
            base_page.append(Page())
            tail_page.append(Page())
        base_page.append(Page())    # schema encoding column
        self.base_page_array.append(base_page)
        self.tail_page_array.append(tail_page)

    def addNewRecord(self, rid, *columns):
        # return a tuple (base page #, offset #)
        self.num_records += 1
        if not self.base_page_array[-1][0].has_capacity():
            # last base page full; create new base page
            base_page = []
            for i in range(self.num_columns + 3):
                base_page.append(Page())
            self.base_page_array.append(base_page)
            self.last_base_offset = 0
            self.last_base_page += 1

        page = self.base_page_array[self.last_base_page]
        # add the record values to pages
        for i in range(self.num_columns):
            page[i].write(columns[i], self.last_base_offset)
        # add rid, indirection to page and schema encoding
        page[self.num_columns].write(rid, self.last_base_offset)
        page[self.num_columns + 1].write(0, self.last_base_offset)
        page[self.num_columns + 2].write(0, self.last_base_offset)

        self.last_base_offset += 8
        return (self.last_base_page, self.last_base_offset - 8)


    def updateRecord(self, base_page_num, offset, *columns):
        new_rid = self.table.generate_unique_rid()
        # add new tail page if last one out of capacity
        if not self.tail_page_array[-1][0].has_capacity():
            tail_page = []
            for i in range(self.num_columns + 2):
                tail_page.append(Page())
            self.tail_page_array.append(tail_page)
            self.last_tail_offset = 0
            self.last_tail_page += 1
        
        page = self.tail_page_array[self.last_tail_page]
        self.tail_page_directory[new_rid] = (self.last_tail_page, self.last_tail_offset)
        schema_encoding = format(self.base_page_array[base_page_num][self.num_columns + 2].read(offset), '064b')
        if self.base_page_array[base_page_num][self.num_columns + 1].read(offset) == 0:
            # base record has not update, adding first tail record
            self.base_page_array[base_page_num][self.num_columns + 1].write(new_rid, offset)
            for i in range(self.num_columns):
                if columns[i] == None:
                    page[i].write(0 , self.last_tail_offset)
                else:
                    page[i].write(columns[i], self.last_tail_offset)
                    schema_encoding = schema_encoding[:i + 1] + '1' + schema_encoding[i+2:]
            page[self.num_columns].write(new_rid, self.last_tail_offset)
            page[self.num_columns + 1].write(self.base_page_array[base_page_num][self.num_columns].read(offset), self.last_tail_offset)
        else:
            # adding subsequent tail record
            prev_tail_rid = self.base_page_array[base_page_num][self.num_columns + 1].read(offset)
            prev_tail_record_location = self.tail_page_directory[prev_tail_rid]
            prev_tail_page = self.tail_page_array[prev_tail_record_location[0]]
            for i in range(self.num_columns):
                if columns[i] is not None:
                    page[i].write(columns[i], self.last_tail_offset)
                    schema_encoding = schema_encoding[:i + 1] + '1' + schema_encoding[i+2:]
                elif schema_encoding[i + 1] == '1':
                    page[i].write(prev_tail_page[i].read(prev_tail_record_location[1]), self.last_tail_offset)
                else:
                    page[i].write(0, self.last_tail_offset)
            page[self.num_columns].write(new_rid, self.last_tail_offset)
            page[self.num_columns + 1].write(prev_tail_rid, self.last_tail_offset)
            self.base_page_array[base_page_num][self.num_columns + 1].write(new_rid, offset)

        self.base_page_array[base_page_num][self.num_columns + 2].write(int(schema_encoding, 2), offset)
        self.last_tail_offset += 8
        return True

    def hasCapacity(self):
        return self.num_records < self.capacity

    def readRecord(self, base_page_num, offset, query_columns):
        record = []
        page = self.base_page_array[base_page_num]
        schema_encoding = format(page[self.num_columns + 2].read(offset), '064b')

        # if value in redirection, use latest tail entry to get query columns
        redirect_val = page[self.num_columns + 1].read(offset)
        if redirect_val != 0:
            location = self.tail_page_directory[redirect_val]
            tail_page = self.tail_page_array[location[0]]
            for col in query_columns:
                # based on bit array value, read from either base record or tail record
                if schema_encoding[col + 1] == '1':
                    record.append(tail_page[col].read(location[1]))
                else:
                    record.append(page[col].read(offset))
        else:
            for col in query_columns:
                record.append(page[col].read(offset))
        return record

    def removeRecord(self, base_page_num, offset):
        page = self.base_page_array[base_page_num]
        page[self.num_columns].write(0, offset)
        return True
