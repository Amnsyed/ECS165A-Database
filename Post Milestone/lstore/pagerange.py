
from lstore.page import Page
import copy

class PageRange:
    def __init__(self, table, num_columns, page_range_num):
        self.table = table
        self.page_range_num = page_range_num
        self.base_page_array = []
        self.tail_page_array = []
        self.num_columns = num_columns
        self.total_bp_columns = num_columns + 3
        self.total_tp_columns = num_columns + 3
        self.capacity = 8192
        self.num_records = 0
        self.last_base_offset = 4096
        self.last_tail_offset = 4096
        self.last_base_page = -1
        self.last_tail_page = -1
        self.tail_page_directory = {}   # rid -> (tail page number, offset)
        self.tps = 0
        self.last_merged_tail_page = 0

    def addNewRecord(self, rid, *columns):
        # return a tuple (base page #, offset #)
        self.num_records += 1
        if self.last_base_offset == 4096:
            # last base page full; create new base page
            base_page = []
            for i in range(self.num_columns + 3):
                base_page.append(Page())
                self.table.bufferpool.get(base_page[-1], self.table.table_path, (self.page_range_num, len(self.base_page_array), i, self.total_bp_columns), 'bp', True, False)
            self.base_page_array.append(base_page)
            self.last_base_offset = 0
            self.last_base_page += 1

        page = self.base_page_array[self.last_base_page]
        # add the record values to pages
        for i in range(self.num_columns):
            self.table.bufferpool.get(page[i], self.table.table_path, (self.page_range_num, self.last_base_page, i, self.total_bp_columns), 'bp')
            page[i].write(columns[i], self.last_base_offset)
            self.table.bufferpool.decrement_and_markDirty(page[i])
        # add rid, indirection to page and schema encoding
        self.table.bufferpool.get(page[self.num_columns], self.table.table_path, (self.page_range_num, self.last_base_page, self.num_columns, self.total_bp_columns), 'bp')
        page[self.num_columns].write(rid, self.last_base_offset)
        self.table.bufferpool.get(page[self.num_columns + 1], self.table.table_path, (self.page_range_num, self.last_base_page, self.num_columns + 1, self.total_bp_columns), 'bp')
        page[self.num_columns + 1].write(0, self.last_base_offset)
        self.table.bufferpool.get(page[self.num_columns + 2], self.table.table_path, (self.page_range_num, self.last_base_page, self.num_columns + 2, self.total_bp_columns), 'bp')
        page[self.num_columns + 2].write(0, self.last_base_offset)
        self.table.bufferpool.decrement_and_markDirty(page[self.num_columns])
        self.table.bufferpool.decrement_and_markDirty(page[self.num_columns + 1])
        self.table.bufferpool.decrement_and_markDirty(page[self.num_columns + 2])

        self.last_base_offset += 8
        return (self.last_base_page, self.last_base_offset - 8)


    def updateRecord(self, base_record_rid, base_page_num, offset, *columns):
        new_rid = self.table.generate_unique_rid()
        # add new tail page if last one out of capacity
        if self.last_tail_offset == 4096:
            tail_page = []
            for i in range(self.num_columns + 3):
                tail_page.append(Page())
                self.table.bufferpool.get(tail_page[-1], self.table.table_path, (self.page_range_num, len(self.tail_page_array), i, self.total_tp_columns), 'tp', True, False)
            self.tail_page_array.append(tail_page)
            self.last_tail_offset = 0
            self.last_tail_page += 1
        
        page = self.tail_page_array[self.last_tail_page]
        self.tail_page_directory[new_rid] = (self.last_tail_page, self.last_tail_offset)

        self.table.bufferpool.get(self.base_page_array[base_page_num][self.num_columns], self.table.table_path, (self.page_range_num, base_page_num, self.num_columns, self.total_bp_columns), 'bp')
        self.table.bufferpool.get(self.base_page_array[base_page_num][self.num_columns + 1], self.table.table_path, (self.page_range_num, base_page_num, self.num_columns + 1, self.total_bp_columns), 'bp')
        self.table.bufferpool.get(self.base_page_array[base_page_num][self.num_columns + 2], self.table.table_path, (self.page_range_num, base_page_num, self.num_columns + 2, self.total_bp_columns), 'bp')
        
        schema_encoding = format(self.base_page_array[base_page_num][self.num_columns + 2].read(offset), '064b')
        if self.base_page_array[base_page_num][self.num_columns + 1].read(offset) == 0:
            # base record has not update, adding first tail record
            self.base_page_array[base_page_num][self.num_columns + 1].write(new_rid, offset)
            for i in range(self.num_columns):
                self.table.bufferpool.get(page[i], self.table.table_path, (self.page_range_num, self.last_tail_page, i, self.total_tp_columns), 'tp')
                if columns[i] == None:
                    page[i].write(0 , self.last_tail_offset)
                else:
                    page[i].write(columns[i], self.last_tail_offset)
                    schema_encoding = schema_encoding[:i + 1] + '1' + schema_encoding[i+2:]
                self.table.bufferpool.decrement_and_markDirty(page[i])
            self.table.bufferpool.get(page[self.num_columns], self.table.table_path, (self.page_range_num, self.last_tail_page, self.num_columns, self.total_tp_columns), 'tp')
            self.table.bufferpool.get(page[self.num_columns + 1], self.table.table_path, (self.page_range_num, self.last_tail_page, self.num_columns + 1, self.total_tp_columns), 'tp')
            self.table.bufferpool.get(page[self.num_columns + 2], self.table.table_path, (self.page_range_num, self.last_tail_page, self.num_columns + 2, self.total_tp_columns), 'tp')
            page[self.num_columns].write(new_rid, self.last_tail_offset)
            page[self.num_columns + 1].write(self.base_page_array[base_page_num][self.num_columns].read(offset), self.last_tail_offset)
            page[self.num_columns + 2].write(base_record_rid, self.last_tail_offset)
            self.table.bufferpool.decrement_and_markDirty(page[self.num_columns])
            self.table.bufferpool.decrement_and_markDirty(page[self.num_columns + 1])
            self.table.bufferpool.decrement_and_markDirty(page[self.num_columns + 2])
        else:
            # adding subsequent tail record
            prev_tail_rid = self.base_page_array[base_page_num][self.num_columns + 1].read(offset)
            prev_tail_record_location = self.tail_page_directory[prev_tail_rid]
            prev_tail_page = self.tail_page_array[prev_tail_record_location[0]]
            for i in range(self.num_columns):
                self.table.bufferpool.get(page[i], self.table.table_path, (self.page_range_num, self.last_tail_page, i, self.total_tp_columns), 'tp')
                if columns[i] is not None:
                    page[i].write(columns[i], self.last_tail_offset)
                    schema_encoding = schema_encoding[:i + 1] + '1' + schema_encoding[i+2:]
                elif schema_encoding[i + 1] == '1':
                    self.table.bufferpool.get(prev_tail_page[i], self.table.table_path, (self.page_range_num, prev_tail_record_location[0], i, self.total_tp_columns), 'tp')
                    page[i].write(prev_tail_page[i].read(prev_tail_record_location[1]), self.last_tail_offset)
                    self.table.bufferpool.decrementPin(prev_tail_page[i])
                else:
                    page[i].write(0, self.last_tail_offset)
                self.table.bufferpool.decrement_and_markDirty(page[i])
            self.table.bufferpool.get(page[self.num_columns], self.table.table_path, (self.page_range_num, self.last_tail_page, self.num_columns, self.total_tp_columns), 'tp')
            self.table.bufferpool.get(page[self.num_columns + 1], self.table.table_path, (self.page_range_num, self.last_tail_page, self.num_columns + 1, self.total_tp_columns), 'tp')
            self.table.bufferpool.get(page[self.num_columns + 2], self.table.table_path, (self.page_range_num, self.last_tail_page, self.num_columns + 2, self.total_tp_columns), 'tp')
            page[self.num_columns].write(new_rid, self.last_tail_offset)
            page[self.num_columns + 1].write(prev_tail_rid, self.last_tail_offset)
            page[self.num_columns + 2].write(base_record_rid, self.last_tail_offset)
            self.table.bufferpool.decrement_and_markDirty(page[self.num_columns])
            self.table.bufferpool.decrement_and_markDirty(page[self.num_columns + 1])
            self.table.bufferpool.decrement_and_markDirty(page[self.num_columns + 2])
            self.base_page_array[base_page_num][self.num_columns + 1].write(new_rid, offset)

        self.base_page_array[base_page_num][self.num_columns + 2].write(int(schema_encoding, 2), offset)
        self.table.bufferpool.decrementPin(self.base_page_array[base_page_num][self.num_columns])
        self.table.bufferpool.decrement_and_markDirty(self.base_page_array[base_page_num][self.num_columns + 1])
        self.table.bufferpool.decrement_and_markDirty(self.base_page_array[base_page_num][self.num_columns + 2])
        self.last_tail_offset += 8
        return True

    def hasCapacity(self):
        return self.num_records < self.capacity

    def readRecord(self, base_page_num, offset, query_columns):
        record = []
        page = self.base_page_array[base_page_num]
        self.table.bufferpool.get(page[self.num_columns + 2], self.table.table_path, (self.page_range_num, base_page_num, self.num_columns + 2, self.total_bp_columns), 'bp')
        schema_encoding = format(page[self.num_columns + 2].read(offset), '064b')
        self.table.bufferpool.decrementPin(page[self.num_columns + 2])

        # if value in redirection, use latest tail entry to get query columns
        self.table.bufferpool.get(page[self.num_columns + 1], self.table.table_path, (self.page_range_num, base_page_num, self.num_columns + 1, self.total_bp_columns), 'bp')
        redirect_val = page[self.num_columns + 1].read(offset)
        self.table.bufferpool.decrementPin(page[self.num_columns + 1])
        if redirect_val != 0:
            location = self.tail_page_directory[redirect_val]
            tail_page = self.tail_page_array[location[0]]
            for col in query_columns:
                if schema_encoding[col + 1] == '1':
                    self.table.bufferpool.get(tail_page[col], self.table.table_path, (self.page_range_num, location[0], col, self.total_tp_columns), 'tp')
                    record.append(tail_page[col].read(location[1]))
                    self.table.bufferpool.decrementPin(tail_page[col])
                else:
                    self.table.bufferpool.get(page[col], self.table.table_path, (self.page_range_num, base_page_num, col, self.total_bp_columns), 'bp')
                    record.append(page[col].read(offset))
                    self.table.bufferpool.decrementPin(page[col])
        else:
            for col in query_columns:
                self.table.bufferpool.get(page[col], self.table.table_path, (self.page_range_num, base_page_num, col, self.total_bp_columns), 'bp')
                record.append(page[col].read(offset))
                self.table.bufferpool.decrementPin(page[col])
        return record

    def travel_readRecord(self, base_page_num, offset, query_columns, relative_version):
        record = []
        page = self.base_page_array[base_page_num]

        self.table.bufferpool.get(page[self.num_columns + 2], self.table.table_path, (self.page_range_num, base_page_num, self.num_columns + 2, self.total_bp_columns), 'bp')
        schema_encoding = format(page[self.num_columns + 2].read(offset), '064b')
        self.table.bufferpool.decrementPin(page[self.num_columns + 2])
        self.table.bufferpool.get(page[self.num_columns + 1], self.table.table_path, (self.page_range_num, base_page_num, self.num_columns + 1, self.total_bp_columns), 'bp')
        redirect_val = page[self.num_columns + 1].read(offset)
        self.table.bufferpool.decrementPin(page[self.num_columns + 1])

        if redirect_val == 0:
            for col in query_columns:
                self.table.bufferpool.get(page[col], self.table.table_path, (self.page_range_num, base_page_num, col, self.total_bp_columns), 'bp')
                record.append(page[col].read(offset))
                self.table.bufferpool.decrementPin(page[col])
            return record
        redirect_version = 0
        while(True):
            if (redirect_version == relative_version):
                break
            if (redirect_val not in self.tail_page_directory):
                break
            pos = self.tail_page_directory[redirect_val]
            self.table.bufferpool.get(self.tail_page_array[pos[0]][self.num_columns + 1], self.table.table_path, (self.page_range_num, pos[0], self.num_columns + 1, self.total_tp_columns), 'tp')
            redirect_val = self.tail_page_array[pos[0]][self.num_columns + 1].read(pos[1])
            self.table.bufferpool.decrementPin(self.tail_page_array[pos[0]][self.num_columns + 1])
            redirect_version -= 1
        if (redirect_val in self.tail_page_directory):
            pos = self.tail_page_directory[redirect_val]
            for col in query_columns:
                if schema_encoding[col + 1] != '1':
                    self.table.bufferpool.get(page[col], self.table.table_path, (self.page_range_num, base_page_num, col, self.total_bp_columns), 'bp')
                    record.append(page[col].read(offset))
                    self.table.bufferpool.decrementPin(page[col])
                else:
                    self.table.bufferpool.get(self.tail_page_array[pos[0]][col], self.table.table_path, (self.page_range_num, pos[0], col, self.total_tp_columns), 'tp')
                    record.append(self.tail_page_array[pos[0]][col].read(pos[1]))
                    self.table.bufferpool.decrementPin(self.tail_page_array[pos[0]][col])
        else:
            for col in query_columns:
                self.table.bufferpool.get(page[col], self.table.table_path, (self.page_range_num, base_page_num, col, self.total_bp_columns), 'bp')
                record.append(page[col].read(offset))
                self.table.bufferpool.decrementPin(page[col])
        return record

    def removeRecord(self, base_page_num, offset):
        page = self.base_page_array[base_page_num]
        self.table.bufferpool.get(page[self.num_columns], self.table.table_path, (self.page_range_num, base_page_num, self.num_columns, self.total_bp_columns), 'bp')
        page[self.num_columns].write(0, offset)
        self.table.bufferpool.decrement_and_markDirty(page[self.num_columns])
        return True

    def get_basepage_copy(self):
        if self.last_tail_page <= self.last_merged_tail_page:
            return [], [], None
        
        tail_array = []
        for i in range(self.last_merged_tail_page, self.last_tail_page):
            tail_array.append(self.tail_page_array[i])
        base_array = []

        for i in range(self.last_base_page):
            base_array.append(self.base_page_array[i][:self.num_columns])
        offset = self.last_merged_tail_page
        self.last_merged_tail_page = self.last_tail_page
        return (base_array, tail_array, offset)


    def timetravel_readRecord(self, base_page_num, offset, query_columns, relative_version):
        record = []
        page = self.base_page_array[base_page_num]

        self.table.bufferpool.get(page[self.num_columns + 2], self.table.table_path, (self.page_range_num, base_page_num, self.num_columns + 2, self.total_bp_columns), 'bp')
        schema_encoding = format(page[self.num_columns + 2].read(offset), '064b')
        self.table.bufferpool.decrementPin(page[self.num_columns + 2])
        self.table.bufferpool.get(page[self.num_columns + 1], self.table.table_path, (self.page_range_num, base_page_num, self.num_columns + 1, self.total_bp_columns), 'bp')
        redirect_val = page[self.num_columns + 1].read(offset)
        self.table.bufferpool.decrementPin(page[self.num_columns + 1])

        if redirect_val == 0:
            for col in query_columns:
                self.table.bufferpool.get(page[col], self.table.table_path, (self.page_range_num, base_page_num, col, self.total_bp_columns), 'bp')
                record.append(page[col].read(offset))
                self.table.bufferpool.decrementPin(page[col])
            return record
        redirect_version = 0
        while(True):
            if (redirect_version == relative_version):
                break
            if (redirect_val not in self.tail_page_directory):
                break
            pos = self.tail_page_directory[redirect_val]
            self.table.bufferpool.get(self.tail_page_array[pos[0]][self.num_columns + 1], self.table.table_path, (self.page_range_num, pos[0], self.num_columns + 1, self.total_tp_columns), 'tp')
            redirect_val = self.tail_page_array[pos[0]][self.num_columns + 1].read(pos[1])
            self.table.bufferpool.decrementPin(self.tail_page_array[pos[0]][self.num_columns + 1])
            redirect_version -= 1
        if (redirect_val in self.tail_page_directory):
            pos = self.tail_page_directory[redirect_val]
            for col in query_columns:
                if schema_encoding[col + 1] != '1':
                    self.table.bufferpool.get(page[col], self.table.table_path, (self.page_range_num, base_page_num, col, self.total_bp_columns), 'bp')
                    record.append(page[col].read(offset))
                    self.table.bufferpool.decrementPin(page[col])
                else:
                    self.table.bufferpool.get(self.tail_page_array[pos[0]][col], self.table.table_path, (self.page_range_num, pos[0], col, self.total_tp_columns), 'tp')
                    record.append(self.tail_page_array[pos[0]][col].read(pos[1]))
                    self.table.bufferpool.decrementPin(self.tail_page_array[pos[0]][col])
        else:
            for col in query_columns:
                self.table.bufferpool.get(page[col], self.table.table_path, (self.page_range_num, base_page_num, col, self.total_bp_columns), 'bp')
                record.append(page[col].read(offset))
                self.table.bufferpool.decrementPin(page[col])
        return record
