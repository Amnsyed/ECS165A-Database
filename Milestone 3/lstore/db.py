from lstore.table import Table
from lstore.bufferpool import BufferPool
import lstore.config as config
import os
import pickle
import threading
import time

class Database():

    def __init__(self):
        self.tables = []
        self.path = None
        self.bufferpool = None
        self.running = True

    def open(self, path):
        if path[-1] != '/':
            path = path + '/'
        self.path = path
        self.bufferpool = BufferPool(self.path)
        
        # if folder doesn't exist, create one
        pathExists = os.path.exists(path)
        if not pathExists:
            os.makedirs(path)

        # if metadata exists, load it in
        metadataPath = path + 'metadata.pkl'
        if os.path.exists(metadataPath):
            with open(metadataPath, 'rb') as inp:
                tables = pickle.load(inp)
                self.tables = tables
        for table in self.tables:
            table.bufferpool = self.bufferpool
            table.locks = []
            table.rid_generate = threading.Lock()
            table.new_record = threading.Lock()
            table.update_record = threading.Lock()
            for t in table.pagerange_array:
                table.locks.append(threading.Lock())
        
        self.mergeLock = threading.Lock()
        thread = threading.Thread(target=self.background_merge, daemon=1)
        thread.start()

    def background_merge(self):
        while 1:
            time.sleep(config.vars['merge_sleep'])
            self.mergeLock.acquire()
            try:
                if self.running:
                    for i in range(len(self.tables)):
                        self.tables[i].merge()
            except:
                print("error in merge thread")
            self.mergeLock.release()
            time.sleep(config.vars['merge_sleep'])
        

    def close(self):
        # clear all bufferpools
        self.mergeLock.acquire()
        self.running = False
        self.bufferpool.clearPool()
        for table in self.tables:
            table.bufferpool = None
            table.locks = None
            table.rid_generate = None
            table.new_record = None
            table.lock_manager = {}
            table.update_record = None
        self.mergeLock.release()

        # if folder doesn't exist, create one
        pathExists = os.path.exists(self.path)
        if not pathExists:
            os.makedirs(self.path)
        
        # write metadata of all tables to metadata.pkl
        metadataPath = self.path + 'metadata.pkl'
        with open(metadataPath, 'wb') as outp:
            pickle.dump(self.tables, outp, pickle.HIGHEST_PROTOCOL)

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        table.bufferpool = self.bufferpool
        table.rid_generate = threading.Lock()
        table.new_record = threading.Lock()
        self.tables.append(table)
        if not os.path.exists(self.path + name):
            os.makedirs(self.path + name)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        for table in self.tables:
            if table.name == name:
                self.tables.remove(table)

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for table in self.tables:
            if table.name == name:
                return table
        
