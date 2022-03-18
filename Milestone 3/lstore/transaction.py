from lstore.query import Query
from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock import Lock

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.table = None
        self.key_idx = None
        self.read_locks = set()
        self.write_locks = set()
        self.insert_locks = set()

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        if len(self.queries) == 1:
            self.table = table
            self.key_idx = table.key
        # use grades_table for aborting

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            if args[self.key_idx] not in self.table.lock_manager:
                self.insert_locks.add(args[0])
                self.table.lock_manager[args[0]] = Lock(val=-1)
            if args[self.key_idx] not in self.write_locks and args[self.key_idx] not in self.insert_locks:
                if self.table.lock_manager[args[0]].acquire_write():
                    self.write_locks.add(args[self.key_idx])
                else:
                    return self.abort()
            '''
            if query == Query.insert:
                self.insert_locks.add(args[self.key_idx])
                self.table.lock_manager[args[self.key_idx]] = Lock(val=-1)
            elif query == Query.select:
                if args[0] in self.read_locks or args[0] in self.write_locks or args[0] in self.insert_locks:
                    continue
                else:
                    if self.table.lock_manager[args[0]].acquire_read():
                        self.read_locks.add(args[0])
                    else:
                        return self.abort()
            elif query == Query.update or query == Query.delete:
                if args[0] in self.read_locks:
                    if self.table.lock_manager[args[0]].upgrade_to_write():
                        self.read_locks.remove(args[0])
                        self.write_locks.add(args[0])
                    else:
                        return self.abort()
                elif args[0] in self.write_locks or args[0] in self.insert_locks:
                    continue
                else:
                    if self.table.lock_manager[args[0]].acquire_write():
                        self.write_locks.add(args[0])
                    else:
                        return self.abort()
            elif query == Query.sum:
                #acquire all locks needed for sum
                pass
                '''
        return self.commit()


    def abort(self):
        #TODO: do roll-back and any other necessary operations
        for key in self.read_locks:
            self.table.lock_manager[key].release_read()
        for key in self.write_locks:
            self.table.lock_manager[key].release_write()
        for key in self.insert_locks:
            del self.table.lock_manager[key]
        return False

    def commit(self):
        # TODO: commit to database
        for query, args in self.queries:
            query(*args)
            # remove lock from lock manager after deleting record
            if query == Query.delete:
                del self.table.lock_manager[args[0]]
                if args[0] in self.write_locks:
                    self.insert_locks.remove(args[0])
                if args[0] in self.insert_locks:
                    self.insert_locks.remove(args[0])
        for key in self.read_locks:
            self.table.lock_manager[key].release_read()
        for key in self.write_locks:
            self.table.lock_manager[key].release_write()
        for key in self.insert_locks:
            self.table.lock_manager[key].release_write()
        return True