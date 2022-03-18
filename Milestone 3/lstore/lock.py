import threading
from lstore.table import Table, Record
from lstore.index import Index

class Lock():
    def __init__(self, val = 0):
        self.lock = threading.Lock()
        self.count = val
        pass

    def acquire_read(self):
        # can't acquire lock if any thread is currently writing
        self.lock.acquire()
        if self.count < 0:
            self.lock.release()
            return False
        self.count += 1
        self.lock.release()
        return True

    def acquire_write(self):
        # can't acquire lock if any threads are currently reading
        self.lock.acquire()
        if self.count != 0:
            self.lock.release()
            return False
        self.count = -1
        self.lock.release()
        return True

    def release_read(self):
        self.lock.acquire()
        self.count -= 1
        self.lock.release()

    def release_write(self):
        self.lock.acquire()
        self.count += 1
        self.lock.release()

    def upgrade_to_write(self):
        self.lock.acquire()
        if self.count == 1:
            self.count = -1
            self.lock.release()
            return True
        self.lock.release()
        return False