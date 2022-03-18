import os
import pickle
from pathlib import Path
import threading
import lstore.config as config
#doubly linked list
#head is most recent
#evict from tail

class Node():
    def __init__(self, x = 0):
        self.page = None
        self.location = None
        self.subpath = None
        self.pageType = None
        self.dirty = False
        self.pin = x
        self.next = None
        self.prev = None

# implement doubly linked list and a hash table with page to node mappings
# every node in linked list is also in hash table
# use hash table to get node
class BufferPool():
    def __init__(self, path):
        self.path = path
        self.capacity = config.vars['bufferpool_size']
        self.count = 0
        self.head = Node()
        self.tail = Node()
        self.pool = {}
        self.lock = threading.Lock()

        self.head.next = self.tail
        self.tail.prev = self.head

    # page = Page class object; location = (PR, BP, column, total columns); newPage
    # use LRU as eviction method
    def get(self, page, subpath, location, pageType, newPage = False, inc = True):
        self.lock.acquire()
        # If page in dictionary, move it to beginning of linked list and return   
        if page in self.pool:
            if inc:
                self.pool[page].pin += 1
            self.move_to_head(self.pool[page])
            self.lock.release()
            return page
        
        # if newPage: create new node and add it to bufferpool
        newNode = Node()
        newNode.page = page
        newNode.location = location
        newNode.subpath = subpath
        newNode.pageType = pageType
        if inc:
            newNode.pin += 1
        if newPage:
            newNode.dirty = True
            self.put(newNode, page)
        # if old page: read it from disk, create new node and add it to bufferpool
        else:
            newNode.page.data = self.read(location, subpath, pageType)
            self.put(newNode, page)
        self.lock.release()
        return page

    # page: page object for this location; location: (PR, BP, column, total columns)
    def read(self, location, subpath, pageType):
        # calculate the memory offset to read
        offset = (location[1] * (location[3] * 4096)) + (location[2] * 4096)    # [BP * (total columns  * 4096)] + [column requested * 4096]
        
        # open the specific file (naming format: PR0bp.txt, PR1tp.txt) and read 4096 bytes from the offset.
        p = self.path + subpath + 'PR' + str(location[0]) + pageType + '.txt'       # ex: ./Grades/PR0bp.txt
        with open(p, 'rb') as inp:
            inp.seek(offset)
            data = bytearray(inp.read(4096))
        return data

    # page: page object for this location; location: (PR, BP, column, total columns)
    def write(self, location, page, subpath, pageType):
        # calculate the memory offset to read
        offset = (location[1] * (location[3] * 4096)) + (location[2] * 4096)    # [BP * (total columns  * 4096)] + [column requested * 4096]
        
        # open the specific file (naming format: PR0bp.txt, PR1tp.txt) and read 4096 bytes from the offset.
        p = self.path + subpath + 'PR' + str(location[0]) + pageType + '.txt'
        Path(p).touch(exist_ok=True)
        with open(p, 'r+b') as out:
            out.seek(offset)
            out.write(page.data)
        return page

    def incrementPin(self, page):
        self.lock.acquire()
        self.pool[page].pin += 1
        self.lock.release()

    def decrementPin(self, page):
        self.lock.acquire()
        self.pool[page].pin -= 1
        self.lock.release()

    def mark_dirty(self, page):
        self.lock.acquire()
        self.pool[page].dirty = True
        self.lock.release()

    def decrement_and_markDirty(self, page):
        self.lock.acquire()
        self.pool[page].pin -= 1
        self.pool[page].dirty = True
        self.lock.release()

    def clearPool(self):
        self.lock.acquire()
        # evict all pages from bufferpool
        while self.count > 0:
            self.evict(False)
        self.lock.release()

    def put(self, node, page):
        if self.count >= self.capacity:
            self.evict()
        self.pool[page] = node
        self._addNode(node)

    # evicts last node in linked list
    def evict(self, notClose = True):
        # if tail is dirty, write to memory
        node = self.tail.prev
        if node.pin > 0 and notClose:
            return
        if node.dirty:
            self.write(node.location, node.page, node.subpath, node.pageType)
        # remove it from linked list and dictionary
        node.page.data = None       # clear the allocated memory
        self._removeNode(node)
        del self.pool[node.page]

    # remove Node from anywhere in linked list
    def _removeNode(self, node):
        self.count -= 1
        
        prev = node.prev
        next = node.next
        prev.next = next
        next.prev = prev
    
    # add Node to head
    def _addNode(self, node):
        self.count += 1

        node.prev = self.head
        node.next = self.head.next
        self.head.next.prev = node
        self.head.next = node

    def move_to_head(self, node):
        self._removeNode(node)
        self._addNode(node)