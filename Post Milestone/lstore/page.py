
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
        self.capacity = 512

    def has_capacity(self):
        return self.num_records <= self.capacity

    def write(self, value, offset):
        self.data[offset : offset+8] = value.to_bytes(8, byteorder='little', signed=True)
        self.num_records += 1

    def read(self, offset):
        return int.from_bytes(self.data[offset: offset+8], byteorder='little', signed=True)

