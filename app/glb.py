import threading
from collections import deque


class State:
    def __init__(self):
        self.cache = {}
        self.lists = {}
        self.streams = {}
        self.sorted = {}

        self.replica = False
        self.replication_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        self.replication_offset = 0
        self.offset = 0
        self.slaves = []

        self.to_become_slave = deque()

        self.write_commands = (b'SET', b'RPUSH', b'LPUSH', b'LPOP', b'BLPOP', b'XADD', b'INCR')
        self.multi_commands = (b'EXEC', b'DISCARD', b'WATCH', b'MULTI')
        self.slave_commands = deque()
        self.subsc_commands = (b'SUBSCRIBE', b'UNSUBSCRIBE', b'PING', b'QUIT', b'RESET')

        self.sent_bytes = 0
        self.slaves_offset = {}

        self.RDB_KEYS = []

        self.channels = {}

        self.users = {b'default': None}


state = State()

thread_state = threading.local()