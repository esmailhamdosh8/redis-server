import threading
from collections import deque


class State:
    def __init__(self):
        self.cache = {}
        self.lists = {}
        self.streams = {}

        self.replica = False
        self.replication_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        self.replication_offset = 0
        self.offset = 0
        self.slaves = []

        self.commands_to_propogate = deque()
        self.specific_to_propotgate = deque()
        self.to_become_slave = deque()
        self.propogate_to_slave_commands = (b'SET', )

        self.sent_bytes = 0
        self.slaves_offset = {}

        self.RDB_KEYS = []


state = State()

thread_state = threading.local()