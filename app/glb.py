import threading

class State:
    def __init__(self):
        self.cache = {}
        self.lists = {}
        self.streams = {}
        self.replica = False


state = State()

thread_state = threading.local()