import threading

class State:
    def __init__(self):
        self.cache = {}
        self.lists = {}
        self.streams = {}



state = State()

thread_state = threading.local()