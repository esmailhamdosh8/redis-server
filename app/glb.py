class State:
    def __init__(self):
        self.cache = {}
        self.lists = {}
        self.streams = {}
        self.queue = False
        self.command_queue = []


state = State()

