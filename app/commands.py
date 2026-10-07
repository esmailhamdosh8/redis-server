from .glb import state
from collections import deque
import time

def PING(args, request_ms):
    return b"+PONG\r\n"

def ECHO(args, request_ms):
    return f"${len(args[0])}\r\n".encode() + args[0] + b"\r\n"

def SET(args, request_ms):
    key, value = bytes(args[0]), args[1]
    if len(args) > 2:
        duration_ms = int(args[3]) if args[2] == b"PX" else 1000 * args[3]
        state.cache[key] = (value, request_ms, duration_ms)
    else:
        state.cache[key] = (value, -1, -1)
    return b"+OK\r\n"

def GET(args, request_ms):
    key = bytes(args[0])
    if key not in state.cache:
        response = b"$-1\r\n"
    else:
        value, saved_ms, duration_ms = state.cache[key]
        if saved_ms == -1 or duration_ms == -1 or request_ms - saved_ms <= duration_ms:
            response = f"${len(value)}\r\n".encode() + value + b"\r\n"
        else:
            del state.cache[key]
            response = b"$-1\r\n"
    return response

def RPUSH(args, request_ms):
    key = bytes(args[0])
    if key not in state.lists:
        state.lists[key] = deque()
    for value in args[1:]:
        state.lists[key].append(value)
    return f":{len(state.lists[key])}\r\n".encode()

def LPUSH(args, request_ms):
    key = bytes(args[0])
    if key not in state.lists:
        state.lists[key] = deque()
    for value in args[1:]:
        state.lists[key].appendleft(value)
    return f":{len(state.lists[key])}\r\n".encode()

def LRANGE(args, request_ms):
    key = bytes(args[0])
    if key not in state.lists:
        state.lists[key] = deque()
        response = b"*0\r\n"
    else:
        n = len(state.lists[key])
        def helper(x):
            if x >= n: return n - 1
            if x >= 0: return x
            if -x > n: return 0
            return x + n
        start, stop = helper(int(args[1])), helper(int(args[2]))
        if start > stop:
            response = b"*0\r\n"
        else:
            response = f"*{stop - start + 1}\r\n".encode()
            i = 0
            for val in state.lists[key]:
                if i < start: i += 1; continue
                if i > stop: break
                response += f"${len(val)}\r\n".encode() + val + b"\r\n"
                i += 1
    return response

def LLEN(args, request_ms):
    key = bytes(args[0])
    if key not in state.lists: 
        response = b":0\r\n"
    else:
        response = f":{len(state.lists[key])}\r\n".encode()
    return response

def LPOP(args, request_ms):
    key = bytes(args[0])
    if key not in state.lists or not state.lists[key]:
        response = b"$-1\r\n"
    else:
        num_to_remove = 1 if len(args) == 1 else min(int(args[1]), len(state.lists[key]))
        response = f"*{num_to_remove}\r\n".encode() if num_to_remove > 1 else b""
        for _ in range(num_to_remove):
            pop_value = state.lists[key].popleft()
            response += f"${len(pop_value)}\r\n".encode() + pop_value + b"\r\n"
    return response

def BLPOP(args, request_ms):
    key = bytes(args[0])
    timeout = float(args[1])
    deadline = None if timeout == 0 else time.monotonic() + timeout
    if key not in state.lists:
        state.lists[key] = deque()
    while True:
        if state.lists[key]:
            pop_value = state.lists[key].popleft()
            response = f"*2\r\n${len(key)}\r\n".encode() + key +\
                    f"\r\n${len(pop_value)}\r\n".encode() + pop_value + b"\r\n"
            break
        if deadline is not None and time.monotonic() > deadline:
            response = b"*-1\r\n"
            break
        time.sleep(0.01)
    return response

def TYPE(args, request_ms):
    key = bytes(args[0])
    if key in state.cache:
        response = b"+string\r\n"
    elif key in state.streams:
        response = b"+stream\r\n"
    else:
        response = b"+none\r\n" 
    return response

# XADD helper
def generate_entry_id(entry_id, stream):
    if entry_id == b"*":
        milliseconds = int(time.time() * 1000)
        if not stream:
            seq = 0
        else:
            last_milliseconds, _, last_seq = stream[-1][0].partition(b"-")
            last_milliseconds, last_seq = int(last_milliseconds), int(last_seq)
            if milliseconds > last_milliseconds:
                seq = 0
            else:
                milliseconds = max(milliseconds, last_milliseconds)
                seq = last_seq + 1
    else:
        milliseconds, _, seq = entry_id.partition(b"-")
        milliseconds = int(milliseconds)
        if seq != b"*":
            seq = int(seq)
        elif not stream:
            if milliseconds == 0:
                seq = 1
            else:
                seq = 0
        else:
            last_milliseconds, _, last_seq = stream[-1][0].partition(b"-")
            last_milliseconds, last_seq = int(last_milliseconds), int(last_seq)
            if milliseconds > last_milliseconds:
                seq = 0
            else:
                seq = last_seq + 1
    return f"{milliseconds}-{seq}".encode()     


def XADD(args, request_ms):
    response = None
    stream_key, entry_id, *parts = args
    stream_key, entry_id = bytes(stream_key), bytes(entry_id)
    if stream_key not in state.streams:
        state.streams[stream_key] = []
    stream = state.streams[stream_key]
    entry_id = generate_entry_id(entry_id, stream)
    milliseconds, _, seq = entry_id.partition(b"-")
    milliseconds, seq = int(milliseconds), int(seq)
    if milliseconds == 0 and seq == 0:
        response = b"-ERR The ID specified in XADD must be greater than 0-0\r\n"
    elif not stream:
        stream.append([entry_id, {}])
    else:
        last_entry_id, _ = stream[-1]
        last_milli, _, last_seq = last_entry_id.partition(b"-")
        last_milli, last_seq = int(last_milli), int(last_seq)
        if milliseconds > last_milli:
            stream.append([entry_id, {}])
        elif milliseconds == last_milli and seq > last_seq:
            stream.append([entry_id, {}])
        else:
            response = b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
    if response is None:
        entry = stream[-1][1]
        n = len(parts)
        for i in range(0, n, 2):
            key_entry = bytes(parts[i])
            val_entry = parts[i + 1]
            entry[key_entry] = val_entry
        response = f"${len(entry_id)}\r\n".encode() + entry_id + b"\r\n"
    return response

