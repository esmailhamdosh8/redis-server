from .glb import state
from collections import deque
import time
import math

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
            last_milliseconds, last_seq = stream[-1][:2]
            if milliseconds > last_milliseconds:
                seq = 0
            else:
                milliseconds = max(milliseconds, last_milliseconds)
                seq = last_seq + 1
    else:
        milliseconds, _, seq = entry_id.partition(b"-")
        milliseconds = int(milliseconds)
        if stream and milliseconds < stream[-1][0]:
            if milliseconds == 0 and seq == b"0":
                raise ValueError(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
            raise ValueError(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
        if seq != b"*":
            seq = int(seq)
            if stream and milliseconds == stream[-1][0] and seq <= stream[-1][1]:
                if milliseconds == 0 and seq == 0:
                    raise ValueError(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
                raise ValueError(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
        elif not stream:
            if milliseconds == 0:
                seq = 1
            else:
                seq = 0
        else:
            last_milliseconds, last_seq = stream[-1][:2]
            if milliseconds > last_milliseconds:
                seq = 0
            else:
                seq = last_seq + 1
    if milliseconds == 0 and seq == 0: raise ValueError(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
    return milliseconds, seq

def XADD(args, request_ms):
    response = None
    stream_key, entry_id, *parts = args
    stream_key, entry_id = bytes(stream_key), bytes(entry_id)
    if stream_key not in state.streams:
        state.streams[stream_key] = []
    stream = state.streams[stream_key]
    try:
        milliseconds, seq = generate_entry_id(entry_id, stream)
        stream.append([milliseconds, seq, {}])
        entry = stream[-1][2]
        n = len(parts)
        for i in range(0, n, 2):
            key_entry = bytes(parts[i])
            val_entry = parts[i + 1]
            entry[key_entry] = val_entry
        processed_entry_id = f"{milliseconds}-{seq}"
        response = f"${len(processed_entry_id)}\r\n{processed_entry_id}\r\n".encode()
    except ValueError as e:
        response = e.args[0]
    return response

def XRANGE(args, request_ms):
    stream_key, entry_id_1, entry_id_2 = args[:3]
    stream_key, entry_id_1, entry_id_2 = bytes(stream_key), bytes(entry_id_1), bytes(entry_id_2)
    if entry_id_1 == b"-":
        milli_1 = 0
        seq_1 = 0
    elif b"-" in entry_id_1:
        milli_1, _, seq_1 = entry_id_1.partition(b"-")
        milli_1, seq_1 = int(milli_1), int(seq_1)
        print(milli_1, seq_1)
    else:
        milli_1 = int(entry_id_1)
        seq_1 = 0

    if entry_id_2 == b"+":
        milli_2, seq_2 = math.inf, math.inf
    elif b"-" in entry_id_2:
        milli_2, _, seq_2 = entry_id_2.partition(b"-")
        milli_2, seq_2 = int(milli_2), int(seq_2)
    else:
        milli_2 = int(entry_id_2)
        seq_2 = math.inf
    stream = state.streams[stream_key]
    start_index, end_index = None, None
    start, end = 0, len(stream) - 1
    while start < end:
        mid = (start + end) // 2
        if stream[mid][0] < milli_1:
            start = mid + 1
        elif stream[mid][0] == milli_1 and stream[mid][1] < seq_1:
            start = mid + 1
        else:
            end = mid
    start_index = start
    start, end = 0, len(stream) - 1
    while start < end:
        mid = (start + end + 1) // 2
        if stream[mid][0] > milli_2:
            end = mid - 1
        elif stream[mid][0] == milli_2 and stream[mid][1] > seq_2:
            end = mid - 1
        else:
            start = mid
    
    end_index = end
    response = f"*{end_index - start_index + 1}\r\n".encode()
    for i in range(start_index, end_index + 1):
        entry_id = f"{stream[i][0]}-{stream[i][1]}"
        entry = stream[i][2]
        response += f"*2\r\n${len(entry_id)}\r\n{entry_id}\r\n*{len(entry) * 2}\r\n".encode()
        for key, val in entry.items():
            response += f"${len(key)}\r\n".encode() + key + f"\r\n${len(val)}\r\n".encode() + val + b"\r\n"
    return response