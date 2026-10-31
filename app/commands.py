from .glb import *
from collections import deque
from sortedcontainers import SortedSet
import time
import math
import threading
import bisect
import os

from .main import parser

def build_response(parts: list[bytearray], request_ms: float) -> bytes:
    parts = [bytes(part) for part in parts]
    command, *args = parts
    if len(state.channels[thread_state.slave_conn]) >= 1 and command not in state.subsc_commands:
        return f"-ERR Can't execute '{command.decode()}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n".encode()
    elif thread_state.in_multi and command not in state.multi_commands:
        thread_state.command_queue.append((parts, request_ms))
        return b"+QUEUED\r\n"
    else:
        response = globals()[command.decode()](args, request_ms)

    if command in state.write_commands:
        flush_to_disk(parts)
        full_command = build_command(parts)
        state.sent_bytes += len(full_command)
        for slave_conn, _, _ in state.slaves:
            state.slave_commands.append((slave_conn, full_command))
    return response

def build_command(parts):
    return f"*{len(parts)}\r\n".encode() + b"\r\n".join(f"${len(part)}\r\n".encode() + part for part in parts) + b"\r\n"

def PING(args: list[bytes], request_ms: float) -> bytes:
    if len(state.channels[thread_state.slave_conn]) == 0:
        return b"+PONG\r\n"
    return b"*2\r\n$4\r\npong\r\n$0\r\n\r\n"

def ECHO(args: list[bytes], request_ms: float) -> bytes:
    return f"${len(args[0])}\r\n".encode() + args[0] + b"\r\n"

def SET(args: list[bytes], request_ms: float) -> bytes:
    key, value = args[0], args[1]
    if key in state.lists:
        del state.lists[key]
    if key in state.streams:
        del state.streams[key]
    if len(args) > 2:
        duration_ms = int(args[3]) if args[2] == b"PX" else 1000 * int(args[3])
        state.cache[key] = [value, (request_ms, duration_ms)]
    else:
        state.cache[key] = [value, None]
    return b"+OK\r\n"

def GET(args: list[bytes], request_ms: float) -> bytes:
    key = args[0]
    if key in state.lists or key in state.streams or key in state.streams:
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    if key not in state.cache:
        response = b"$-1\r\n"
    else:
        value, info_ms = state.cache[key]
        if info_ms is None or request_ms - info_ms[0] <= info_ms[1]:
            if isinstance(value, int):
                length = value.bit_length() // 8 + 1
                value = value.to_bytes(length, byteorder='big')
            response = f"${len(value)}\r\n".encode() + value + b"\r\n"
        else:
            del state.cache[key]
            response = b"$-1\r\n"
    return response

def RPUSH(args: list[bytes], request_ms: float) -> bytes:
    key = args[0]
    if key in state.cache or key in state.streams:
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    if key not in state.lists:
        state.lists[key] = deque()
    for value in args[1:]:
        state.lists[key].append(value)
    return f":{len(state.lists[key])}\r\n".encode()

def LPUSH(args: list[bytes], request_ms: float) -> bytes:
    key = args[0]
    if key in state.cache or key in state.streams:
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    if key not in state.lists:
        state.lists[key] = deque()
    for value in args[1:]:
        state.lists[key].appendleft(value)
    return f":{len(state.lists[key])}\r\n".encode()

def LRANGE(args: list[bytes], request_ms: float) -> bytes:
    key = args[0]
    if key in state.cache or key in state.streams:
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    if key not in state.lists:
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

def LLEN(args: list[bytes], request_ms: float) -> bytes:
    key = args[0]
    if key in state.cache or key in state.streams:
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    if key not in state.lists: 
        response = b":0\r\n"
    else:
        response = f":{len(state.lists[key])}\r\n".encode()
    return response

def LPOP(args: list[bytes], request_ms: float) -> bytes:
    key = args[0]
    if key in state.cache or key in state.streams:
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    if key not in state.lists or not state.lists[key]:
        response = b"$-1\r\n"
    else:
        num_to_remove = 1 if len(args) == 1 else min(int(args[1]), len(state.lists[key]))
        response = f"*{num_to_remove}\r\n".encode() if len(args) > 1 else b""
        for _ in range(num_to_remove):
            pop_value = state.lists[key].popleft()
            response += f"${len(pop_value)}\r\n".encode() + pop_value + b"\r\n"
    return response

def BLPOP(args: list[bytes], request_ms: float) -> bytes:
    key = args[0]
    if key in state.cache or key in state.streams:
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    if key not in state.lists:
        state.lists[key] = deque()
    timeout = float(args[1])
    deadline = None if timeout == 0 else time.time() + timeout
    while True:
        if state.lists[key]:
            pop_value = state.lists[key].popleft()
            response = f"*2\r\n${len(key)}\r\n".encode() + key +\
                    f"\r\n${len(pop_value)}\r\n".encode() + pop_value + b"\r\n"
            break
        if deadline is not None and time.time() > deadline:
            response = b"*-1\r\n"
            break
        time.sleep(0.01)
    return response

def TYPE(args: list[bytes], request_ms: float) -> bytes:
    key = args[0]
    if key in state.cache:
        _, info_ms = state.cache[key]
        if info_ms is None or request_ms - info_ms[0] <= info_ms[1]:
            response = b"+string\r\n"
        else:
            del state.cache[key]
            response = b"+none\r\n"
    elif key in state.streams:
        response = b"+stream\r\n"
    elif key in state.lists:
        response = b"+lists\r\n"
    else:
        response = b"+none\r\n" 
    return response

# XADD helper
def generate_entry_id(entry_id: bytes, stream: list[list]) -> tuple[int, int]:
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
            if milliseconds == 0 and seq == 0:
                raise ValueError(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
            if stream and milliseconds == stream[-1][0] and seq <= stream[-1][1]:
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
    return milliseconds, seq

def XADD(args: list[bytes], request_ms: float) -> bytes:
    response = None
    stream_key, entry_id, *parts = args
    if stream_key in state.cache or stream_key in state.lists:
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    if stream_key not in state.streams:
        state.streams[stream_key] = []
    stream = state.streams[stream_key]
    try:
        milliseconds, seq = generate_entry_id(entry_id, stream)
        stream.append([milliseconds, seq, {}])
        entry = stream[-1][2]
        n = len(parts)
        for i in range(0, n, 2):
            key_entry = parts[i]
            val_entry = parts[i + 1]
            entry[key_entry] = val_entry
        processed_entry_id = f"{milliseconds}-{seq}"
        response = f"${len(processed_entry_id)}\r\n{processed_entry_id}\r\n".encode()
    except ValueError as e:
        response = e.args[0]
    return response

def XRANGE(args: list[bytes], request_ms: float) -> bytes:
    stream_key, entry_id_1, entry_id_2 = args[:3]
    if stream_key in state.cache or stream_key in state.lists:
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    if entry_id_1 == b"-":
        milli_1 = 0
        seq_1 = 0
    elif b"-" in entry_id_1:
        milli_1, _, seq_1 = entry_id_1.partition(b"-")
        milli_1, seq_1 = int(milli_1), int(seq_1)
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
    start_index = bisect.bisect_left(stream, (milli_1, seq_1), key=lambda x: (x[0], x[1]))
    end_index = bisect.bisect_right(stream, (milli_2, seq_2), key=lambda x: (x[0], x[1])) - 1
    response = f"*{end_index - start_index + 1}\r\n".encode()
    for i in range(start_index, end_index + 1):
        entry_id = f"{stream[i][0]}-{stream[i][1]}"
        entry = stream[i][2]
        response += f"*2\r\n${len(entry_id)}\r\n{entry_id}\r\n*{len(entry) * 2}\r\n".encode()
        for key, val in entry.items():
            response += f"${len(key)}\r\n".encode() + key + f"\r\n${len(val)}\r\n".encode() + val + b"\r\n"
    return response
            
def XREAD(args: list[bytes], request_ms: float) -> bytes:
    block_ms = -1 if args[0] == b"streams" else int(args[1])
    start_of_keys = 1 if args[0] == b"streams" else 3
    m = (len(args) - start_of_keys) // 2    
    response = f"*{m}\r\n".encode()
    for i in range(m):
        stream_key = args[start_of_keys + i]
        if stream_key in state.cache or stream_key in state.lists:
            return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"

    for i in range(m):
        stream_key = args[start_of_keys + i]
        entry_id = args[start_of_keys + i + m]
        if stream_key not in state.streams:
            state.streams[stream_key] = []
        stream = state.streams[stream_key]
        if entry_id == b"$":
            if stream:
                milli, seq = stream[-1][0], stream[-1][1]
            else:
                milli, seq = 0, 0
        else:
            milli, _, seq = entry_id.partition(b"-")
            milli, seq = int(milli), int(seq)
        deadline = time.time() * 1000 + block_ms if block_ms != 0 else math.inf
        while True:
            n = len(stream)
            start_index = bisect.bisect_right(stream, (milli, seq), key=lambda x: (x[0], x[1]))
            if block_ms == -1 or start_index != n:
                response += f"*2\r\n${len(stream_key)}\r\n".encode() + stream_key + f"\r\n*{n - start_index}\r\n".encode()
                for i in range(start_index, n):
                    entry_id = f"{stream[i][0]}-{stream[i][1]}"
                    entry = stream[i][2]
                    response += f"*2\r\n${len(entry_id)}\r\n{entry_id}\r\n*{2 * len(entry)}\r\n".encode()
                    for key, val in entry.items():
                        response += f"${len(key)}\r\n".encode() + key + f"\r\n${len(val)}\r\n".encode() + val + b"\r\n"
                break
            elif time.time() * 1000 > deadline:
                return b"*-1\r\n"
            else:
                time.sleep(0.01)
    return response

def INCR(args: list[bytes], request_ms: float) -> bytes:
    key = args[0]
    if key in state.streams or key in state.lists:
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    if key in state.cache:
        try:
            state.cache[key][0] = str(int(state.cache[key][0]) + 1).encode()
        except:
            return b"-ERR value is not an integer or out of range\r\n"
    else:
        state.cache[key] = [b"1", None]
    return b":" + state.cache[key][0] + b"\r\n"

def MULTI(args: list[bytes], request_ms: float) -> bytes:
    if thread_state.in_multi:
        return b"-ERR MULTI calls can not be nested"
    thread_state.in_multi = True
    return b"+OK\r\n"

def EXEC(args: list[bytes], request_ms: float) -> bytes:
    response = b"-ERR EXEC without MULTI\r\n"
    if thread_state.in_multi:
        thread_state.in_multi = False
        for watched_key, val in thread_state.watched_keys.items():
            if val is not None:
                for che in [state.cache, state.lists, state.streams]:
                    if watched_key in che:
                        if val != che[watched_key]:
                            response = b"*-1\r\n"
                        break
                else:
                    response = b"*-1\r\n"
                if response == b"*-1\r\n": break
            else:
                if any(watched_key in che for che in [state.cache, state.lists, state.streams]):
                    response = b"*-1\r\n"
                    break
        else:
            response = f"*{len(thread_state.command_queue)}\r\n".encode()
            for command_args, command_request_ms in thread_state.command_queue:
                response += build_response(command_args, command_request_ms)
    thread_state.command_queue = []
    thread_state.watched_keys = {}
    return response

def DISCARD(args: list[bytes], request_ms: float) -> bytes:
    if not thread_state.in_multi:
        return b"-ERR DISCARD without MULTI\r\n"
    else:
        thread_state.in_multi = False
        thread_state.command_queue = []
        thread_state.watched_keys = {}

        return b"+OK\r\n"

def WATCH(args: list[bytes], request_ms: float) -> bytes:
    if thread_state.in_multi:
        return b"-ERR WATCH inside MULTI is not allowed\r\n"
    else:
        for key in args:
            if key in state.cache:
                thread_state.watched_keys[key] = state.cache[key]
            elif key in state.streams:
                thread_state.watched_keys[key] = state.streams[key]
            elif key in state.lists:
                thread_state.watched_keys[key] = state.lists[key]
            else:
                thread_state.watched_keys[key] = None


        return b"+OK\r\n"
        
def UNWATCH(args: list[bytes], request_ms: float) -> bytes:
    thread_state.watched_keys = {}
    return b"+OK\r\n"

def INFO(args: list[bytes], request_ms: float) -> bytes:
    role = "slave" if state.replica else "master"
    infos = [f"role:{role}", f"master_replid:{state.replication_id}", f"master_repl_offset:{state.replication_offset}"]
    if len(args) > 0 and args[0] == b"replication":
        info = "\r\n".join(infos)
    else:
        info = infos[0]
    return f"${len(info)}\r\n{info}\r\n".encode()

def REPLCONF(args: list[bytes], request_ms: float) -> bytes:
    if args[0] == b"GETACK":
        return f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(state.offset))}\r\n{state.offset}\r\n".encode()
    if args[0] == b"ACK":
        slave_offset = int(args[1])
        state.slaves_offset[thread_state.slave_conn] = slave_offset    
        return b""    
    slave_conn = thread_state.slave_conn
    index = -1
    for t_slave, _, _ in state.to_become_slave:
        index += 1
        if t_slave is slave_conn:
            break
    else:
        index = len(state.to_become_slave)
        state.to_become_slave.append([slave_conn, None, None])

    if args[0] == b"listening-port":
        state.to_become_slave[index][1] = int(args[1])
    elif args[0] == b"capa":
        state.to_become_slave[index][2] = args[1]
    return b"+OK\r\n"

def PSYNC(args: list[bytes], request_ms: float) -> bytes:
    replication_id = args[0]
    replication_offset = args[1]
    empty_body = bytes.fromhex("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
    thread_state.type = 'slave'
    thread_state.slave_conn.sendall(b"+FULLRESYNC " + state.replication_id.encode() + b" 0\r\n" + f"${len(empty_body)}\r\n".encode() + empty_body)
    while state.to_become_slave:
        state.slaves.append(state.to_become_slave.popleft())
    return b""

def WAIT(args: list[bytes], request_ms: float) -> bytes:
    num_replicas = int(args[0])
    time_to_wait = int(args[1])
    if state.sent_bytes == 0:
        return f":{len(state.slaves)}\r\n".encode()
    getack_command = b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
    for slave_conn, _, _ in state.slaves:
        state.slave_commands.append((slave_conn, getack_command))
    deadline = int(time.time() * 1000) + time_to_wait
    while True:
        num = 0
        for slave, _, _ in state.slaves:
            if slave in state.slaves_offset and state.slaves_offset[slave] >= state.sent_bytes:
                num += 1
                if num >= num_replicas: break
        else:
            if int(time.time() * 1000) <= deadline: time.sleep(0.01); continue
        break
    state.sent_bytes += len(getack_command)
    return f":{num}\r\n".encode()

def CONFIG(args: list[bytes], request_ms: float) -> bytes:
    if args[0] == b'GET':
        argument = {
            b'dir': state.args.dir,
            b'dbfilename': state.args.dbfilename,
            b'appendonly': state.args.appendonly,
            b'appenddirname': state.args.appenddirname,
            b'appendfilename': state.args.appendfilename,
            b'appendfsync': state.args.appendfsync
            }.get(args[1])
        if argument is None:
            return b"-ERR No Such Argument Exists\r\n"
        return f"*2\r\n${len(args[1])}\r\n".encode() + args[1] + f"\r\n${len(argument)}\r\n{argument}\r\n".encode()
    return b"-ERR No Such Command Exists\r\n"

def read_bytes(file, first_byte):
    first_byte = int.from_bytes(first_byte)
    if first_byte & 192 == 0: # 1100 0000
        bytes_to_read = first_byte & 63 # 0011 1111
    elif first_byte & 192 == 64:
        bytes_to_read = (first_byte & 63) * 256 + int.from_bytes(file.read(1))
    elif first_byte & 192 == 128:
        bytes_to_read = int.from_bytes(file.read(4), byteorder='big')
    else:
        r6 = first_byte & 63
        if r6 <= 2:
            bytes_to_read = 1 << r6
        else:
            return b"-ERR Unsupported Number (6-bit) For 0x11\r\n"
    return file.read(bytes_to_read)

def process_RDB():
    if state.args.dbfilename is None: return
    _dir = state.args.dir
    dbfilename = state.args.dbfilename
    request_ms = time.time() * 1000
    path = os.path.join(_dir, dbfilename)
    if not os.path.exists(path): return
    with open(path, 'rb') as file:
        magic_number = file.read(9)
        if magic_number != bytes.fromhex("524544495330303131"): return b"-ERR Invalid Magic Number\r\n"
        file.read(1) # FA
        while True:
            first_byte = file.read(1)
            if first_byte == bytes.fromhex('FE'): break
            read_bytes(file, first_byte)
        file.read(2) # index + FB
        n = int.from_bytes(file.read(1))
        file.read(1) # EXP keys
        keys = []
        for _ in range(n):
            next_byte = file.peek(1)[:1]
            expire_ms = None
            if next_byte == bytes.fromhex('FC'):
                file.read(1) # 1 + 8
                expire_ms = (request_ms, int.from_bytes(file.read(8), byteorder='little') - request_ms)
            elif next_byte == bytes.fromhex('FD'):
                file.read(1) # 1 + 4
                expire_ms = (request_ms, int.from_bytes(file.read(4), byteorder='little') * 1000 - request_ms)
            typ = file.read(1) # type of val
            first_key = file.read(1)
            key = read_bytes(file, first_key)
            if key.startswith(b"-ERR"): return key
            keys.append(key)
            first_val = file.read(1)
            val = read_bytes(file, first_val)
            state.cache[key] = [val, expire_ms]
        state.RDB_KEYS.extend(keys)

def KEYS(args: list[bytes], request_ms: float) -> bytes:
    if state.args.dir is None or state.args.dbfilename is None: return b"*0\r\n"
    path = os.path.join(state.args.dir, state.args.dbfilename)
    if not os.path.exists(path): return b"*0\r\n"
    keys = state.RDB_KEYS
    return f"*{len(keys)}\r\n".encode() + b"\r\n".join(f"${len(key)}\r\n".encode() + key for key in keys) + b"\r\n"

class Conn:
    def __init__(self, binary):
        self.binary = binary

    def recv(self, n, MSG_WAITALL=None):
        if n > len(self.binary):
            if MSG_WAITALL: raise ValueError("Parse Error")
            ret = self.binary
            self.binary = b""
        else:
            ret = self.binary[:n]
            self.binary = self.binary[n:]
        return ret

    def is_empty(self):
        return len(self.binary) == 0

def pre_process():
    if state.args.appendonly == 'yes':
        _dir =  state.args.dir
        append_dir = state.args.appenddirname
        filename = state.args.appendfilename
        path = os.path.join(_dir, append_dir)
        manifest_file = os.path.join(path, filename + ".manifest")
        if os.path.exists(path):
            with open(manifest_file, 'r') as m_file:
                line = m_file.readline()
                append_file = os.path.join(path, line.split(' ')[1])
                with open(append_file, 'rb') as a_file:
                    conn_obj = Conn(a_file.read())
            buf = b""
            while not conn_obj.is_empty() or buf != b"":
                request, buf = parser(conn_obj, buf)
                if request:
                    build_response(request, time.time() * 1000)

        else:
            os.makedirs(path, exist_ok=True)
            append_file = filename + ".1.incr.aof"
            file_path = os.path.join(path, append_file)
            with open(file_path, 'w'):
                pass
            with open(manifest_file, 'w') as file:
                file.write(f"file {append_file} seq 1 type i")

def flush_to_disk(args):
    if state.args.appendonly == 'yes':
        command = build_command(args)
        manifest_file = os.path.join(state.args.dir, state.args.appenddirname, state.args.appendfilename + ".manifest")
        with open(manifest_file, 'r') as m_file:
            line = m_file.readline()
            append_file = os.path.join(state.args.dir, state.args.appenddirname, line.split(' ')[1])
            with open(append_file, 'ab') as a_file:
                a_file.write(command)
                a_file.flush()
                os.fsync(a_file.fileno())

def SUBSCRIBE(args: list[bytes], request_ms: float) -> bytes:
    channel = args[0]
    state.channels[thread_state.slave_conn].add(channel)
    return f"*3\r\n$9\r\nsubscribe\r\n${len(args[0])}\r\n".encode() + args[0] + f"\r\n:{len(state.channels[thread_state.slave_conn])}\r\n".encode()

def PUBLISH(args: list[bytes], request_ms: float) -> bytes:
    connection = args[0]
    message = args[1]
    ans = 0
    for conn, channel in state.channels.items():
        if connection in channel:
            ans += 1
            conn.sendall(f"*3\r\n$7\r\nmessage\r\n${len(connection)}\r\n".encode() + connection + f"\r\n${len(message)}\r\n".encode() + message + b"\r\n")
    return f":{ans}\r\n".encode()

def UNSUBSCRIBE(args: list[bytes], request_ms: float) -> bytes:
    channel = args[0]
    state.channels[thread_state.slave_conn].discard(channel)
    return f"*3\r\n$11\r\nunsubscribe\r\n${len(args[0])}\r\n".encode() + args[0] + f"\r\n:{len(state.channels[thread_state.slave_conn])}\r\n".encode()

class ZSET():
    def __init__(self):
        self.scores = {}
        self.ordered = SortedSet()

    def add(self, value, score):
        ans = 1
        if value in self.scores:
            ans = 0
            self.ordered.discard((self.scores[value], value))
        self.scores[value] = score
        self.ordered.add((score, value))
        return ans

    def __len__(self):
        return len(self.ordered)

    def __contains__(self, item):
        return item in self.scores

    def index(self, value):
        score = self.scores[value]
        return self.ordered.index((score, value))

    def __getitem__(self, i):
        return self.ordered[i]

    def __delitem__(self, value):
        score = self.scores[value]
        self.ordered.remove((score, value))
        del self.scores[value]

    def get_score(self, value):
        return self.scores[value]

def ZADD(args: list[bytes], request_ms: float) -> bytes:
    sorted_set, score, value = args[:3]
    if sorted_set not in state.sorted:
        state.sorted[sorted_set] = ZSET()
    sorted_set = state.sorted[sorted_set]
    score = float(score.decode('utf-8'))
    ans = sorted_set.add(value, score)
    return f":{ans}\r\n".encode()

def ZRANK(args: list[bytes], request_ms: float) -> bytes:
    sorted_set, value = args[:2]
    if sorted_set not in state.sorted: return b"$-1\r\n"
    sorted_set = state.sorted[sorted_set]
    if value not in sorted_set: return b"$-1\r\n"
    return f":{sorted_set.index(value)}\r\n".encode()

def ZRANGE(args: list[bytes], request_ms: float) -> bytes:
    sorted_set, start, end = args[:3]
    start, end = int(start), int(end)
    if (sorted_set not in state.sorted): return b"*0\r\n"
    sorted_set = state.sorted[sorted_set]
    n = len(sorted_set)
    if start >= n: return b"*0\r\n"
    def helper(x):
            if x >= n: return n - 1
            if x >= 0: return x
            if -x > n: return 0
            return x + n
    start, end = helper(start), helper(end)
    if start > end: return b"*0\r\n"
    size = end - start + 1
    response = f"*{size}\r\n".encode()
    for i in range(start, end + 1):
        _, value = sorted_set[i]
        response += f"${len(value)}\r\n".encode() + value + b"\r\n"
    return response

def ZCARD(args: list[bytes], request_ms: float) -> bytes:
    sorted_set = args[0]
    if sorted_set not in state.sorted: return b":0\r\n"
    sorted_set = state.sorted[sorted_set]
    return f":{len(sorted_set)}\r\n".encode()

def ZSCORE(args: list[bytes], request_ms: float) -> bytes:
    sorted_set, value = args[:2]
    if sorted_set not in state.sorted: return b"$-1\r\n"
    sorted_set = state.sorted[sorted_set]
    if value not in sorted_set: return b"$-1\r\n"
    num = sorted_set.get_score(value)
    return f"${len(str(num))}\r\n{num}\r\n".encode()

def ZREM(args: list[bytes], request_ms: float) -> bytes:
    sorted_set, value = args[:2]
    if sorted_set not in state.sorted: return b":0\r\n"
    sorted_set = state.sorted[sorted_set]
    if value not in sorted_set: return b":0\r\n"
    del sorted_set[value]
    return b":1\r\n"

def SETBIT(args: list[bytes], request_ms: float) -> bytes:
    key, offset, bit = args[0], int(args[1]), args[2]
    if key in state.lists or key in state.streams or key in state.sorted:
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    if key not in state.cache:
        state.cache[key] = [0, None]
    pos = (offset // 8) * 8 + 7 - (offset % 8)
    old_bit = (state.cache[key][0]) & (1 << pos)
    response = f":{0 if old_bit == 0 else 1}\r\n".encode()
    if bit == b"1":
        state.cache[key][0] |= (1 << pos)
    else:
        state.cache[key][0] &= ~(1 << pos)
    return response

def GETBIT(args: list[bytes], request_ms: float) -> bytes:
    key, offset = args[0], int(args[1])
    if key in state.lists or key in state.streams or key in state.sorted:
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    if key not in state.cache: return b":0\r\n"
    pos = (offset // 8) * 8 + 7 - (offset % 8)
    val, _ = state.cache[key]
    val = val if isinstance(val, int) else int.from_bytes(val, byteorder='little')
    old_bit = (val) & (1 << pos)
    return f":{0 if old_bit == 0 else 1}\r\n".encode()

def STRLEN(args: list[bytes], request_ms: float) -> bytes:
    key = args[0]
    if key in state.lists or key in state.streams or key in state.sorted:
        return b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    if key not in state.cache: return b":0\r\n"
    offset = state.cache[key][0].bit_length() - 1
    return f":{offset // 8 + 1}\r\n".encode()
