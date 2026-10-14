import socket  # noqa: F401
import threading
import time
from collections import deque
from .glb import *
from .commands import *


def build_response(parts: list[bytearray], request_ms):
    command, *args = parts
    exceptions_commands_to_MULTI = (b'EXEC', b'DISCARD', b'WATCH', b'UNWATCH')
    if thread_state.queue and command not in exceptions_commands_to_MULTI:
        thread_state.command_queue.append((command, args, request_ms))
        return b"+QUEUED\r\n"
    else:
        return globals()[command.decode()](args, request_ms)
    

def handle_client(conn):
    thread_state.queue = False
    thread_state.command_queue = []
    thread_state.watched_keys = {}
    buf = bytearray()
    current_request = []
    request_ms = None
    cnt_of_args = 0
    try:
        while True:
            chunk = conn.recv(1024)
            if not chunk: 
                if not current_request: return
                raise ValueError
            buf += chunk
            parts = buf.split(b"\r\n")
            n = len(parts)
            find_len = True
            for i in range(n - 1):
                if cnt_of_args == 0:
                    if not parts[i].startswith(b"*"): raise ValueError
                    if current_request:
                        response = build_response(current_request, request_ms)
                        conn.sendall(response)
                    current_request = []
                    cnt_of_args = int(parts[i][1:])
                    request_ms = int(time.time() * 1000)
                else:
                    if find_len:
                        if not parts[i].startswith(b"$"): raise ValueError
                        len_of_arg = int(parts[i][1:])
                    else:
                        if len(parts[i]) != len_of_arg: raise ValueError
                        current_request.append(parts[i])
                        cnt_of_args -= 1
                    find_len = not find_len
            if not parts[-1] and cnt_of_args == 0:
                if current_request:
                    response = build_response(current_request, request_ms)
                    conn.sendall(response)
                current_request = []
                buf = b""
            elif parts[-1].startswith(b"*") and cnt_of_args == 0:
                if current_request:
                    response = build_response(current_request, request_ms)
                    conn.sendall(response)
                current_request = []
                buf = parts[-1]
            else:
                buf = parts[-1]
    finally:
        conn.close()


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    
    # Uncomment the code below to pass the first stage
    #
    server_socket = socket.create_server(('localhost', 6379), reuse_port=True)
    while True:
        conn, addr = server_socket.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()
