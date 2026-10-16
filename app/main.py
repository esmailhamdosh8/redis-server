import socket  # noqa: F401
import threading
import sys
import time
from .glb import *
from .commands import *


def build_response(parts: list[bytearray], request_ms):
    parts = [bytes(part) for part in parts]
    command, *args = parts
    exceptions_commands_to_MULTI = (b'EXEC', b'DISCARD', b'WATCH', b'UNWATCH', b'MULTI')
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
                    request_ms = time.time() * 1000
                else:
                    if find_len:
                        if not parts[i].startswith(b"$"): raise ValueError
                        len_of_arg = int(parts[i][1:])
                    else:
                        if len(parts[i]) != len_of_arg: raise ValueError
                        current_request.append(parts[i])
                        cnt_of_args -= 1
                    find_len = not find_len
            if (not parts[-1] or parts[-1].startswith(b"*")) and cnt_of_args == 0:
                if current_request:
                    response = build_response(current_request, request_ms)
                    conn.sendall(response)
                current_request = []
            buf = parts[-1]
    finally:
        conn.close()


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    
    # Uncomment the code below to pass the first stage
    #
    args = sys.argv
    n = len(args)
    port = 6379
    for i in range(n):
        if args[i] == '--port' and i + 1 < n:
            try:
                port = int(args[i + 1])
            except:
                pass
        elif args[i] == '--replicaof':
            state.replica = True
            master_host, _, master_port = args[i + 1].partition(" ")
            master_port = int(master_port)
            state.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            state.client_socket.connect((master_host, master_port))
            state.client_socket.sendall(b"*1\r\n$4\r\nPING\r\n")
            if b"+PONG\r\n" in state.client_socket.recv(1024):
                state.client_socket.sendall(f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{port}\r\n".encode())
                if b"+OK\r\n" in state.client_socket.recv(1024):
                    state.client_socket.sendall(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
                    if b"+OK\r\n" in state.client_socket.recv(1024):
                        state.client_socket.sendall(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")


    server_socket = socket.create_server(('localhost', port), reuse_port=True)
    while True:
        conn, addr = server_socket.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()
