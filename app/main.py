import socket  # noqa: F401
import threading
import sys
import time
import argparse
from .glb import *
from .commands import *


def write_to_slaves():
    while True:
        if state.slave_commands:
            slave_conn, response = state.slave_commands.popleft()
            slave_conn.sendall(response)
        time.sleep(0.01)

def parser(conn, buf: bytes) -> tuple[list[bytes], bytes]:
    is_byte_digit = lambda num: num >= 48 and num <= 57
    request = []
    star = 0
    chunk = buf
    while True:
        i = 0
        n = len(chunk)
        while i < n:
            if chunk[i] == 42:
                i += 1
            elif is_byte_digit(chunk[i]):
                star = star * 10 + chunk[i] - 48
                i += 1
            else:
                if len(chunk) - 1 - i > 1:
                    chunk = chunk[i + 2:]
                else:
                    if len(chunk) - 1 == i:
                        conn.recv(1, socket.MSG_WAITALL)
                    chunk = conn.recv(1024)
                break
        else:
            chunk = conn.recv(1024)
            continue
        break
    for _ in range(star):
        amp = 0
        while True:
            i = 0
            n = len(chunk)
            while i < n:
                if chunk[i] == 36:
                    i += 1
                elif is_byte_digit(chunk[i]):
                    amp = amp * 10 + chunk[i] - 48
                    i += 1
                else:
                    if len(chunk) - 1 - i > 1:
                        chunk = chunk[i + 2:]
                    else:
                        if len(chunk) - 1 == i:
                            conn.recv(1, socket.MSG_WAITALL)
                        chunk = conn.recv(1024)
                    break
            else:
                chunk = conn.recv(1024)
                continue
            break
        while len(chunk) < amp + 2:
            chunk += conn.recv(1024)
        request.append(chunk[:amp])
        chunk = chunk[amp + 2:]
    return request, chunk

def handle_client(conn, respond_type):
    thread_state.in_multi = False
    thread_state.command_queue = []
    thread_state.watched_keys = {}
    thread_state.slave_conn = conn
    thread_state.respond_to = respond_type
    try:
        buf = b""
        while True:
            request, buf = parser(conn, buf)
            if request:
                response = build_response(request, time.time() * 1000)
                if thread_state.respond_to == 'master':
                    command = build_command(request)
                    state.offset += len(command)
                
                if thread_state.respond_to == 'client':
                    conn.sendall(response)
                elif thread_state.respond_to == 'master' and request[:2] == [b"REPLCONF", b"GETACK"]:
                    conn.sendall(response)
                elif thread_state.respond_to == 'slave':
                    state.slave_commands.append((conn, response))
    finally:
        conn.close()

def set_up_communication_with_master():
    replicaof = state.args.replicaof
    port = state.args.port
    if replicaof:
        state.replica = True
        master_host, _, master_port = replicaof.partition(' ')
        master_port = int(master_port)
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((master_host, master_port))
        client_socket.sendall(b"*1\r\n$4\r\nPING\r\n")
        if b"+PONG\r\n" in client_socket.recv(7, socket.MSG_WAITALL):
            client_socket.sendall(f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{port}\r\n".encode())
            if b"+OK\r\n" in client_socket.recv(5, socket.MSG_WAITALL):
                client_socket.sendall(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
                if b"+OK\r\n" in client_socket.recv(5, socket.MSG_WAITALL):
                    client_socket.sendall(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
                    body = client_socket.recv(149, socket.MSG_WAITALL)
                    threading.Thread(target=handle_client, args=(client_socket, 'master'), daemon=True).start()

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    
    # Uncomment the code below to pass the first stage
    #

    threading.Thread(target=write_to_slaves, daemon=True).start()

    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=6379)
    parser.add_argument('--replicaof', type=str)
    parser.add_argument('--dir', type=str, default=os.getcwd())
    parser.add_argument('--dbfilename', type=str)
    parser.add_argument('--appendonly', type=str, default='no')
    parser.add_argument('--appenddirname', type=str, default='appendonlydir')
    parser.add_argument('--appendfilename', type=str, default='appendonly.aof')
    parser.add_argument('--appendfsync', type=str, default='everysec')

    state.args = parser.parse_args()
    thread_state.in_multi = False
    process_RDB()
    pre_process()
    set_up_communication_with_master()


    server_socket = socket.create_server(('localhost', state.args.port), reuse_port=True)
    while True:
        conn, _ = server_socket.accept()
        threading.Thread(target=handle_client, args=(conn, 'client'), daemon=True).start()


if __name__ == "__main__":
    main()
