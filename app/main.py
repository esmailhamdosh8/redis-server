import socket  # noqa: F401
import threading
import time
from collections import deque
from .glb import state
from .commands import *

def build_response(parts: list[bytearray], request_ms):
    command, *args = parts
    return globals()[command.decode()](args, request_ms)
    

def handle_client(conn):
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
                    if parts[i].startswith(b"$"):
                        len_of_arg = int(parts[i][1:])
                    else:
                        if len(parts[i]) != len_of_arg: raise ValueError
                        current_request.append(parts[i])
                        cnt_of_args -= 1
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
    except:
        pass
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
