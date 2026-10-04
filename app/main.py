import socket  # noqa: F401
import threading
import time

cache = {}
lists = {}


def build_response(requests: list[bytearray], request_ms):
    response = bytearray()
    n = len(requests)
    i = 0
    while i < n:
        if requests[i] == b'PING':
            response += b"+PONG\r\n"
            i += 1
        elif requests[i] == b"ECHO":
            response += f"${len(requests[i + 1])}\r\n".encode() + requests[i + 1] + b"\r\n"
            i += 2
        elif requests[i] == b"SET":
            key, value = bytes(requests[i + 1]), requests[i + 2]
            if i + 3 < n and requests[i + 3] in (b"PX", b"EX"):
                duration_ms = int(requests[i + 4]) if requests[i + 3] == b"PX" else 1000 * requests[i + 4]
                cache[key] = (value, request_ms, duration_ms)
                i += 5
            else:
                cache[key] = (value, -1, -1)
                i += 3
            response += b"+OK\r\n"
        elif requests[i] == b"GET":
            key = bytes(requests[i + 1])
            if key not in cache:
                response += b"$-1\r\n"
            else:
                value, saved_ms, duration_ms = cache[key]
                if saved_ms == -1 or duration_ms == -1 or request_ms - saved_ms <= duration_ms:
                    response += f"${len(value)}\r\n".encode() + value + b"\r\n"
                else:
                    del cache[key]
                    response += b"$-1\r\n"
            i += 2
        elif requests[i] == b"RPUSH":
            key = bytes(requests[i + 1])
            value = requests[i + 2]
            if key not in lists:
                lists[key] = []
            lists[key].append(value)
            response += f":{len(lists[key])}\r\n".encode()
            i += 3
    return response
    

def handle_client(conn):
    request_ms = int(time.time() * 1000)
    buf = bytearray()
    current_request = []
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
                if parts[i].startswith(b"*"):
                    if current_request:
                        response = build_response(current_request, request_ms)
                        conn.sendall(response)
                    request_ms = int(time.time() * 1000)
                    current_request = []
                    cnt_of_args = int(parts[i][1:])
                elif parts[i].startswith(b"$"):
                    len_of_arg = int(parts[i][1:])
                else:
                    if len(parts[i]) != len_of_arg: raise ValueError
                    current_request.append(parts[i])
            if not parts[-1] and len(current_request) == cnt_of_args:
                if current_request:
                    response = build_response(current_request, request_ms)
                    conn.sendall(response)
                current_request = []
                buf = b""
            elif parts[-1].startswith(b"*"):
                if current_request:
                    response = build_response(current_request, request_ms)
                    conn.sendall(response)
                request_ms = int(time.time() * 1000)
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
    while True:
        server_socket = socket.create_server(('localhost', 6379), reuse_port=True)
        conn, addr = server_socket.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()
