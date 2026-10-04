import socket  # noqa: F401
import threading
import time

cache = {}
lists = {}


def build_response(parts: list[bytearray], request_ms):
    response = bytearray()
    command, *args = parts

    if command == b'PING':
        response += b"+PONG\r\n"
    elif command == b"ECHO":
        response += f"${len(args[0])}\r\n".encode() + args[0] + b"\r\n"
    elif command == b"SET":
        key, value = bytes(args[0]), args[1]
        if len(args) > 2:
            duration_ms = int(args[3]) if args[2] == b"PX" else 1000 * args[3]
            cache[key] = (value, request_ms, duration_ms)
        else:
            cache[key] = (value, -1, -1)
        response += b"+OK\r\n"
    elif command == b"GET":
        key = bytes(args[0])
        if key not in cache:
            response += b"$-1\r\n"
        else:
            value, saved_ms, duration_ms = cache[key]
            if saved_ms == -1 or duration_ms == -1 or request_ms - saved_ms <= duration_ms:
                response += f"${len(value)}\r\n".encode() + value + b"\r\n"
            else:
                del cache[key]
                response += b"$-1\r\n"
    elif command == b"RPUSH":
        key = bytes(args[0])
        if key not in lists:
            lists[key] = []
        for value in args[1:]:
            lists[key].append(value)
        response += f":{len(lists[key])}\r\n".encode()
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
