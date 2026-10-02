import socket  # noqa: F401
import threading

cache = {}


def build_response(requests: list[bytearray]):
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
            cache[key] = value
            response += b"+OK\r\n"
            i += 3
        elif requests[i] == b"GET":
            key = bytes(requests[i + 1])
            if key in cache:
                value = cache[key]
                response += f"${len(value)}\r\n".encode() + value + b"\r\n"
            else:
                response += b"$-1\r\n"
            i += 2
    return response
    

def handle_client(conn):
    buf = bytearray()
    requests = []
    while True:
        chunk: bytes = conn.recv(1024)
        if not chunk:
            return
        buf += chunk
        currrent_requests = [current_request for current_request in buf.split(b"\r\n") if not current_request.startswith((b"*", b"$"))]
        buf = currrent_requests.pop()
        requests += currrent_requests
        n = len(requests)
        i = 0
        while i < n:
            if requests[i] == b'PING':
                i += 1
            elif requests[i] in (b'ECHO', b'GET'):
                if i + 1 < n:
                    i += 2
                else:
                    break
            elif requests[i] == b'SET':
                if i + 2 < n:
                    i += 3
                else:
                    break
        response = build_response(requests[:i])
        conn.sendall(response)
        requests = requests[i:]


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
