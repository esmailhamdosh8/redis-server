import socket  # noqa: F401
import threading


def handle_client(conn):
    while True:
        chunk = conn.recv(1024)
        if not chunk:
            return
        requests = chunk.split(b"\r\n")
        for request in requests:
            if request == b'PING':
                conn.sendall(b"+PONG\r\n")


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
