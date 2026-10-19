empty_body = bytes.fromhex("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")

a = b"+FULLRESYNC " + b"8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"+ b" 0\r\n" + f"${len(empty_body)}\r\n".encode() + empty_body

print(len(b"+PONG\r\n"))

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
                    break
        else: