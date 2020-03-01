import socket


def check_listening(host, port):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
    except socket.error:
        return False
    else:
        sock.close()
        return True


def tcp_connect(host, port, blocking=True):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    sock.setblocking(blocking)
    return sock


def tcp_listen(host, port, blocking=True, max_conn=1000):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen(max_conn)
    sock.setblocking(blocking)
    return sock
