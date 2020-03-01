import sys
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


def check_udp_port_available(port):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(('127.0.0.1', port))
    except socket.error:
        return False
    else:
        sock.close()
        return True


def check_python_version(main, sub):
    if sys.version_info < (main, sub):
        raise RuntimeError(f'"Python {main}.{sub}" or higher version is required.')
    return True


def stringify_bytes_val(val):
    unit = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    level = 0
    positive = True
    if val < 0:
        val = - val
        positive = False
    while level < len(unit) - 1:
        if val < 1000:
            break
        else:
            level += 1
            val /= 1000
    return f'{"" if positive else "-"}{str(val)[:5]} {unit[level]}'


def stringify_speed_val(val):
    return stringify_bytes_val(val) + '/s'


def validate_id_string(id_str, length):
    try:
        if not id_str or len(id_str) != length:
            raise ValueError
        int(id_str, 16)  # check if `id_str` is a hex format string or not.
        return True
    except ValueError:
        return False


def validate_ip_address(ip_str):
    try:
        values = ip_str.split('.')
        if len(values) != 4:
            raise ValueError
        for v in values:
            int(v)
        return True
    except ValueError:
        return False
