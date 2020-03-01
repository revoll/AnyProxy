from proxy_client import ProxyClient
import sys


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print('\nError: Invalid parameters!!!')
        print('Usage: python3 proxy_client.py <host> <port> <port_map>')
        exit()
    __server_host = sys.argv[1]
    __server_port = int(sys.argv[2])
    __port_map = eval(sys.argv[3])
    __proxy_client = ProxyClient(__server_host, __server_port, __port_map)
    __proxy_client.start()
    try:
        __proxy_client.wait_exit()
    except KeyboardInterrupt:
        pass
    finally:
        __proxy_client.stop()
