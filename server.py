from proxy_server import ProxyServer


if __name__ == '__main__':
    import sys
    if len(sys.argv) != 3:
        print('\nError: Invalid parameters!!!')
        print('Usage: python3 proxy_server.py <host> <port>')
        exit()
    __host = sys.argv[1]
    __port = sys.argv[2]
    __proxy_client = ProxyServer(__host, __port)
    __proxy_client.start()
    try:
        __proxy_client.wait_exit()
    except KeyboardInterrupt:
        pass
    finally:
        __proxy_client.stop()
