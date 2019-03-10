#!/usr/bin/env python3
import sys
from time import sleep
from client_proxy import ClientProxy
from loguru import logger as log


class Client:
    """
    内网穿透工具客户端程序（主动连接服务端进行端口映射）
    """

    class Config:
        SERVER_HOST = '127.0.0.1'
        SERVER_PORT = 10000

    def __init__(self, host=Config.SERVER_HOST, port=Config.SERVER_PORT):
        self.proxy = ClientProxy(host, port)

    def start(self):
        self.proxy.start()
        log.info('='*80)
        log.info('  Client Started Successfully !!!')
        log.info('='*80)
        try:
            while self.proxy.is_healthy():
                sleep(1)
            raise OSError('Proxy module is not healthy!')
        except OSError as e:
            log.critical(e)
        except KeyboardInterrupt as e:
            log.warning(e)
        finally:
            self.proxy.stop()
            exit(0)


if __name__ == '__main__':
    _host = Client.Config.SERVER_HOST
    _port = Client.Config.SERVER_PORT
    if len(sys.argv) == 1:
        log.warning('Running with default settings: server=%s:%d' % (_host, _port))
    elif len(sys.argv) == 2:
        _host = sys.argv[1]
    elif len(sys.argv) == 3:
        _host = sys.argv[1]
        _port = int(sys.argv[2])
    else:
        log.error('Invalid argument list.')
        log.error('Usage: python3 client.py <host> [<port>]')
        exit()
    Client(_host, _port).start()
