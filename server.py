#!/usr/bin/env python3
import sys
from time import sleep
from server_proxy import ServerProxy
from loguru import logger as log


class Server:
    """
    内网穿透工具服务端程序（接受客户端程序的连接，并在服务端开通端口映射）
    """

    class Config:
        SERVER_HOST = '0.0.0.0'
        SERVER_PORT = 10000
        PORT_MAPPING_TABLE = {80: 10080}

    def __init__(self, port_map=Config.PORT_MAPPING_TABLE,
                 host=Config.SERVER_HOST, port=Config.SERVER_PORT):
        self.proxy = ServerProxy(port_map, host, port)

    def start(self):
        self.proxy.start()
        log.info('='*80)
        log.info('  Server Started Successfully !!!')
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
    _host = Server.Config.SERVER_HOST
    _port = Server.Config.SERVER_PORT
    _map = Server.Config.PORT_MAPPING_TABLE
    if len(sys.argv) == 1:
        log.warning('Running with default settings: bind=%s:%d, map=%s' % (_host, _port, _map))
    elif len(sys.argv) == 2:
        _map = eval(sys.argv[1])
    elif len(sys.argv) == 4:
        _host = sys.argv[1]
        _port = int(sys.argv[2])
        _map = eval(sys.argv[3])
    else:
        log.error('Invalid argument list.')
        log.error('Usage: python3 server.py [<host> <port>] <port_map_table>')
        exit()
    Server(_map, _host, _port).start()
