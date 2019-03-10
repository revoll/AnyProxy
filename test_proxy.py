#!/usr/bin/env python3
from time import sleep
from threading import Thread
from server_proxy import ServerProxy
from client_proxy import ClientProxy
from loguru import logger as log


SERVER_HOST = '127.0.0.1'
SERVER_PORT = 10000
PORT_MAPPING_TABLE = {80: 10080, 81: 10081}


if __name__ == '__main__':
    server_proxy = ServerProxy(PORT_MAPPING_TABLE, SERVER_HOST, SERVER_PORT)
    client_proxy = ClientProxy(SERVER_HOST, SERVER_PORT)
    server_handler = Thread(None, server_proxy.start, 'ServerProxy-Start()', ())
    client_handler = Thread(None, client_proxy.start, 'ClientProxy-Start()', ())
    server_handler.start()
    client_handler.start()
    server_handler.join()
    client_handler.join()
    log.debug('='*80)
    log.debug('%s SERVER and CLIENT are started !!!' % (' '*20))
    log.debug('='*80)
    try:
        while server_proxy.is_healthy() and client_proxy.is_healthy():
            sleep(1)
    except KeyboardInterrupt:
        pass
    server_proxy.stop()
    client_proxy.stop()
