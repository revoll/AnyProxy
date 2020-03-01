#!/usr/bin/env python3
from time import sleep
from proxy_server import ProxyServer
from proxy_client import ProxyClient
from loguru import logger as log


SERVER_HOST = 'wangkui.tech'
SERVER_PORT = 10000
PORT_MAPPING_TABLE = {80: 10080, 81: 10081, 8080: 18080}
TEST_CLIENT = 1
TEST_SERVER = 2

TEST_MODE = TEST_CLIENT
# TEST_MODE = TEST_SERVER
# TEST_MODE = TEST_CLIENT | TEST_SERVER


if __name__ == '__main__':
    server = ProxyServer(SERVER_HOST, SERVER_PORT)
    client = ProxyClient(SERVER_HOST, SERVER_PORT, PORT_MAPPING_TABLE)
    if TEST_MODE & TEST_SERVER:
        server.start()
    if TEST_MODE & TEST_CLIENT:
        client.start()
    sleep(1)
    log.debug('='*80)
    log.debug('%s SERVER and CLIENT are started !!!' % (' '*20))
    log.debug('='*80)
    try:
        while TEST_MODE & TEST_SERVER and server.is_running():
            sleep(0.25)
        while TEST_MODE & TEST_CLIENT and client.is_running():
            sleep(0.25)
    except KeyboardInterrupt:
        client.stop()
        server.stop()
