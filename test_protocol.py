from threading import Thread
from protocol import Protocol, ProtocolError
from utils import tcp_listen, tcp_connect
from loguru import logger as log


def client_thread():
    sock_client = tcp_connect('127.0.0.1', 10000, blocking=False)
    while client_thread_flag:
        try:
            cmd, data = Protocol.receive_request(sock_client, timeout=1)
        except ProtocolError:
            continue
        log.info('[CLIENT] %s : %s' % (cmd, data))
        if cmd == Protocol.Command.IDENTIFY_CONNECTION and len(data) == 32:
            log.debug('[CLIENT] %s : %s' % (cmd, client_uuid + '*' + conn_uuid))
            Protocol.response_server(sock_client, cmd, client_uuid + '*' + conn_uuid)
        elif cmd == Protocol.Command.QUERY_MAPPING_PORTS and len(data) == 0:
            log.debug('[CLIENT] %s : %s' % (cmd, mapping_ports))
            Protocol.response_server(sock_client, cmd, mapping_ports)
        elif cmd == Protocol.Command.CONFIRM_MAPPING_PORTS and data == mapping_ports:
            log.debug('[CLIENT] %s : %s' % (cmd, Protocol.Result.SUCCESS))
            Protocol.response_server(sock_client, cmd, Protocol.Result.SUCCESS)
        elif cmd == Protocol.Command.ADD_NEW_CONNECTION:
            log.debug('[CLIENT] %s : %s' % (cmd, conn_uuid))
            Protocol.response_server(sock_client, cmd, conn_uuid)
        elif cmd == Protocol.Command.CONFIRM_CONNECTION and data == conn_uuid:
            log.debug('[CLIENT] %s : %s' % (cmd, Protocol.Result.SUCCESS))
            Protocol.response_server(sock_client, cmd, Protocol.Result.SUCCESS)
        else:
            log.error('[CLIENT] Error -- %s : %s' % (cmd, data))
    sock_client.close()
    log.info('[CLIENT] exited.')


def server_thread():
    log.warning('----------------------------------------------------------------------------------------')
    log.debug('[SERVER] %s : %s' % (Protocol.Command.IDENTIFY_CONNECTION, server_uuid))
    _cmd, _data = Protocol.request_client(sock_server, Protocol.Command.IDENTIFY_CONNECTION, server_uuid)
    log.info('[SERVER] %s : %s' % (_cmd, _data))
    log.warning('----------------------------------------------------------------------------------------')
    log.debug('[SERVER] %s : %s' % (Protocol.Command.QUERY_MAPPING_PORTS, ''))
    _cmd, _data = Protocol.request_client(sock_server, Protocol.Command.QUERY_MAPPING_PORTS, '')
    log.info('[SERVER] %s : %s' % (_cmd, _data))
    log.warning('')
    log.debug('[SERVER] %s : %s' % (Protocol.Command.CONFIRM_MAPPING_PORTS, _data))
    _cmd, _data = Protocol.request_client(sock_server, Protocol.Command.CONFIRM_MAPPING_PORTS, _data)
    log.info('[SERVER] %s : %s' % (_cmd, _data))
    log.warning('----------------------------------------------------------------------------------------')
    log.debug('[SERVER] %s : %s' % (Protocol.Command.ADD_NEW_CONNECTION, 80))
    _cmd, _data = Protocol.request_client(sock_server, Protocol.Command.ADD_NEW_CONNECTION, str(80))
    log.info('[SERVER] %s : %s' % (_cmd, _data))
    log.warning('')
    log.debug('[SERVER] %s : %s' % (Protocol.Command.CONFIRM_CONNECTION, _data))
    _cmd, _data = Protocol.request_client(sock_server, Protocol.Command.CONFIRM_CONNECTION, _data)
    log.info('[SERVER] %s : %s' % (_cmd, _data))
    log.warning('----------------------------------------------------------------------------------------')


if __name__ == '__main__':

    server_uuid = '____SERVER__UUID__(32__bits)____'
    client_uuid = '____CLIENT__UUID__(32__bits)____'
    conn_uuid = '____SOCKET__UUID__(32__bits)____'
    mapping_ports = '{80: 10080, 81: 10081}'

    client_handler = Thread(None, client_thread)
    server_handler = Thread(None, server_thread)
    client_thread_flag = True

    sock = tcp_listen('', 10000, blocking=True)
    client_handler.start()
    sock_server, address = sock.accept()
    sock_server.setblocking(False)
    sock.close()

    server_handler.start()
    server_handler.join()
    client_thread_flag = False
    client_handler.join()
    sock_server.close()
    log.info('[SERVER] exited.')
