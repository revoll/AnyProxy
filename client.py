import sys
import asyncio
import socket
import time
from datetime import datetime
from uuid import uuid4
from protocol import Protocol
from common import ProxyError, RunningStatus
from utils import check_python_version, check_listening
from loguru import logger as log


class TcpMapInfo:
    SERVER_PORT = 'server_port'
    SWITCH = 'is_running'
    STATISTIC = 'statistic'
    CREATE_TIME = 'create_time'


class UdpMapInfo:
    SERVER_PORT = 'server_port'
    SWITCH = 'is_running'
    STATISTIC = 'statistic'
    CREATE_TIME = 'create_time'


class ProxyClient:
    """端口代理转发类（客户端）
    """
    def __init__(self, server_host, server_port=10000, port_map=None, cid=None, name='DefaultClient'):
        for maps in (port_map['TCP'], port_map['UDP']):
            for p in maps:
                if type(p) != int or type(maps[p]) != int or list(maps.values()).count(maps[p]) != 1:
                    raise ValueError('Invalid argument for `port_map`: %s' % port_map)
        server_host = socket.gethostbyname(server_host)
        server_port = int(server_port)

        self.client_id = cid                    # generates by server after connection if not provided.
        self.client_name = name                 # user defined client name.
        self.server_host = server_host          # server side host address.
        self.server_port = server_port          # for both tcp and udp service.

        self.ini_tcp_map = port_map['TCP']      # = {80: 10080, 81: 10081, ...}
        self.ini_udp_map = port_map['UDP']      # = {80: 10080, ...}

        self.tcp_maps = None                    # tcp proxy map
        self.udp_maps = None                    # udp proxy map

        self.__udp_socket = None                # socket for receiving udp packet from server to local udp service.
        self.__udp_req_map = None               # socket for feedback udp packet from local udp service to server.

        self.__protocol = None                  # tcp connection used to communicate with server.
        self.__task_serve_req = None            # task for processing requests form server.
        self.__task_udp_receive = None          # task for receiving udp packet form server.
        self.__task_udp_feedback = None         # task for feedback udp packet to server.
        self.__task_daemon = None               # task for daemon

        self.status = RunningStatus.PREPARE     # PREPARE -> RUNNING (-> PENDING) -> STOPPED
        self.ping = time.time()                 # the period when the last udp ping receives.
        self.timestamp = datetime.utcnow()      # the period when proxy client creates.

    def tcp_statistic(self, upstream=None, downstream=None):
        if upstream is None:
            self.__protocol.tcp_statistic[0] = 0
        else:
            self.__protocol.tcp_statistic[0] += upstream
        if downstream is None:
            self.__protocol.tcp_statistic[1] = 0
        else:
            self.__protocol.tcp_statistic[1] += downstream
        return tuple(self.__protocol.tcp_statistic)

    def udp_statistic(self, upstream=None, downstream=None):
        if upstream is None:
            self.__protocol.udp_statistic[0] = 0
        else:
            self.__protocol.udp_statistic[0] += upstream
        if downstream is None:
            self.__protocol.udp_statistic[1] = 0
        else:
            self.__protocol.udp_statistic[1] += downstream
        return tuple(self.__protocol.udp_statistic)

    def _map_msg(self, client_port, server_port, status, extra=None):
        return f'{ sys._getframe(1).f_code.co_name.upper() }: ' \
               f'Local({client_port}) >>> {self.server_host}({server_port}) ... ' \
               f'[{ status.upper() }]{ " (" + extra + ")" if extra else "" }'

    async def add_tcp_map(self, client_port, server_port):
        if client_port in self.tcp_maps:
            raise ProxyError(self._map_msg(client_port, server_port, 'ERROR', f'already registered.'))
        status, detail = (await self.__protocol.request(
            Protocol.ClientCommand.ADD_TCP_MAP, f'{client_port}{Protocol.PARAM_SEPARATOR}{server_port}',
            Protocol.CONNECTION_TIMEOUT)).split(Protocol.PARAM_SEPARATOR, 1)
        if status != Protocol.Result.SUCCESS:
            raise ProxyError(self._map_msg(client_port, server_port, 'ERROR', detail))
        self.tcp_maps[client_port] = {
            TcpMapInfo.SERVER_PORT: server_port,
            TcpMapInfo.SWITCH: True,
            TcpMapInfo.STATISTIC: [0, 0],
            TcpMapInfo.CREATE_TIME: datetime.utcnow(),
        }
        log.success(self._map_msg(client_port, server_port, 'OK'))

    async def pause_tcp_map(self, client_port):
        if client_port not in self.tcp_maps:
            raise ProxyError(self._map_msg(client_port, None, 'ERROR', f'not registered.'))
        else:
            server_port = self.tcp_maps[client_port][TcpMapInfo.SERVER_PORT]
        status, detail = (await self.__protocol.request(
                Protocol.ClientCommand.PAUSE_TCP_MAP, f'{self.tcp_maps[client_port][TcpMapInfo.SERVER_PORT]}',
                Protocol.CONNECTION_TIMEOUT)).split(Protocol.PARAM_SEPARATOR, 1)
        if status != Protocol.Result.SUCCESS:
            raise ProxyError(self._map_msg(client_port, server_port, 'ERROR'), detail)
        self.tcp_maps[client_port][TcpMapInfo.SWITCH] = False
        log.success(self._map_msg(client_port, server_port, 'OK'))

    async def resume_tcp_map(self, client_port):
        if client_port not in self.tcp_maps:
            raise ProxyError(self._map_msg(client_port, None, 'ERROR', f'not registered.'))
        else:
            server_port = self.tcp_maps[client_port][TcpMapInfo.SERVER_PORT]
        status, detail = (await self.__protocol.request(
                Protocol.ClientCommand.RESUME_TCP_MAP, f'{self.tcp_maps[client_port][TcpMapInfo.SERVER_PORT]}',
                Protocol.CONNECTION_TIMEOUT)).split(Protocol.PARAM_SEPARATOR, 1)
        if status != Protocol.Result.SUCCESS:
            raise ProxyError(self._map_msg(client_port, server_port, 'ERROR'), detail)
        self.tcp_maps[client_port][TcpMapInfo.SWITCH] = True
        log.success(self._map_msg(client_port, server_port, 'OK'))

    async def remove_tcp_map(self, client_port):
        if client_port not in self.tcp_maps:
            raise ProxyError(self._map_msg(client_port, None, 'ERROR', f'not registered.'))
        else:
            server_port = self.tcp_maps[client_port][TcpMapInfo.SERVER_PORT]
        status, detail = (await self.__protocol.request(
                Protocol.ClientCommand.REMOVE_TCP_MAP, f'{self.tcp_maps[client_port][TcpMapInfo.SERVER_PORT]}',
                Protocol.CONNECTION_TIMEOUT)).split(Protocol.PARAM_SEPARATOR, 1)
        if status != Protocol.Result.SUCCESS:
            raise ProxyError(self._map_msg(client_port, server_port, 'ERROR'), detail)
        del self.tcp_maps[client_port]
        log.success(self._map_msg(client_port, server_port, 'OK'))

    async def add_udp_map(self, client_port, server_port):
        if client_port in self.udp_maps:
            raise ProxyError(self._map_msg(client_port, server_port, 'ERROR', f'already registered.'))
        status, detail = (await self.__protocol.request(
                Protocol.ClientCommand.ADD_UDP_MAP, f'{client_port}{Protocol.PARAM_SEPARATOR}{server_port}',
                Protocol.CONNECTION_TIMEOUT)).split(Protocol.PARAM_SEPARATOR, 1)
        if status != Protocol.Result.SUCCESS:
            raise ProxyError(self._map_msg(client_port, server_port, 'ERROR'), detail)
        self.udp_maps[client_port] = {
            UdpMapInfo.SERVER_PORT: server_port,
            UdpMapInfo.SWITCH: True,
            UdpMapInfo.STATISTIC: [0, 0],
            UdpMapInfo.CREATE_TIME: datetime.utcnow(),
        }
        log.success(self._map_msg(client_port, server_port, 'OK'))

    async def pause_udp_map(self, client_port):
        if client_port not in self.udp_maps:
            raise ProxyError(self._map_msg(client_port, None, 'ERROR', f'not registered.'))
        else:
            server_port = self.udp_maps[client_port][UdpMapInfo.SERVER_PORT]
        status, detail = (await self.__protocol.request(
                Protocol.ClientCommand.PAUSE_UDP_MAP, f'{self.udp_maps[client_port][UdpMapInfo.SERVER_PORT]}',
                Protocol.CONNECTION_TIMEOUT)).split(Protocol.PARAM_SEPARATOR, 1)
        if status != Protocol.Result.SUCCESS:
            raise ProxyError(self._map_msg(client_port, server_port, 'ERROR'), detail)
        self.udp_maps[client_port][UdpMapInfo.SWITCH] = False
        log.success(self._map_msg(client_port, server_port, 'OK'))

    async def resume_udp_map(self, client_port):
        if client_port not in self.udp_maps:
            raise ProxyError(self._map_msg(client_port, None, 'ERROR', f'not registered.'))
        else:
            server_port = self.udp_maps[client_port][UdpMapInfo.SERVER_PORT]
        status, detail = (await self.__protocol.request(
                Protocol.ClientCommand.RESUME_UDP_MAP, f'{self.udp_maps[client_port][UdpMapInfo.SERVER_PORT]}',
                Protocol.CONNECTION_TIMEOUT)).split(Protocol.PARAM_SEPARATOR, 1)
        if status != Protocol.Result.SUCCESS:
            raise ProxyError(self._map_msg(client_port, server_port, 'ERROR'), detail)
        self.udp_maps[client_port][UdpMapInfo.SWITCH] = True
        log.success(self._map_msg(client_port, server_port, 'OK'))

    async def remove_udp_map(self, client_port):
        if client_port not in self.udp_maps:
            raise ProxyError(self._map_msg(client_port, None, 'ERROR', f'not registered.'))
        else:
            server_port = self.udp_maps[client_port][UdpMapInfo.SERVER_PORT]
        status, detail = (await self.__protocol.request(
                Protocol.ClientCommand.REMOVE_UDP_MAP, f'{self.udp_maps[client_port][UdpMapInfo.SERVER_PORT]}',
                Protocol.CONNECTION_TIMEOUT)).split(Protocol.PARAM_SEPARATOR, 1)
        if status != Protocol.Result.SUCCESS:
            raise ProxyError(self._map_msg(client_port, server_port, 'ERROR'), detail)
        del self.udp_maps[client_port]
        log.success(self._map_msg(client_port, server_port, 'OK'))

    async def __tcp_data_relay_task(self, client_port, sock_stream, peer_stream):

        async def data_relay_monitor():
            while True:
                if self.status == RunningStatus.RUNNING and client_port in self.tcp_maps \
                        and self.tcp_maps[client_port][TcpMapInfo.SWITCH]:
                    await asyncio.sleep(Protocol.TASK_SCHEDULE_PERIOD)
                else:
                    break

        async def simplex_data_relay(reader, writer, upstream=True):
            while True:
                try:
                    data = await reader.read(Protocol.SOCKET_BUFFER_SIZE)
                    if not data:
                        break
                    writer.write(data)
                    await writer.drain()
                    self.tcp_maps[client_port][TcpMapInfo.STATISTIC][0 if upstream else 1] += len(data)
                except IOError:
                    break

        server_port = self.tcp_maps[client_port][TcpMapInfo.SERVER_PORT]
        rp = peer_stream[1].get_extra_info("sockname")[1]
        log.info(f'Server({server_port}) ----> {self.client_id}({client_port}) [{rp}]')
        _, pending = await asyncio.wait({
            asyncio.create_task(data_relay_monitor()),
            asyncio.create_task(simplex_data_relay(sock_stream[0], peer_stream[1], upstream=True)),
            asyncio.create_task(simplex_data_relay(peer_stream[0], sock_stream[1], upstream=False)),
        }, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        sock_stream[1].close()
        peer_stream[1].close()
        log.info(f'Server({server_port}) --x-> {self.client_id}({client_port}) [{rp}]')

    async def __serve_request_task(self):
        while True:
            uuid, cmd, data = await self.__protocol.get_request(timeout=None)
            result = Protocol.PARAM_SEPARATOR.join((Protocol.Result.SUCCESS, ''))
            try:
                if cmd == Protocol.Command.PING:
                    result += datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

                elif cmd == Protocol.ServerCommand.ADD_TCP_CONNECTION:
                    client_port = int(data)
                    server_port = self.tcp_maps[client_port][TcpMapInfo.SERVER_PORT]
                    reader1, writer1 = await asyncio.open_connection('127.0.0.1', client_port)
                    reader2, writer2 = await asyncio.open_connection(self.server_host, self.server_port)
                    conn_uuid = uuid4().hex
                    identify = Protocol.PARAM_SEPARATOR.join(
                        (Protocol.ConnectionType.PROXY_TCP_DATA, self.client_id, str(server_port), conn_uuid))
                    writer2.write((identify + '\n').encode())
                    await writer2.drain()
                    resp = (await reader2.readline()).decode().strip()
                    if not resp or resp[:len(Protocol.Result.SUCCESS)] != Protocol.Result.SUCCESS:
                        raise ProxyError('Add tcp connection failed while connecting to server.')
                    asyncio.create_task(
                        self.__tcp_data_relay_task(client_port, (reader1, writer1), (reader2, writer2)))
                    result += conn_uuid

                else:
                    log.warning(f'Unrecognised request from Server: {Protocol.make_req(uuid, cmd, data)}')
                    result = Protocol.PARAM_SEPARATOR.join((Protocol.Result.INVALID, 'unrecognised request'))

            except Exception as e:
                log.error(f'Error while processing request({Protocol.make_req(uuid, cmd, data)}): {e}')
                result = Protocol.PARAM_SEPARATOR.join((Protocol.Result.ERROR, str(e)))
            finally:
                try:
                    await self.__protocol.send_response(uuid, cmd, result)
                except Exception as e:
                    log.error(e)
                    break

    async def __udp_receive_task(self):

        def udp_send_data(__b_data, __udp_port, __user_address):
            key = hash((__udp_port, __user_address))
            if key in self.__udp_req_map:
                sock = self.__udp_req_map[key][2]
                self.__udp_req_map[key][3] = time.time()
            else:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.setblocking(False)
                self.__udp_req_map[key] = [__udp_port, user_address, sock, time.time()]
            sock.sendto(__b_data, ('127.0.0.1', __udp_port))

        while True:
            try:
                data, address = self.__udp_socket.recvfrom(Protocol.SOCKET_BUFFER_SIZE)
            except BlockingIOError:
                await asyncio.sleep(Protocol.TASK_SCHEDULE_PERIOD)
                continue
            except IOError as e:
                log.error(e)
                break

            try:
                # if address[0] != self.server_host:
                #    raise ProxyError(f'Received udp packet from {address} which is not the server.')
                try:
                    packet_info = Protocol.unpack_udp_packet(data, unpack_data=False)
                except Exception as e:
                    raise ProxyError(f'Protocol.unpack_udp_packet(): {e}')
                if packet_info[Protocol.UdpPacketInfo.TYPE] == Protocol.UdpPacketType.SYNC:
                    self.udp_statistic(0, len(data))
                    self.__udp_ping = time.time()
                    log.debug(f'UDP_PING: {packet_info[Protocol.UdpPacketInfo.TIMESTAMP]}')
                elif packet_info[Protocol.UdpPacketInfo.TYPE] == Protocol.UdpPacketType.DATA:
                    self.udp_statistic(0, Protocol.UDP_DATA_HEADER_LEN)
                    self.udp_maps[UdpMapInfo.STATISTIC][1] += len(data) - Protocol.UDP_DATA_HEADER_LEN
                    user_address = packet_info[Protocol.UdpPacketInfo.USER_ADDRESS]
                    server_port = packet_info[Protocol.UdpPacketInfo.SERVER_PORT]
                    client_port = None
                    for port in self.udp_maps:
                        if self.udp_maps[port][UdpMapInfo.SERVER_PORT] == server_port:
                            client_port = port
                            break
                    if not client_port:
                        log.warning(f'Received udp data packet on server port({server_port}) that not registered.')
                        continue
                    try:
                        udp_send_data(data[Protocol.UDP_DATA_HEADER_LEN:], client_port, user_address)
                    except IOError as e:
                        log.error(e)
                        continue
                    log.debug(f'Received udp data packet from {user_address} on port({client_port})')
                else:
                    self.udp_statistic(0, len(data))
                    raise ProxyError(f'Received udp packet from {address} with unknown type.')
            except Exception as e:
                log.debug(f'Received udp packet from: {address}, data:\n' + data.hex())
                log.warning(e)
        self.__protocol.close()

    async def __udp_feedback_task(self):
        while True:
            for key in list(self.__udp_req_map):
                client_port, user_address, sock, _ = self.__udp_req_map[key]
                try:
                    while True:
                        try:
                            data = sock.recv(Protocol.SOCKET_BUFFER_SIZE)
                        except BlockingIOError:
                            break
                        data_packet = Protocol.pack_udp_data_packet(
                            self.udp_maps[client_port][UdpMapInfo.SERVER_PORT], user_address, data, pack_data=True)
                        sock.sendto(data_packet, (self.server_host, self.server_port))
                        self.udp_statistic(Protocol.UDP_DATA_HEADER_LEN, 0)
                        self.udp_maps[UdpMapInfo.STATISTIC][0] += len(data)
                        log.debug(f'Sent feedback to server port({self.server_port})')
                except IOError as e:
                    log.error(e)
                    val = self.__udp_req_map.pop(key)
                    val[2].close()
            await asyncio.sleep(Protocol.TASK_SCHEDULE_PERIOD)

    async def __daemon_task(self):
        cnt = 0
        await asyncio.sleep(Protocol.CLIENT_UDP_PING_PERIOD)
        while True:
            #
            # Send tcp keep alive message.
            #
            if not (cnt % Protocol.CLIENT_TCP_PING_PERIOD):
                try:
                    await self.__protocol.request(Protocol.Command.PING, timeout=Protocol.CONNECTION_TIMEOUT)
                except Exception as e:
                    log.error(e)
                    break
            #
            # Send udp keep alive data packet.
            #
            if not (cnt % Protocol.CLIENT_UDP_PING_PERIOD):
                data_packet = Protocol.pack_udp_sync_packet(client_id=self.client_id)
                try:
                    self.__udp_socket.sendto(data_packet, (self.server_host, self.server_port))
                except IOError as e:
                    log.error(e)
                # log.debug(f'Sent UDP ping packet to server({self.server_host}:{self.server_port})')
            #
            #  Check if udp keep alive data packet received in time.
            #
            if time.time() - self.__udp_ping > Protocol.UDP_UNREACHABLE_WARNING_PERIOD:
                log.warning('Too many udp ping packet lost!')
                self.__udp_ping += Protocol.CLIENT_UDP_PING_PERIOD
            #
            # Remove virtual udp connections in `self.__udp_req_map` which is too old.
            #
            for key in list(self.__udp_req_map.keys()):
                if time.time() - self.__udp_req_map[key][3] > Protocol.UDP_REQUEST_DURATION:
                    val = self.__udp_req_map.pop(key)
                    val[2].close()
            #
            # Every loop comes with a rest.
            #
            cnt += 1
            await asyncio.sleep(1)

        self.__protocol.close()  # kill itself.

    async def __main_task(self):
        if self.status == RunningStatus.RUNNING or self.status == RunningStatus.PENDING:
            log.error('Abort starting up the Proxy Client as it is already running.')
            return
        log.info(f'Connecting with the Proxy Server({self.server_host}:{self.server_port}) ...')
        reader, writer = await asyncio.open_connection(self.server_host, self.server_port)
        identify = Protocol.PARAM_SEPARATOR.join(
            (Protocol.ConnectionType.PROXY_CLIENT, self.client_id if self.client_id else "", self.client_name))
        writer.write((identify + '\n').encode())
        await writer.drain()
        resp = (await reader.readline()).decode().strip().split(Protocol.PARAM_SEPARATOR)
        if resp and resp[0] == Protocol.Result.SUCCESS:
            self.client_id = resp[1]
            log.success(f'Connected!')
        else:
            log.error(f'Failed!')
            return

        self.tcp_maps = {}
        self.udp_maps = {}
        self.__udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.__udp_socket.setblocking(False)
        self.__udp_req_map = {}
        self.__udp_ping = time.time()

        self.__protocol = Protocol((reader, writer))
        self.__task_serve_req = asyncio.create_task(self.__serve_request_task())
        self.__task_udp_receive = asyncio.create_task(self.__udp_receive_task())
        self.__task_udp_feedback = asyncio.create_task(self.__udp_feedback_task())
        self.__task_daemon = asyncio.create_task(self.__daemon_task())

        self.status = RunningStatus.RUNNING
        log.success('>>>>>>>> Proxy Client is now STARTED !!! <<<<<<<<')

        try:
            for tcp_port in self.ini_tcp_map:
                await self.add_tcp_map(tcp_port, self.ini_tcp_map[tcp_port])
            for udp_port in self.ini_udp_map:
                await self.add_udp_map(udp_port, self.ini_udp_map[udp_port])
        except Exception as e:
            log.error(e)
            self.__protocol.close()

        await self.__protocol.wait_closed()
        log.warning('Stopping the Proxy Client ...')

        self.__task_serve_req.cancel()
        self.__task_udp_receive.cancel()
        self.__task_udp_feedback.cancel()
        self.__task_daemon.cancel()

        for key in self.__udp_req_map:
            self.__udp_req_map[key][2].close()
        self.__udp_socket.close()

        self.status = RunningStatus.STOPPED
        log.warning('>>>>>>>> Proxy Client is now STOPPED !!! <<<<<<<<')

    async def startup(self):
        asyncio.create_task(self.__main_task())

    async def shutdown(self):
        log.warning(f'Stopping the Proxy Client by calling ...')
        self.__protocol.close()
        while self.status != RunningStatus.STOPPED:
            await asyncio.sleep(Protocol.TASK_SCHEDULE_PERIOD)

    async def pause(self):
        if Protocol.Result.SUCCESS == await self.__protocol.request(
                Protocol.ClientCommand.PAUSE_PROXY, '', Protocol.CONNECTION_TIMEOUT):
            self.status = RunningStatus.PENDING
            log.warning('>>>>>>>> Proxy Client is now PENDING ... <<<<<<<<')
        else:
            log.error('Failed to pause the Proxy Client.')

    async def resume(self):
        if Protocol.Result.SUCCESS == await self.__protocol.request(
                Protocol.ClientCommand.RESUME_PROXY, '', Protocol.CONNECTION_TIMEOUT):
            self.status = RunningStatus.RUNNING
            log.success('>>>>>>>> Proxy Client is now RESUMED ... <<<<<<<<')
        else:
            log.error('Failed to resume the Proxy Client.')

    def run(self, debug=False):
        asyncio.run(self.__main_task(), debug=debug)


def run_proxy_client(server_host, server_port, port_map, cid=None, name='AnonymousClient'):
    """ 运行代理客户端。
    :param server_host: 服务器主机
    :param server_port: 服务器端口
    :param port_map: 客户端的映射表
    :param cid: 客户端ID
    :param name: 客户端名称（客户端ID由服务端自动生成）
    :return:
    """
    proxy_client = ProxyClient(server_host, server_port, port_map, cid, name)
    proxy_client.run(debug=False)


def run_proxy_client_with_reconnect(server_host, server_port, port_map, cid=None, name='AnonymousClient'):
    """ 运行代理客户端，并在连接断开的情况下按一定的等待机制（使用相同的客户端ID）自动重连。
    :param server_host: 服务器主机
    :param server_port: 服务器端口
    :param port_map: 客户端的映射表
    :param cid: 客户端ID
    :param name: 客户端名称（客户端ID由服务端自动生成）
    :return:
    """
    seed = 1
    wait = 2
    while True:
        if check_listening(server_host, server_port):
            proxy_client = ProxyClient(server_host, server_port, port_map, cid, name)
            proxy_client.run(debug=False)
            time_halt = 10  # halting period in case of disconnected
            log.info(f'Proxy client is disconnected, halt {time_halt}s before reconnect ...')
            time.sleep(time_halt)
            seed = 1
            wait = 2
            cid = proxy_client.client_id
        else:
            log.error('Proxy Service is Unreachable.')
            if wait < Protocol.MAX_RETRY_PERIOD:
                tmp = wait
                wait += seed
                seed = tmp
                if wait > Protocol.MAX_RETRY_PERIOD:
                    wait = Protocol.MAX_RETRY_PERIOD
            log.info(f'Trying to reconnect Server({server_host}:{server_port}) after {wait}s ...')
            time.sleep(wait)


if __name__ == '__main__':
    check_python_version(3, 7)

    log.remove()
    log.add(sys.stderr, level="DEBUG")
    # log.add("any-proxy-server.log", level="INFO", rotation="1 month")

    _client_id = '80000001'
    _client_name = 'DemoClient'
    _port_map = {'TCP': {80: 10080}, 'UDP': {}}

    # run_proxy_client('0.0.0.0', 10000, _port_map, _client_id, _client_name)
    run_proxy_client_with_reconnect('localhost', 10000, _port_map, _client_id, _client_name)
