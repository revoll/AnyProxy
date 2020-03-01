import asyncio
import socket
import time
from datetime import datetime
from uuid import uuid4
from protocol import Protocol
from common import ProxyError, ProxyStatus as _Status
from utils import check_python_version, check_listening
from loguru import logger as log


class TcpMapInfo:
    CLIENT_PORT = 'client_port'
    SERVER_PORT = 'server_port'
    SWITCH = 'is_running'
    STATISTIC = 'statistic'
    CREATE_TIME = 'create_time'


class UdpMapInfo:
    CLIENT_PORT = 'client_port'
    SERVER_PORT = 'server_port'
    SWITCH = 'is_running'
    STATISTIC = 'statistic'
    CREATE_TIME = 'create_time'


class ProxyClient:
    """端口代理转发类（客户端）
    """

    def __init__(self, server_host, server_port=10000, port_map=None, client_name='DefaultClient'):
        if type(server_port) != int:
            raise ProxyError('Invalid argument for `server_port`: %s' % server_port)
        if not port_map:
            raise ProxyError('Invalid argument for `port_map`: %s' % port_map)
        for maps in (port_map['TCP'], port_map['UDP']):
            for p in maps:
                if type(p) != int or type(maps[p]) != int or list(maps.values()).count(maps[p]) != 1:
                    raise ProxyError('Invalid argument for `port_map`: %s' % port_map)
        check_python_version(3, 7)

        self.client_id = None                   # Generates by server after connection
        self.client_name = client_name          # User defined name
        self.server_host = server_host          # server side host address
        self.server_port = server_port          # for both tcp and udp service

        self.ini_tcp_map = port_map['TCP']      # = {80: 10080, 81: 10081}
        self.ini_udp_map = port_map['UDP']      # = {80: 10080}

        self.tcp_maps = {}                      #
        self.udp_maps = {}                      #

        self.__udp_socket = None                # socket for udp packet relay
        self.__udp_req_map = {}                 #

        self.__protocol = None                  #
        self.__task_serve_req = None            #
        self.__task_udp_accept = None           #
        self.__task_udp_feedback = None         #
        self.__task_daemon = None               #
        self.__task_watchdog = None             #

        self.status = _Status.PREPARE           # 运行状态
        self.timestamp = datetime.utcnow()      # 服务创建时间

    async def __tcp_data_relay_task(self, client_port, stream_1, stream_2):

        async def data_relay_monitor(mappings, _port):
            while True:
                if _port not in mappings or not mappings[_port][TcpMapInfo.SWITCH]:
                    break
                await asyncio.sleep(Protocol.TASK_SCHEDULE_PERIOD)

        async def simplex_data_relay(reader, writer):
            while True:
                try:
                    data = await reader.read(Protocol.SOCKET_BUFFER_SIZE)
                    if not data:
                        break
                    writer.write(data)
                    await writer.drain()
                    self.tcp_maps[client_port][TcpMapInfo.STATISTIC] += len(data)
                except ConnectionError:
                    break

        server_port = self.tcp_maps[client_port][TcpMapInfo.SERVER_PORT]
        _, pending = await asyncio.wait({
            asyncio.create_task(data_relay_monitor(self.tcp_maps, client_port)),
            asyncio.create_task(simplex_data_relay(stream_1[0], stream_2[1])),
            asyncio.create_task(simplex_data_relay(stream_2[0], stream_1[1])),
        }, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        stream_1[1].close()
        stream_2[1].close()
        log.info(f'Local({client_port}) --x-> {self.server_host}({server_port})')

    async def __serve_request_task(self):
        while True:
            uuid, cmd, data = await self.__protocol.get_request()
            result = None
            try:
                if cmd == Protocol.Command.PING:
                    result = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

                elif cmd == Protocol.ServerCommand.ADD_TCP_CONNECTION:
                    try:
                        client_port = int(data)
                        server_port = self.tcp_maps[client_port][TcpMapInfo.SERVER_PORT]
                        reader1, writer1 = await asyncio.open_connection('127.0.0.1', client_port)
                        reader2, writer2 = await asyncio.open_connection(self.server_host, self.server_port)
                        conn_uuid = uuid4().hex
                        identify = (f'{Protocol.ConnectionType.PROXY_TCP_DATA}:'
                                    f'{self.client_id}{Protocol.SEPARATOR}{server_port}{Protocol.SEPARATOR}{conn_uuid}')
                        writer2.write((identify + '\n').encode())
                        await writer2.drain()
                        resp = (await reader2.readline()).decode().strip()
                        if not resp or resp != Protocol.Result.SUCCESS:
                            raise ProxyError('Add tcp connection failed while connecting to server.')
                    except (ConnectionError, ProxyError) as e:
                        log.warning(e)
                        result = None
                    else:
                        log.info(f'Local({client_port}) ----> {self.server_host}({server_port})')
                        asyncio.create_task(
                            self.__tcp_data_relay_task(client_port, (reader1, writer1), (reader2, writer2)))
                        result = conn_uuid

                else:
                    raise ValueError
            except (ValueError, Exception):
                result = Protocol.Result.INVALID
                log.warning(f'Invalid request from Server: "{Protocol.make_req(uuid, cmd, data)}"')
            finally:
                await self.__protocol.send_response(uuid, cmd, result)

    def __udp_send_data(self, data, udp_port, user_address):
        key = hash((udp_port, user_address))
        if key in self.__udp_req_map:
            sock = self.__udp_req_map[key][2]
            self.__udp_req_map[key][3] = time.time()
        else:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setblocking(False)
            self.__udp_req_map[key] = [udp_port, user_address, sock, time.time()]
        sock.sendto(data, ('', udp_port))

    async def __udp_accept_task(self):
        while True:
            try:
                data, address = self.__udp_socket.recvfrom(Protocol.SOCKET_BUFFER_SIZE)
            except BlockingIOError:
                await asyncio.sleep(Protocol.TASK_SCHEDULE_PERIOD)
                continue
            # if address[0] != self.server_host:
            #    log.warning(f'Illegal udp packet form {address}, not the server.')
            #    continue
            pkt_type, values = Protocol.parse_datagram_header(data)
            if pkt_type == Protocol.UdpPacketType.SYNC:
                log.info(f'UDP_PING: {values[1]}')  # f'Received UDP ping response from server at: {values[1]}'
            elif pkt_type == Protocol.UdpPacketType.DATA:  # or address[1] != values[0]:
                user_address = values[1]
                server_port = values[0]
                client_port = None
                for port in self.udp_maps:
                    if self.udp_maps[port][UdpMapInfo.SERVER_PORT] == server_port:
                        client_port = port
                        break
                if not client_port:
                    log.warning(f'Udp proxy port({server_port}) is not registered.')
                    continue
                self.__udp_send_data(data[Protocol.UDP_DATA_HEADER_LEN:], client_port, user_address)
                log.debug(f'Accept data packet from {user_address} on port({client_port})')
            else:
                log.warning(f'Invalid udp packet from {address}, format is unknown.')
                continue

    async def __udp_feedback_task(self):
        while True:
            for key in self.__udp_req_map:
                client_port, user_address, sock, _ = self.__udp_req_map[key]
                while True:
                    try:
                        data = sock.recv(Protocol.SOCKET_BUFFER_SIZE)
                    except BlockingIOError:
                        break
                    header = Protocol.make_datagram_header(
                        Protocol.UdpPacketType.DATA,
                        proxy_port=self.udp_maps[client_port][UdpMapInfo.SERVER_PORT],
                        user_address=user_address, data_length=len(data))
                    sock.sendto(header + data, (self.server_host, self.server_port))
                    log.debug(f'Sent feedback to server port({self.server_port})')
            await asyncio.sleep(Protocol.TASK_SCHEDULE_PERIOD)

    async def __daemon_task(self):
        cnt = 0
        while True:
            #
            # Keep TCP Channel alive.
            #
            if not (cnt % Protocol.CLIENT_TCP_PING_PERIOD):
                try:
                    timestamp = await self.__protocol.request(Protocol.Command.PING,
                                                              datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                                                              Protocol.CONNECTION_TIMEOUT)
                    log.info(f'TCP_PING: {timestamp}')  # f'Received TCP ping response from server at: {timestamp}'
                except asyncio.TimeoutError:
                    log.warning('Receive TCP ping response from server timeout.')
            #
            # Keep UDP Channel alive.
            #
            if not (cnt % Protocol.CLIENT_UDP_PING_PERIOD):
                pkt = Protocol.make_datagram_header(Protocol.UdpPacketType.SYNC, info=self.client_id)
                self.__udp_socket.sendto(pkt, (self.server_host, self.server_port))
                # log.debug(f'Sent UDP ping packet to server({self.server_host}:{self.server_port})')
            #
            # Remove udp socket connections that is too old.
            #
            timestamp = time.time()
            for key in list(self.__udp_req_map.keys()):
                if timestamp - self.__udp_req_map[key][3] > Protocol.UDP_REQUEST_DEADLINE:
                    val = self.__udp_req_map.pop(key)
                    val[2].close()
            #
            # Every loop comes with a rest.
            #
            cnt += 1
            await asyncio.sleep(1)

    async def __watchdog_task(self):
        await self.__protocol.wait_closed()

        self.__task_serve_req.cancel()
        self.__task_udp_accept.cancel()
        self.__task_udp_feedback.cancel()
        self.__task_daemon.cancel()

        self.status = _Status.STOPPED
        log.warning('Watchdog is activated, and the proxy client will be terminated ...')

    async def add_tcp_map(self, client_port, server_port):
        try:
            if client_port in self.tcp_maps:
                raise ProxyError
            if Protocol.Result.SUCCESS != await self.__protocol.request(
                    Protocol.ClientCommand.ADD_TCP_MAP,
                    f'{client_port}:{server_port}',
                    Protocol.CONNECTION_TIMEOUT):
                raise ProxyError
            self.tcp_maps[client_port] = {
                TcpMapInfo.CLIENT_PORT: client_port,
                TcpMapInfo.SERVER_PORT: server_port,
                TcpMapInfo.SWITCH: False,
                TcpMapInfo.STATISTIC: 0,
                TcpMapInfo.CREATE_TIME: datetime.utcnow(),
            }
        except ProxyError:
            log.error(f'ADD_TCP_MAP: Local({client_port}) >>> {self.server_host}({server_port}) ... [ERROR]')
            return False
        else:
            log.success(f'ADD_TCP_MAP: Local({client_port}) >>> {self.server_host}({server_port}) ... [OK]')
            return True

    async def start_tcp_map(self, client_port):
        server_port = None
        try:
            if client_port in self.tcp_maps:
                server_port = self.tcp_maps[client_port][TcpMapInfo.SERVER_PORT]
            else:
                raise ProxyError
            if not self.tcp_maps[client_port][TcpMapInfo.SWITCH]:
                if Protocol.Result.SUCCESS != await self.__protocol.request(
                        Protocol.ClientCommand.START_TCP_MAP,
                        f'{self.tcp_maps[client_port][TcpMapInfo.SERVER_PORT]}',
                        Protocol.CONNECTION_TIMEOUT):
                    raise ProxyError
            self.tcp_maps[client_port][TcpMapInfo.SWITCH] = True
        except ProxyError:
            log.error(f'START_TCP_MAP: Local({client_port}) >>> {self.server_host}({server_port}) ... [ERROR]')
            return False
        else:
            log.success(f'START_TCP_MAP: Local({client_port}) >>> {self.server_host}({server_port}) ... [OK]')
            return True

    async def stop_tcp_map(self, client_port):
        server_port = None
        try:
            if client_port in self.tcp_maps:
                server_port = self.tcp_maps[client_port][TcpMapInfo.SERVER_PORT]
            else:
                raise ProxyError
            if self.tcp_maps[client_port][TcpMapInfo.SWITCH]:
                if Protocol.Result.SUCCESS != await self.__protocol.request(
                        Protocol.ClientCommand.STOP_TCP_MAP,
                        f'{self.tcp_maps[client_port][TcpMapInfo.SERVER_PORT]}',
                        Protocol.CONNECTION_TIMEOUT):
                    raise ProxyError
            self.tcp_maps[client_port][TcpMapInfo.SWITCH] = False
        except ProxyError:
            log.error(f'STOP_TCP_MAP: Local({client_port}) >>> {self.server_host}({server_port}) ... [ERROR]')
            return False
        else:
            log.success(f'STOP_TCP_MAP: Local({client_port}) >>> {self.server_host}({server_port}) ... [OK]')
            return True

    async def remove_tcp_map(self, client_port):
        server_port = None
        try:
            if client_port in self.tcp_maps:
                server_port = self.tcp_maps[client_port][TcpMapInfo.SERVER_PORT]
            else:
                raise ProxyError
            if Protocol.Result.SUCCESS != await self.__protocol.request(
                    Protocol.ClientCommand.REMOVE_TCP_MAP,
                    f'{self.tcp_maps[client_port][TcpMapInfo.SERVER_PORT]}',
                    Protocol.CONNECTION_TIMEOUT):
                raise ProxyError
            del self.tcp_maps[client_port]
        except ProxyError:
            log.error(f'REMOVE_TCP_MAP: Local({client_port}) >>> {self.server_host}({server_port}) ... [ERROR]')
            return False
        else:
            log.success(f'REMOVE_TCP_MAP: Local({client_port}) >>> {self.server_host}({server_port}) ... [OK]')
            return True

    async def add_udp_map(self, client_port, server_port):
        try:
            if client_port in self.udp_maps:
                raise ProxyError
            if Protocol.Result.SUCCESS != await self.__protocol.request(
                    Protocol.ClientCommand.ADD_UDP_MAP,
                    f'{client_port}:{server_port}',
                    Protocol.CONNECTION_TIMEOUT):
                raise ProxyError
            self.udp_maps[client_port] = {
                UdpMapInfo.CLIENT_PORT: client_port,
                UdpMapInfo.SERVER_PORT: server_port,
                UdpMapInfo.SWITCH: False,
                UdpMapInfo.STATISTIC: 0,
                UdpMapInfo.CREATE_TIME: datetime.utcnow(),
            }
        except ProxyError:
            log.error(f'ADD_UDP_MAP: Local({client_port}) >>> {self.server_host}({server_port}) ... [ERROR]')
            return False
        else:
            log.success(f'ADD_UDP_MAP: Local({client_port}) >>> {self.server_host}({server_port}) ... [OK]')
            return True

    async def start_udp_map(self, client_port):
        server_port = None
        try:
            if client_port in self.udp_maps:
                server_port = self.udp_maps[client_port][UdpMapInfo.SERVER_PORT]
            else:
                raise ProxyError
            if not self.udp_maps[client_port][UdpMapInfo.SWITCH]:
                if Protocol.Result.SUCCESS != await self.__protocol.request(
                        Protocol.ClientCommand.START_UDP_MAP,
                        f'{self.udp_maps[client_port][UdpMapInfo.SERVER_PORT]}',
                        Protocol.CONNECTION_TIMEOUT):
                    raise ProxyError
            self.udp_maps[client_port][UdpMapInfo.SWITCH] = True
        except ProxyError:
            log.error(f'START_UDP_MAP: Local({client_port}) >>> {self.server_host}({server_port}) ... [ERROR]')
            return False
        else:
            log.success(f'START_UDP_MAP: Local({client_port}) >>> {self.server_host}({server_port}) ... [OK]')
            return True

    async def stop_udp_map(self, client_port):
        server_port = None
        try:
            if client_port in self.udp_maps:
                server_port = self.udp_maps[client_port][UdpMapInfo.SERVER_PORT]
            else:
                raise ProxyError
            if self.udp_maps[client_port][UdpMapInfo.SWITCH]:
                if Protocol.Result.SUCCESS != await self.__protocol.request(
                        Protocol.ClientCommand.STOP_UDP_MAP,
                        f'{self.udp_maps[client_port][UdpMapInfo.SERVER_PORT]}',
                        Protocol.CONNECTION_TIMEOUT):
                    raise ProxyError
            self.udp_maps[client_port][UdpMapInfo.SWITCH] = False
        except ProxyError:
            log.error(f'STOP_UDP_MAP: Local({client_port}) >>> {self.server_host}({server_port}) ... [ERROR]')
            return False
        else:
            log.success(f'STOP_UDP_MAP: Local({client_port}) >>> {self.server_host}({server_port}) ... [OK]')
            return True

    async def remove_udp_map(self, client_port):
        server_port = None
        try:
            if client_port in self.udp_maps:
                server_port = self.udp_maps[client_port][UdpMapInfo.SERVER_PORT]
            else:
                raise ProxyError
            if Protocol.Result.SUCCESS != await self.__protocol.request(
                    Protocol.ClientCommand.REMOVE_UDP_MAP,
                    f'{self.udp_maps[client_port][UdpMapInfo.SERVER_PORT]}',
                    Protocol.CONNECTION_TIMEOUT):
                raise ProxyError
            del self.udp_maps[client_port]
        except ProxyError:
            log.error(f'REMOVE_UDP_MAP: Local({client_port}) >>> {self.server_host}({server_port}) ... [ERROR]')
            return False
        else:
            log.success(f'REMOVE_UDP_MAP: Local({client_port}) >>> {self.server_host}({server_port}) ... [OK]')
            return True

    async def startup(self):
        log.info(f'Trying to Connect to Server({self.server_host}:{self.server_port})')
        reader, writer = await asyncio.open_connection(self.server_host, self.server_port)
        writer.write(f'{Protocol.ConnectionType.PROXY_CLIENT}:{self.client_name}\n'.encode())
        await writer.drain()
        resp = (await reader.readline()).decode().strip()
        if resp and resp[:len(Protocol.Result.SUCCESS)] == Protocol.Result.SUCCESS:
            self.client_id = resp[len(Protocol.Result.SUCCESS)+1:]
            log.success(f'Connected!')
        else:
            log.error(f'Error!')
            exit(1)
        self.__udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.__udp_socket.setblocking(False)

        self.__protocol = Protocol((reader, writer))
        self.__task_serve_req = asyncio.create_task(self.__serve_request_task())
        self.__task_udp_accept = asyncio.create_task(self.__udp_accept_task())
        self.__task_udp_feedback = asyncio.create_task(self.__udp_feedback_task())
        self.__task_daemon = asyncio.create_task(self.__daemon_task())
        self.__task_watchdog = asyncio.create_task(self.__watchdog_task())

        self.status = _Status.RUNNING
        log.info('>>>>>>>> Proxy Client is now STARTED !!! <<<<<<<<')
        try:
            for tcp_port in self.ini_tcp_map:
                if await self.add_tcp_map(tcp_port, self.ini_tcp_map[tcp_port]):
                    await self.start_tcp_map(tcp_port)
            for udp_port in self.ini_udp_map:
                if await self.add_udp_map(udp_port, self.ini_udp_map[udp_port]):
                    await self.start_udp_map(udp_port)
        except (ProxyError, Exception) as e:
            log.critical(e)

        while self.status != _Status.STOPPED:
            await asyncio.sleep(Protocol.TASK_SCHEDULE_PERIOD)
        log.info('>>>>>>>> Proxy Client is now STOPPED !!! <<<<<<<<')

    async def pause(self):
        if Protocol.Result.SUCCESS == await self.__protocol.request(
                Protocol.ClientCommand.PAUSE_PROXY, '', Protocol.CONNECTION_TIMEOUT):
            log.success('>>>>>>>> Proxy Client is now PENDING ... <<<<<<<<')
            return True
        else:
            log.error('Failed to pause the Proxy Client.')
            return False

    async def resume(self):
        if Protocol.Result.SUCCESS == await self.__protocol.request(
                Protocol.ClientCommand.RESUME_PROXY, '', Protocol.CONNECTION_TIMEOUT):
            log.success('Proxy Client is now RESUMED.')
            return True
        else:
            log.error('Failed to resume the Proxy Client.')
            return False

    async def exit(self):
        self.status = False
        log.warning('Proxy Client is shutting down ...')
        return True


def run_proxy_client(server_host, server_port, port_map, name):
    client = ProxyClient(server_host, server_port, port_map, name)
    asyncio.run(client.startup(), debug=False)


def run_proxy_client_with_reconnect(server_host, server_port, port_map, name):
    seed = 0
    wait = 1
    while True:
        if check_listening(server_host, server_port):
            client = ProxyClient(server_host, server_port, port_map, name)
            asyncio.run(client.startup(), debug=False)
            seed = 0
            wait = 1
            continue
        if wait < Protocol.MAX_RETRY_PERIOD:
            waited = wait
            wait += seed
            seed = waited
            if wait > Protocol.MAX_RETRY_PERIOD:
                wait = Protocol.MAX_RETRY_PERIOD
        log.warning(f'Trying to reconnect Server({server_host}:{server_port}) after {wait}s ...')
        time.sleep(wait)


if __name__ == '__main__':
    import sys

    log.remove()
    log.add(sys.stderr, level="INFO")
    # log.add("any-proxy-server.log", level="INFO", rotation="1 month")

    _port_map = {'TCP': {80: 10080, 81: 10081, 8080: 18080}, 'UDP': {80: 10080}}
    _client_name = 'TestClient'

    # run_proxy_client('0.0.0.0', 10000, _port_map, _client_name)
    run_proxy_client_with_reconnect('0.0.0.0', 10000, _port_map, _client_name)
    # run_proxy_client_with_reconnect('198.35.45.253', 10000, _port_map, _client_name)
