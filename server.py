import sys
import asyncio
import socket
import time
from datetime import datetime
from uuid import uuid4
from operator import methodcaller
from protocol import Protocol, ProtocolError
from common import RunningStatus, ProxyError
from utils import check_python_version, check_listening, check_udp_port_available, validate_id_string
from loguru import logger as log


class TcpMapInfo:
    CLIENT_PORT = 'client_port'
    CONN_POOL = 'connection_pool'
    TASK_LISTEN = 'task_listen'
    SWITCH = 'is_running'
    STATISTIC = 'statistic'
    CREATE_TIME = 'create_time'


class UdpMapInfo:
    CLIENT_PORT = 'client_port'
    UDP_SOCKET = 'udp_socket'
    TASK_TRANSFER = 'task_transfer'
    SWITCH = 'is_running'
    STATISTIC = 'statistic'
    CREATE_TIME = 'create_time'


class _PeerClient:
    """端口代理转发客户端连接类：支持TCP和UDP映射
    """
    CONN_QUEUE_SIZE = 100
    
    def __init__(self, server_host, sock_stream, client_id, client_name):
        self.server_host = socket.gethostbyname(server_host)
        self.client_id = client_id
        self.client_name = client_name
        self.tcp_e_address = sock_stream[1].get_extra_info('peername')
        self.udp_e_address = (None, None)       # NAT external address of this client.

        self.tcp_maps = {}                      # {server_port: {...}, ...}
        self.udp_maps = {}                      # {server_port: {...}, ...}

        self.__conn_queue = asyncio.Queue()     # {(server_port, reader, writer), ...}

        self.__protocol = Protocol(sock_stream)
        self.__task_serve_req = asyncio.create_task(self.__serve_request_task())
        self.__task_process_conn = asyncio.create_task(self.__process_conn_task())
        self.__task_watchdog = asyncio.create_task(self.__watchdog_task())

        self.status = RunningStatus.RUNNING     # PREPARE -> RUNNING (-> PENDING) -> STOPPED
        self.ping = time.time()                 # the period when the last tcp ping receives.
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

    async def __tcp_service_task(self, server_port):

        async def tcp_service_handler(reader, writer):
            if self.status == RunningStatus.RUNNING and self.tcp_maps[server_port][TcpMapInfo.SWITCH]:
                await self.__conn_queue.put((server_port, reader, writer))
            else:
                writer.close()

        server = await asyncio.start_server(tcp_service_handler, self.server_host, server_port)
        async with server:
            await server.wait_closed()

    async def __tcp_data_relay_task(self, server_port, sock_stream, peer_stream):

        async def data_relay_monitor():
            while True:
                if self.status == RunningStatus.RUNNING and server_port in self.tcp_maps \
                        and self.tcp_maps[server_port][TcpMapInfo.SWITCH]:
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
                    self.tcp_maps[server_port][TcpMapInfo.STATISTIC][0 if upstream else 1] += len(data)
                except IOError:
                    break

        client_port = self.tcp_maps[server_port][TcpMapInfo.CLIENT_PORT]
        rp = peer_stream[1].get_extra_info("peername")[1]
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

    async def __udp_service_task(self, server_port):
        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_sock.setblocking(False)
        udp_sock.bind((self.server_host, server_port))
        self.udp_maps[server_port][UdpMapInfo.UDP_SOCKET] = udp_sock
        while True:
            if self.status != RunningStatus.RUNNING or not self.udp_maps[server_port][TcpMapInfo.SWITCH]:
                await asyncio.sleep(Protocol.TASK_SCHEDULE_PERIOD)
                continue
            try:
                data, address = udp_sock.recvfrom(Protocol.SOCKET_BUFFER_SIZE)
                if not data:
                    raise IOError(f'EOF from {address}')
            except BlockingIOError:
                await asyncio.sleep(Protocol.TASK_SCHEDULE_PERIOD)
                continue
            except Exception as e:
                log.error(e)
                break
            try:
                len_data = len(data)
                data_packet = Protocol.pack_udp_data_packet(server_port, address, data, pack_data=True)
                if self.udp_e_address:
                    udp_sock.sendto(data_packet, self.udp_e_address)
                    self.udp_statistic(Protocol.UDP_DATA_HEADER_LEN, 0)
                    self.udp_maps[server_port][UdpMapInfo.STATISTIC][0] += len_data
                log.debug(f'Received UDP Packet on Port:{server_port}, Client:{address}, Size:{len_data}')
            except IOError as e:
                log.error(e)
                break
            except Exception as e:
                log.error(e)
        self.udp_maps[server_port][UdpMapInfo.SWITCH] = False
        self.udp_maps[server_port][UdpMapInfo.UDP_SOCKET].close()

    async def __process_conn_task(self):
        while True:
            try:
                server_port, reader, writer = await self.__conn_queue.get()
            except Exception as e:
                log.error(e)
                break
            else:
                self.__conn_queue.task_done()

            try:
                client_port = self.tcp_maps[server_port][TcpMapInfo.CLIENT_PORT]

                result = await self.__protocol.request(
                    Protocol.ServerCommand.ADD_TCP_CONNECTION, client_port, timeout=Protocol.CONNECTION_TIMEOUT)

                status, detail = result.split(Protocol.PARAM_SEPARATOR, 1)
                if status == Protocol.Result.SUCCESS:
                    conn_uuid = detail
                else:
                    raise ProxyError(detail)

                wait_time = 0
                while wait_time < Protocol.CONNECTION_TIMEOUT:
                    if conn_uuid in self.tcp_maps[server_port][TcpMapInfo.CONN_POOL]:
                        break
                    await asyncio.sleep(Protocol.TASK_SCHEDULE_PERIOD)
                    wait_time += Protocol.TASK_SCHEDULE_PERIOD
                if conn_uuid not in self.tcp_maps[server_port][TcpMapInfo.CONN_POOL]:
                    raise ProxyError(f'{self.client_id}({client_port}) ----> Server({server_port}) ... [TIMEOUT].')

                reader2, writer2 = self.tcp_maps[server_port][TcpMapInfo.CONN_POOL][conn_uuid]
                del self.tcp_maps[server_port][TcpMapInfo.CONN_POOL][conn_uuid]
                asyncio.create_task(self.__tcp_data_relay_task(server_port, (reader, writer), (reader2, writer2)))

            except Exception as e:
                log.error(e)
                writer.close()

    async def __serve_request_task(self):
        while True:
            try:
                uuid, cmd, data = await self.__protocol.get_request(timeout=None)
            except Exception as e:
                log.error(e)
                break
            result = Protocol.PARAM_SEPARATOR.join((Protocol.Result.SUCCESS, ''))

            try:
                if self.status != RunningStatus.RUNNING:  # reject any client request.
                    result = Protocol.PARAM_SEPARATOR.join((Protocol.Result.INVALID, 'server is not running'))

                elif cmd == Protocol.Command.PING:
                    self.ping = time.time()
                    result += datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

                elif cmd == Protocol.ClientCommand.CHECK_TCP_PORT:
                    port = int(data)
                    if check_listening('', port):
                        result = Protocol.PARAM_SEPARATOR.join((Protocol.Result.ERROR, 'tcp port is in use'))

                elif cmd == Protocol.ClientCommand.CHECK_UDP_PORT:
                    port = int(data)
                    if not check_udp_port_available(port):
                        result = Protocol.PARAM_SEPARATOR.join((Protocol.Result.ERROR, 'udp port is in use'))

                elif cmd == Protocol.ClientCommand.ADD_TCP_MAP:
                    str_pair = data.split(Protocol.PARAM_SEPARATOR)
                    c = int(str_pair[0])
                    s = int(str_pair[1])
                    await self.add_tcp_map(s, c)

                elif cmd == Protocol.ClientCommand.PAUSE_TCP_MAP:
                    port = int(data)
                    await self.pause_tcp_map(port)

                elif cmd == Protocol.ClientCommand.RESUME_TCP_MAP:
                    port = int(data)
                    await self.resume_tcp_map(port)

                elif cmd == Protocol.ClientCommand.REMOVE_TCP_MAP:
                    port = int(data)
                    await self.remove_tcp_map(port)

                elif cmd == Protocol.ClientCommand.ADD_UDP_MAP:
                    str_pair = data.split(Protocol.PARAM_SEPARATOR)
                    c = int(str_pair[0])
                    s = int(str_pair[1])
                    await self.add_udp_map(s, c)

                elif cmd == Protocol.ClientCommand.PAUSE_UDP_MAP:
                    port = int(data)
                    await self.pause_udp_map(port)

                elif cmd == Protocol.ClientCommand.RESUME_UDP_MAP:
                    port = int(data)
                    await self.resume_udp_map(port)

                elif cmd == Protocol.ClientCommand.REMOVE_UDP_MAP:
                    port = int(data)
                    await self.remove_udp_map(port)

                elif cmd == Protocol.ClientCommand.PAUSE_PROXY:
                    await self.pause_client()

                elif cmd == Protocol.ClientCommand.RESUME_PROXY:
                    await self.resume_client()

                elif cmd == Protocol.ClientCommand.DISCONNECT:
                    await self.close_client()

                else:
                    log.warning(f'Unrecognised request from {self.client_name}(CID:{self.client_id}): '
                                f'{Protocol.make_req(uuid, cmd, data)}')
                    result = Protocol.PARAM_SEPARATOR.join((Protocol.Result.INVALID, 'unrecognised request'))

            except Exception as e:
                log.error(f'Error while processing request({Protocol.make_req(uuid, cmd, data)}) '
                          f'from {self.client_name}(CID:{self.client_id}): {e}')
                result = Protocol.PARAM_SEPARATOR.join((Protocol.Result.ERROR, 'error while processing request'))
            finally:
                try:
                    await self.__protocol.send_response(uuid, cmd, result)
                except Exception as e:
                    log.error(e)
                    break

    async def __watchdog_task(self):
        await self.__protocol.wait_closed()
        log.warning(f'Stopping {self.client_name}(CID:{self.client_id}) by watchdog ...')

        for server_port in list(self.tcp_maps.keys()):
            await self.remove_tcp_map(server_port)
        for server_port in list(self.udp_maps.keys()):
            await self.remove_udp_map(server_port)
        while not self.__conn_queue.empty():
            stream = self.__conn_queue.get_nowait()
            stream[1].close()
            self.__conn_queue.task_done()
        self.__conn_queue = None
        self.__task_serve_req.cancel()
        self.__task_process_conn.cancel()

        self.status = RunningStatus.STOPPED
        log.warning(f'{self.client_name}(CID:{self.client_id}) is STOPPED by watchdog !!!')

    def _map_msg(self, server_port, client_port, status, extra=None):
        return f'{ sys._getframe(1).f_code.co_name.upper() }: ' \
               f'Server({server_port}) >>> {self.client_id}({client_port}) ... ' \
               f'[{ status.upper() }]{ " (" + extra + ")" if extra else "" }'

    async def add_tcp_map(self, server_port, client_port):
        if server_port in self.tcp_maps:
            raise ProxyError(self._map_msg(server_port, client_port, 'ERROR', f'already registered.'))
        if check_listening(self.server_host, server_port):
            raise ProxyError(self._map_msg(server_port, client_port, 'ERROR', f'target port is in use.'))
        self.tcp_maps[server_port] = {
            TcpMapInfo.CLIENT_PORT: client_port,
            TcpMapInfo.SWITCH: True,
            TcpMapInfo.STATISTIC: [0, 0],
            TcpMapInfo.CREATE_TIME: datetime.utcnow(),
            TcpMapInfo.CONN_POOL: {},
            TcpMapInfo.TASK_LISTEN: asyncio.create_task(self.__tcp_service_task(server_port)),
        }
        log.success(self._map_msg(server_port, client_port, 'OK'))

    async def pause_tcp_map(self, server_port):
        if server_port not in self.tcp_maps:
            raise ProxyError(self._map_msg(server_port, None, 'ERROR', f'not registered.'))
        self.tcp_maps[server_port][TcpMapInfo.SWITCH] = False
        log.success(self._map_msg(server_port, self.tcp_maps[server_port][TcpMapInfo.CLIENT_PORT], 'OK'))

    async def resume_tcp_map(self, server_port):
        if server_port not in self.tcp_maps:
            raise ProxyError(self._map_msg(server_port, None, 'ERROR', f'not registered.'))
        self.tcp_maps[server_port][TcpMapInfo.SWITCH] = True
        log.success(self._map_msg(server_port, self.tcp_maps[server_port][TcpMapInfo.CLIENT_PORT], 'OK'))

    async def remove_tcp_map(self, server_port):
        if server_port not in self.tcp_maps:
            raise ProxyError(self._map_msg(server_port, None, 'ERROR', f'not registered.'))
        await self.pause_tcp_map(server_port)
        for stream in self.tcp_maps[server_port][TcpMapInfo.CONN_POOL]:
            stream[1].close()
        self.tcp_maps[server_port][TcpMapInfo.TASK_LISTEN].cancel()
        log.success(self._map_msg(server_port, self.tcp_maps[server_port][TcpMapInfo.CLIENT_PORT], 'OK'))
        del self.tcp_maps[server_port]

    async def add_udp_map(self, server_port, client_port):
        if server_port in self.udp_maps:
            raise ProxyError(self._map_msg(server_port, client_port, 'ERROR', f'already registered.'))
        if not check_udp_port_available(server_port):
            raise ProxyError(self._map_msg(server_port, client_port, 'ERROR', f'target port is in use.'))
        self.udp_maps[server_port] = {
            UdpMapInfo.CLIENT_PORT: client_port,
            UdpMapInfo.UDP_SOCKET: None,
            UdpMapInfo.SWITCH: True,
            UdpMapInfo.STATISTIC: [0, 0],
            UdpMapInfo.CREATE_TIME: datetime.utcnow(),
            UdpMapInfo.TASK_TRANSFER: asyncio.create_task(self.__udp_service_task(server_port)),
        }
        log.success(self._map_msg(server_port, client_port, 'OK'))

    async def pause_udp_map(self, server_port):
        if server_port not in self.udp_maps:
            raise ProxyError(self._map_msg(server_port, None, 'ERROR', f'not registered.'))
        self.udp_maps[server_port][UdpMapInfo.SWITCH] = False
        log.success(self._map_msg(server_port, self.udp_maps[server_port][UdpMapInfo.CLIENT_PORT], 'OK'))

    async def resume_udp_map(self, server_port):
        if server_port not in self.udp_maps:
            raise ProxyError(self._map_msg(server_port, None, 'ERROR', f'not registered.'))
        self.udp_maps[server_port][UdpMapInfo.SWITCH] = True
        log.success(self._map_msg(server_port, self.udp_maps[server_port][UdpMapInfo.CLIENT_PORT], 'OK'))

    async def remove_udp_map(self, server_port):
        if server_port not in self.udp_maps:
            raise ProxyError(self._map_msg(server_port, None, 'ERROR', f'not registered.'))
        await self.pause_udp_map(server_port)
        self.udp_maps[server_port][UdpMapInfo.UDP_SOCKET].close()
        self.udp_maps[server_port][UdpMapInfo.TASK_TRANSFER].cancel()
        log.success(self._map_msg(server_port, self.udp_maps[server_port][UdpMapInfo.CLIENT_PORT], 'OK'))
        del self.udp_maps[server_port]

    async def pause_client(self):
        msg = f'PAUSE_CLIENT: {self.client_name}(CID:{self.client_id}) ... '
        if self.status == RunningStatus.STOPPED:
            raise ProxyError(msg + '[ERROR] (already stopped.)')
        self.status = RunningStatus.PENDING
        log.success(msg + '[OK]')

    async def resume_client(self):
        msg = f'RESUME_CLIENT: {self.client_name}(CID:{self.client_id}) ... '
        if self.status == RunningStatus.STOPPED:
            raise ProxyError(msg + '[ERROR] (already stopped.)')
        self.status = RunningStatus.RUNNING
        log.success(msg + '[OK]')

    async def close_client(self, wait=True):
        self.__protocol.close()
        if wait:
            while self.status != RunningStatus.STOPPED:
                await asyncio.sleep(Protocol.TASK_SCHEDULE_PERIOD)


class ProxyServer:
    """端口代理转发客户端连接管理类
    """
    def __init__(self, host='', port=10000, sid=None, name=None):
        host = socket.gethostbyname(host)
        port = int(port)
        if not sid:
            sid = uuid4().hex[-Protocol.DEFAULT_SID_LENGTH:].upper()
        if not name:
            name = 'ProxyServer'

        self.server_id = sid                    #
        self.server_name = name                 #
        self.server_host = host                 # '0.0.0.0'
        self.server_port = port                 #

        self.__clients = {}                     # {client_id: {...}, ...}
        self.__conn_queue = None                # {(reader, writer), ...}
        self.__udp_socket = None                # udp socket for datagram relaying.

        self.__task_tcp_service = None          #
        self.__task_process_conn = None         #
        self.__task_dispatch_datagram = None    #
        self.__task_daemon = None               #

        self.status = RunningStatus.PREPARE     # PREPARE -> RUNNING -> STOPPED
        self.timestamp = datetime.utcnow()      # the period when server creates.

    class AsyncApi:
        ADD_TCP_MAP = 'add_tcp_map'             # args: server_port, client_port
        PAUSE_TCP_MAP = 'pause_tcp_map'         # args: server_port
        RESUME_TCP_MAP = 'resume_tcp_map'       # args: server_port
        REMOVE_TCP_MAP = 'remove_tcp_map'       # args: server_port

        ADD_UDP_MAP = 'add_udp_port'            # args: server_port, client_port
        PAUSE_UDP_MAP = 'pause_udp_map'         # args: server_port
        RESUME_UDP_MAP = 'resume_udp_map'       # args: server_port
        REMOVE_UDP_MAP = 'remove_udp_map'       # args: server_port

        PAUSE_CLIENT = 'pause_client'           # args: client_id
        RESUME_CLIENT = 'resume_client'         # args: client_id
        CLOSE_CLIENT = 'close_client'           # args: client_id

        # ADD_CLIENT = '__add_client'           # args: client_id, client_name, sock_stream
        REMOVE_CLIENT = 'remove_client'         # args: client_id

        STARTUP_SERVER = 'startup'              # args:
        SHUTDOWN_SERVER = 'shutdown'            # args:

    async def __add_client(self, client_id, client_name, sock_stream):
        msg = f'ADD_CLIENT: {client_name}(CID:{client_id}) ... '
        if not validate_id_string(client_id, Protocol.CID_LENGTH):
            raise ProxyError(msg + f'[ERROR] (invalid client id provided.)')
        if client_id in self.__clients:
            if self.__clients[client_id].status == RunningStatus.STOPPED:
                del self.__clients[client_id]
            else:
                raise ProxyError(msg + f'[ERROR] (client id already registered.)')
        self.__clients[client_id] = _PeerClient(self.server_host, sock_stream, client_id, client_name)
        log.success(msg + '[OK]')

    async def remove_client(self, client_id):
        if client_id not in self.__clients:
            raise ProxyError(f'REMOVE_CLIENT: (None)(CID:{client_id}) ... [ERROR] (not registered.)')
        if self.__clients[client_id].status != RunningStatus.STOPPED:
            await self.__clients[client_id].close_client(wait=True)
        log.success(f'REMOVE_CLIENT: {self.__clients[client_id].client_name}(CID:{client_id}) ... [OK]')
        del self.__clients[client_id]

    async def __tcp_service_task(self):

        async def tcp_service_handler(reader, writer):
            await self.__conn_queue.put((reader, writer))

        server = await asyncio.start_server(tcp_service_handler, self.server_host, self.server_port)
        async with server:
            await server.wait_closed()

    async def __process_conn_task(self):

        async def stream_readline(stream_reader):
            return await stream_reader.readline()

        while True:
            try:
                reader, writer = await self.__conn_queue.get()
            except Exception as e:
                log.error(e)
                break
            else:
                self.__conn_queue.task_done()

            try:
                task = asyncio.create_task(stream_readline(reader))
                await asyncio.wait_for(task, timeout=Protocol.CONNECTION_TIMEOUT)
                identify = task.result().decode().strip()
                if not identify:
                    peer = writer.get_extra_info('peername')
                    raise ProxyError(f'Identify string is empty of tcp connection from {peer}')
                log.debug(identify)
            except Exception as e:
                log.error(e)
                writer.close()
                continue

            reject_reason = 'unknown error'
            try:
                conn_type, data = identify.split(Protocol.PARAM_SEPARATOR, 1)
                val_list = data.split(Protocol.PARAM_SEPARATOR)

                if conn_type == Protocol.ConnectionType.MANAGER:
                    reject_reason = 'role of MANAGER is not supported yet.'
                    raise NotImplementedError('Reject manager connection as ' + reject_reason)
                elif conn_type == Protocol.ConnectionType.PROXY_CLIENT:
                    client_id = val_list[0]
                    client_name = val_list[1]
                    if not client_id:  # generate new client id if it is empty
                        while True:
                            client_id = uuid4().hex[-Protocol.CID_LENGTH:].upper()
                            if client_id not in self.__clients:
                                break
                    try:
                        await self.__add_client(client_id, client_name, (reader, writer))
                    except Exception as e:
                        reject_reason = str(e)
                        raise
                    writer.write(f'{Protocol.Result.SUCCESS}{Protocol.PARAM_SEPARATOR}{client_id}\n'.encode())
                    await writer.drain()
                elif conn_type == Protocol.ConnectionType.PROXY_TCP_DATA:
                    client_id = val_list[0]
                    server_port = int(val_list[1])
                    conn_id = val_list[2]
                    if client_id not in self.__clients:
                        reject_reason = f'provided client_id({client_id}) not found.'
                        raise ProxyError('Reject reverse-data-connection (tcp) as ' + reject_reason)
                    else:
                        client = self.__clients[client_id]
                    if client.status != RunningStatus.RUNNING:
                        reject_reason = f'{client.client_name}(CID:{client.client_id}) is paused.'
                        raise ProxyError('Reject reverse-data-connection (tcp) as ' + reject_reason)
                    if not client.tcp_maps[server_port][TcpMapInfo.SWITCH]:
                        reject_reason = f'server_port({server_port}) is paused.'
                        raise ProxyError('Reject reverse-data-connection (tcp) as ' + reject_reason)
                    client.tcp_maps[server_port][TcpMapInfo.CONN_POOL][conn_id] = (reader, writer)
                    writer.write(f'{Protocol.Result.SUCCESS}{Protocol.PARAM_SEPARATOR}\n'.encode())
                    await writer.drain()
                else:
                    reject_reason = 'unrecognised connection type'
                    raise ProxyError(f'Reject connection on port({self.server_port}) with identify: {identify}')
            except Exception as e:
                log.error(e)
                writer.write(f'{Protocol.Result.ERROR}{Protocol.PARAM_SEPARATOR}{reject_reason}\n'.encode())
                await writer.drain()
                writer.close()

    async def __dispatch_datagram_task(self):
        while True:
            try:
                data, address = self.__udp_socket.recvfrom(Protocol.SOCKET_BUFFER_SIZE)
            except BlockingIOError:
                await asyncio.sleep(Protocol.TASK_SCHEDULE_PERIOD)
                continue
            except Exception as e:
                log.error(e)
                break

            try:
                try:
                    packet_info = Protocol.unpack_udp_packet(data, unpack_data=False)
                except Exception as e:
                    raise ProxyError(f'Protocol.unpack_udp_packet(): {e}')

                if packet_info[Protocol.UdpPacketInfo.TYPE] == Protocol.UdpPacketType.SYNC:
                    cid = packet_info['client_id']
                    timestamp = packet_info[Protocol.UdpPacketInfo.TIMESTAMP]
                    if cid not in self.__clients:
                        raise ProxyError(f'Received udp sync packet from client(CID:{cid}) which is not found.')
                    client = self.__clients[cid]
                    log.debug(f'Received udp sync packet from {client.client_name}(CID:{cid}) at {timestamp}')
                    client.udp_statistic(0, len(data))
                    client.udp_e_address = address
                    if client.status == RunningStatus.RUNNING:
                        sync_packet = Protocol.pack_udp_sync_packet(server_id=self.server_id)
                        try:
                            self.__udp_socket.sendto(sync_packet, address)
                        except IOError as e:
                            log.error(e)
                            break
                        client.udp_statistic(len(sync_packet), 0)
                elif packet_info[Protocol.UdpPacketInfo.TYPE] == Protocol.UdpPacketType.DATA:
                    server_port = packet_info[Protocol.UdpPacketInfo.SERVER_PORT]
                    user_address = packet_info[Protocol.UdpPacketInfo.USER_ADDRESS]
                    client = None
                    for cid in self.__clients:
                        if server_port in self.__clients[cid].udp_maps:
                            client = self.__clients[cid]
                            client.udp_statistic(0, Protocol.UDP_DATA_HEADER_LEN)
                            client.udp_maps[server_port][UdpMapInfo.STATISTIC][1] += \
                                len(data) - Protocol.UDP_DATA_HEADER_LEN
                            break
                    if not client:
                        raise ProxyError(f'Received udp data packet which is not owned by any client.')
                    if client.status == RunningStatus.RUNNING and client.udp_maps[server_port][UdpMapInfo.SWITCH]:
                        udp_socket = client.udp_maps[server_port][UdpMapInfo.UDP_SOCKET]
                        try:
                            udp_socket.sendto(data[Protocol.UDP_DATA_HEADER_LEN:], user_address)
                        except IOError as e:
                            log.error(e)
                            continue
                        log.debug(f'Forwards udp data packet from port({server_port}) to {user_address}')
                    else:
                        log.warning(f'Drops udp data packet on port({server_port}) as switch is turned off.')
                else:
                    raise ProxyError(f'Received udp packet from {address} with unknown type.')
            except Exception as e:
                log.debug(f'Received udp packet from: {address}, data:\n' + data.hex())
                log.warning(e)

    async def __daemon_task(self):
        cnt = 0
        ping_timeout = Protocol.CLIENT_TCP_PING_PERIOD + Protocol.CONNECTION_TIMEOUT  # *2
        while True:
            #
            # Check if client connection is lost.
            #
            for c in self.__clients:
                #
                # Attention： As the client ping request may arrives in
                # T(Protocol.CLIENT_TCP_PING_PERIOD) + 2 * T(Protocol.CONNECTION_TIMEOUT) seconds,
                # and the server should regard a client as LOST and REMOVE it prior to the client
                # detected the connection is unreachable (the client may auto reconnect after that). So,
                # the definition of threshold `ping_timeout` here is very important !!!
                #
                # Also see the definition of ProxyClient.__daemon_task in client.py.
                #
                if self.__clients[c].status != RunningStatus.STOPPED and \
                        time.time() - self.__clients[c].ping > ping_timeout:
                    log.warning(f'{self.__clients[c].client_name}(CID:{self.__clients[c].client_id})'
                                f' is inactive for {ping_timeout}s, and will be terminated ...')
                    await self.__clients[c].close_client(wait=False)
            #
            # Every loop comes with a rest.
            #
            cnt += 1
            await asyncio.sleep(1)

    async def __main_task(self):
        if self.status == RunningStatus.RUNNING:
            log.error('Abort starting up the Proxy Server as it is already running.')
            return
        self.__clients = {}
        self.__conn_queue = asyncio.Queue()
        self.__udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.__udp_socket.setblocking(False)
        self.__udp_socket.bind((self.server_host, self.server_port))

        self.__task_tcp_service = asyncio.create_task(self.__tcp_service_task())
        self.__task_process_conn = asyncio.create_task(self.__process_conn_task())
        self.__task_dispatch_datagram = asyncio.create_task(self.__dispatch_datagram_task())
        self.__task_daemon = asyncio.create_task(self.__daemon_task())

        self.status = RunningStatus.RUNNING
        log.success('>>>>>>>> Proxy Service is now STARTED !!! <<<<<<<<')
        log.info(f'Serving on {self.server_host}:{self.server_port} (tcp & udp) ...')

        done, pending = await asyncio.wait({
            self.__task_tcp_service,
            self.__task_process_conn,
            self.__task_dispatch_datagram,
            self.__task_daemon,  # We terminate `__main_task` by cancel `__task_daemon`.
        }, return_when=asyncio.FIRST_COMPLETED)
        log.warning('Stopping the Proxy Server ...')

        for task in done:
            log.critical(task)
        for task in pending:
            task.cancel()

        for c in list(self.__clients):
            await self.__clients[c].close_client(wait=True)
            del self.__clients[c]
        while not self.__conn_queue.empty():
            stream = self.__conn_queue.get_nowait()
            stream[1].close()
            self.__conn_queue.task_done()
        self.__conn_queue = None
        self.__udp_socket.close()

        self.status = RunningStatus.STOPPED
        log.warning('>>>>>>>> Proxy Service is now STOPPED !!! <<<<<<<<')

    async def startup(self):
        asyncio.create_task(self.__main_task())

    async def shutdown(self):
        log.warning(f'Stopping the Proxy Server by calling ...')
        self.__task_daemon.cancel()
        while self.status != RunningStatus.STOPPED:
            await asyncio.sleep(Protocol.TASK_SCHEDULE_PERIOD)

    def run(self, debug=False):
        asyncio.run(self.__main_task(), debug=debug)

    def is_running(self):
        return self.status == RunningStatus.RUNNING

    def reset_statistic(self, client_id=None):
        if client_id and client_id not in self.__clients:
            log.error(f'Failed to reset statistic of Client(CID:{client_id}) as target client not found.')
            return
        for c in (client_id,) if client_id else self.__clients:
            for p in self.__clients[c].tcp_maps:
                self.__clients[c].tcp_maps[p][TcpMapInfo.STATISTIC][0] = 0
                self.__clients[c].tcp_maps[p][TcpMapInfo.STATISTIC][1] = 0
            for p in self.__clients[c].udp_maps:
                self.__clients[c].udp_maps[p][UdpMapInfo.STATISTIC][0] = 0
                self.__clients[c].udp_maps[p][UdpMapInfo.STATISTIC][1] = 0
            self.__clients[c].tcp_statistic(None, None)
            self.__clients[c].udp_statistic(None, None)
        log.success(f'Client(CID:{client_id})' if client_id else 'All clients' + ' statistic was reset.')

    def execute(self, loop, api_name, *args, timeout=None):
        """
        客户端及其代理端口的操作接口。以一般调用的方式执行协程函数功能。
        :param loop:
        :param api_name:
        :param args:
        :param timeout:
        :return:
        """
        tcp_api_list = (self.AsyncApi.PAUSE_TCP_MAP, self.AsyncApi.RESUME_TCP_MAP, self.AsyncApi.REMOVE_TCP_MAP)
        udp_api_list = (self.AsyncApi.PAUSE_UDP_MAP, self.AsyncApi.RESUME_UDP_MAP, self.AsyncApi.REMOVE_UDP_MAP)
        client_api_list = (self.AsyncApi.PAUSE_CLIENT, self.AsyncApi.RESUME_CLIENT)
        client_api_list2 = (self.AsyncApi.REMOVE_CLIENT,)
        server_api_list = (self.AsyncApi.STARTUP_SERVER, self.AsyncApi.SHUTDOWN_SERVER)

        try:
            if api_name in tcp_api_list + udp_api_list:
                client_object = None
                server_port = args[0]
                for c in self.__clients:
                    if api_name in tcp_api_list and server_port in self.__clients[c].tcp_maps or \
                            api_name in udp_api_list and server_port in self.__clients[c].udp_maps:
                        client_object = self.__clients[c]
                        break
                if client_object is None:
                    raise ProxyError(f'proxy port{server_port} not found.')
                asyncio.run_coroutine_threadsafe(
                    methodcaller(api_name, server_port)(client_object), loop=loop
                ).result(timeout=timeout)
            elif api_name in client_api_list:
                client_id = args[0]
                if client_id not in self.__clients:
                    raise ProxyError(f'client with cid({client_id}) not found.')
                asyncio.run_coroutine_threadsafe(
                    methodcaller(api_name)(self.__clients[client_id]), loop=loop
                ).result(timeout=timeout)
            elif api_name in client_api_list2:
                client_id = args[0]
                if client_id not in self.__clients:
                    raise ProxyError(f'client with cid({client_id}) not found.')
                asyncio.run_coroutine_threadsafe(
                    methodcaller(api_name, client_id)(self), loop=loop
                ).result(timeout=timeout)
            elif api_name in server_api_list:
                asyncio.run_coroutine_threadsafe(
                    methodcaller(api_name)(self), loop=loop
                ).result(timeout=timeout)
            else:
                raise ProxyError(f'{api_name} not supported.')
        except ProxyError as e:
            raise ProxyError(f'Call of API({api_name}) failed as {e}')

    def _get_server_info(self):
        protocol_tcp_s = [0, 0]
        protocol_udp_s = [0, 0]
        port_tcp_s = [0, 0]
        port_udp_s = [0, 0]
        alive_clients = 0
        for c in self.__clients:
            if self.__clients[c].status != RunningStatus.STOPPED:
                alive_clients += 1
            pt = self.__clients[c].tcp_statistic(0, 0)
            pu = self.__clients[c].udp_statistic(0, 0)
            protocol_tcp_s[0] += pt[0]
            protocol_tcp_s[1] += pt[1]
            protocol_udp_s[0] += pu[0]
            protocol_udp_s[1] += pu[1]
            for p in self.__clients[c].tcp_maps:
                port_tcp_s[0] += self.__clients[c].tcp_maps[p][TcpMapInfo.STATISTIC][0]
                port_tcp_s[1] += self.__clients[c].tcp_maps[p][TcpMapInfo.STATISTIC][1]
            for p in self.__clients[c].udp_maps:
                port_udp_s[0] += self.__clients[c].udp_maps[p][UdpMapInfo.STATISTIC][0]
                port_udp_s[1] += self.__clients[c].udp_maps[p][UdpMapInfo.STATISTIC][1]
        server_info = {
            "server_id": self.server_id,
            "server_name": self.server_name,
            "server_host": self.server_host,
            "server_port": self.server_port,
            "start_time": self.timestamp,
            "is_running": self.status == RunningStatus.RUNNING,
            "alive_clients": alive_clients,
            "total_clients": len(self.__clients),
            "tcp_statistic": (port_tcp_s[0], port_tcp_s[1]),
            "udp_statistic": (port_udp_s[0], port_udp_s[1]),
            "total_statistic": (protocol_tcp_s[0] + protocol_udp_s[0] + port_tcp_s[0] + port_udp_s[0],
                                protocol_tcp_s[1] + protocol_udp_s[1] + port_tcp_s[1] + port_udp_s[1]),
        }
        return server_info

    def _get_clients_info(self, client_id=None):
        clients_info = {}
        if client_id and client_id not in self.__clients:
            return clients_info
        for c in (client_id,) if client_id else self.__clients:
            protocol_tcp_s = self.__clients[c].tcp_statistic(0, 0)
            protocol_udp_s = self.__clients[c].udp_statistic(0, 0)
            port_tcp_s = [0, 0]
            port_udp_s = [0, 0]
            c_switch = self.__clients[c].status == RunningStatus.RUNNING
            tcp_maps = {}
            udp_maps = {}
            for p in sorted(self.__clients[c].tcp_maps):
                tcp_maps[p] = {
                    "client_port": self.__clients[c].tcp_maps[p][TcpMapInfo.CLIENT_PORT],
                    "is_running": self.__clients[c].tcp_maps[p][TcpMapInfo.SWITCH] and c_switch,
                    "statistic": tuple(self.__clients[c].tcp_maps[p][TcpMapInfo.STATISTIC]),
                    "create_time": self.__clients[c].tcp_maps[p][TcpMapInfo.CREATE_TIME],
                }
                port_tcp_s[0] += self.__clients[c].tcp_maps[p][TcpMapInfo.STATISTIC][0]
                port_tcp_s[1] += self.__clients[c].tcp_maps[p][TcpMapInfo.STATISTIC][1]
            for p in sorted(self.__clients[c].udp_maps):
                udp_maps[p] = {
                    "client_port": self.__clients[c].udp_maps[p][UdpMapInfo.CLIENT_PORT],
                    "is_running": self.__clients[c].udp_maps[p][UdpMapInfo.SWITCH] and c_switch,
                    "statistic": tuple(self.__clients[c].udp_maps[p][UdpMapInfo.STATISTIC]),
                    "create_time": self.__clients[c].udp_maps[p][UdpMapInfo.CREATE_TIME],
                }
                port_udp_s[0] += self.__clients[c].udp_maps[p][UdpMapInfo.STATISTIC][0]
                port_udp_s[1] += self.__clients[c].udp_maps[p][UdpMapInfo.STATISTIC][1]
            clients_info[c] = {
                "client_id": self.__clients[c].client_id,
                "client_name": self.__clients[c].client_name,
                "tcp_e_addr": self.__clients[c].tcp_e_address,
                "udp_e_addr": self.__clients[c].udp_e_address,
                "start_time": self.__clients[c].timestamp,
                "is_alive": self.__clients[c].status != RunningStatus.STOPPED,
                "is_running": self.__clients[c].status == RunningStatus.RUNNING,
                "tcp_maps": tcp_maps,
                "udp_maps": udp_maps,
                "tcp_statistic": port_tcp_s,
                "udp_statistic": port_udp_s,
                "total_statistic": (protocol_tcp_s[0] + protocol_udp_s[0] + port_tcp_s[0] + port_udp_s[0],
                                    protocol_tcp_s[1] + protocol_udp_s[1] + port_tcp_s[1] + port_udp_s[1]),
            }
        return clients_info


def run_proxy_server(host, port, name):
    """ 运行代理服务端。
    :param host: 代理服务器地址（本机）
    :param port: 代理服务端口（TCP和UDP同时开启，共用同一个端口号）
    :param name: 代理服务器名称
    :return:
    """
    proxy_server = ProxyServer(host, port, None, name)
    proxy_server.run(debug=False)


def run_proxy_server_with_web(host, port, name, web_port, start_proxy=True):
    """ 运行带web管理功能的代理服务端。
    :param host: 代理服务器地址（本机）
    :param port: 代理服务端口（TCP和UDP同时开启，共用同一个端口号）
    :param name: 代理服务器名称
    :param web_port: Web管理服务端口
    :param start_proxy: 是否直接启动代理服务
    :return:
    """
    from threading import Thread
    from flask_app import create_app

    def thread_proxy_server(loop):
        asyncio.set_event_loop(loop)
        loop.run_forever()

    def proxy_server_api(api_name, *args, timeout=None):
        result = None
        try:
            result = proxy_server.execute(proxy_loop, api_name, *args, timeout=timeout)
        except (asyncio.TimeoutError, ProtocolError, ProxyError, Exception) as e:
            log.error(f'Call of Proxy Server: {e}')
        finally:
            return result

    flask_app = create_app()
    proxy_server = ProxyServer(host, port, None, name)
    proxy_loop = asyncio.new_event_loop()
    thread_proxy = Thread(target=thread_proxy_server, args=(proxy_loop,))

    # flask_app.proxy_server = proxy_server
    flask_app.proxy_api = ProxyServer.AsyncApi
    flask_app.proxy_execute = proxy_server_api
    flask_app.proxy_is_running = proxy_server.is_running
    flask_app.proxy_reset_statistic = proxy_server.reset_statistic
    flask_app.proxy_server_info = proxy_server._get_server_info
    flask_app.proxy_clients_info = proxy_server._get_clients_info

    thread_proxy.setDaemon(True)
    thread_proxy.start()

    if start_proxy:
        flask_app.proxy_execute(ProxyServer.AsyncApi.STARTUP_SERVER)
    try:
        flask_app.run(host=host, port=web_port, debug=False)
    except KeyboardInterrupt:
        flask_app.execute(ProxyServer.AsyncApi.SHUTDOWN_SERVER)
    finally:
        proxy_loop.stop()


if __name__ == '__main__':
    check_python_version(3, 7)

    log.remove()
    log.add(sys.stderr, level="DEBUG")
    # log.add("any-proxy-server.log", level="INFO", rotation="1 month")

    # run_proxy_server('0.0.0.0', 10000, 'ProxyServer')
    run_proxy_server_with_web('0.0.0.0', 10000, 'ProxyServer', web_port=9999)
