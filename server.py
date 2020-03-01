import asyncio
import socket
import time
from datetime import datetime
from uuid import uuid4
from protocol import Protocol, ProtocolError
from common import ProxyError, ProxyStatus as _Status
from utils import check_python_version, check_listening, check_udp_port_available, stringify_bytes_val
from loguru import logger as log


class TcpMapInfo:
    SERVER_PORT = 'server_port'
    CLIENT_PORT = 'client_port'
    CONN_POOL = 'connection_pool'
    TASK_LISTEN = 'task_listen'
    SWITCH = 'is_running'
    STATISTIC = 'statistic'
    CREATE_TIME = 'create_time'


class UdpMapInfo:
    SERVER_PORT = 'server_port'
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
        self.server_host = server_host
        self.client_id = client_id
        self.client_name = client_name
        self.tcp_e_address = sock_stream[1].get_extra_info('peername')
        self.udp_e_address = (None, None)       # NAT external address of this client.

        self.tcp_maps = {}                      # {server_port: {...}, ...}
        self.udp_maps = {}                      # {server_port: {...}, ...}

        self.__conn_queue = asyncio.Queue(self.CONN_QUEUE_SIZE)  # {(server_port, reader, writer), ...}

        self.__protocol = Protocol(sock_stream)
        self.__task_serve_req = asyncio.create_task(self.__serve_request_task())
        self.__task_process_conn = asyncio.create_task(self.__process_conn_task())
        self.__task_watchdog = asyncio.create_task(self.__watchdog_task())

        self.status = _Status.RUNNING           # PREPARE -> RUNNING -> PENDING -> STOPPED
        self.timestamp = datetime.utcnow()

    async def __tcp_connection_handler(self, reader, writer):
        server_port = writer.get_extra_info('sockname')[1]
        if server_port in self.tcp_maps:
            await self.__conn_queue.put((server_port, reader, writer))
        else:
            writer.close()

    async def __tcp_service_task(self, server_port):
        server = await asyncio.start_server(
            self.__tcp_connection_handler, self.server_host, server_port)
        self.tcp_maps[server_port][TcpMapInfo.SWITCH] = True
        async with server:
            await server.wait_closed()
        self.tcp_maps[server_port][TcpMapInfo.SWITCH] = False

    async def __tcp_data_relay_task(self, server_port, stream_1, stream_2):

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
                    self.tcp_maps[server_port][TcpMapInfo.STATISTIC] += len(data)
                except ConnectionError:
                    break

        client_port = self.tcp_maps[server_port][TcpMapInfo.CLIENT_PORT]
        _, pending = await asyncio.wait({
            asyncio.create_task(data_relay_monitor(self.tcp_maps, server_port)),
            asyncio.create_task(simplex_data_relay(stream_1[0], stream_2[1])),
            asyncio.create_task(simplex_data_relay(stream_2[0], stream_1[1])),
        }, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        stream_1[1].close()
        stream_2[1].close()
        log.info(f'Server({server_port}) --x-> {self.client_id}({client_port})')

    async def add_tcp_map(self, server_port, client_port):
        try:
            if server_port in self.tcp_maps:
                raise ProxyError(f'Tcp port({server_port}) is already registered in proxy map.')
            if check_listening(self.server_host, server_port):
                raise ProxyError(f'Tcp port({server_port}) is in use.')
            self.tcp_maps[server_port] = {
                TcpMapInfo.SERVER_PORT: server_port,
                TcpMapInfo.CLIENT_PORT: client_port,
                TcpMapInfo.CONN_POOL: {},
                TcpMapInfo.TASK_LISTEN: None,
                TcpMapInfo.SWITCH: False,
                TcpMapInfo.STATISTIC: 0,
                TcpMapInfo.CREATE_TIME: datetime.utcnow(),
            }
        except ProxyError as e:
            log.error(e)
            log.warning(f'ADD_TCP_MAP: Server({server_port}) >>> {self.client_id}({client_port}) ... [ERROR]')
            return False
        else:
            log.success(f'ADD_TCP_MAP: Server({server_port}) >>> {self.client_id}({client_port}) ... [OK]')
            return True

    async def start_tcp_map(self, server_port):
        client_port = None
        try:
            if server_port in self.tcp_maps:
                client_port = self.tcp_maps[server_port][TcpMapInfo.CLIENT_PORT]
            else:
                raise ProxyError(f'Tcp port({server_port}) is not registered in proxy map.')
            if check_listening(self.server_host, server_port):
                raise ProxyError(f'Tcp port({server_port}) is in use.')
            self.tcp_maps[server_port][TcpMapInfo.CONN_POOL] = {}
            self.tcp_maps[server_port][TcpMapInfo.TASK_LISTEN] = \
                asyncio.create_task(self.__tcp_service_task(server_port))
            self.tcp_maps[server_port][TcpMapInfo.SWITCH] = True
        except ProxyError as e:
            log.error(e)
            log.warning(f'START_TCP_MAP: Server({server_port}) >>> {self.client_id}({client_port}) ... [ERROR]')
            return False
        else:
            log.success(f'START_TCP_MAP: Server({server_port}) >>> {self.client_id}({client_port}) ... [OK]')
            return True

    async def stop_tcp_map(self, server_port):
        client_port = None
        try:
            if server_port in self.tcp_maps:
                client_port = self.tcp_maps[server_port][TcpMapInfo.CLIENT_PORT]
            else:
                raise ProxyError(f'Tcp port({server_port}) is not registered in proxy map.')
            if self.tcp_maps[server_port][TcpMapInfo.SWITCH]:
                for stream in self.tcp_maps[server_port][TcpMapInfo.CONN_POOL]:
                    stream[1].close()
                self.tcp_maps[server_port][TcpMapInfo.TASK_LISTEN].cancel()
            self.tcp_maps[server_port][TcpMapInfo.CONN_POOL] = {}
            self.tcp_maps[server_port][TcpMapInfo.SWITCH] = False
        except ProxyError as e:
            log.error(e)
            log.warning(f'STOP_TCP_MAP: Server({server_port}) >>> {self.client_id}({client_port}) ... [ERROR]')
            return False
        else:
            log.success(f'STOP_TCP_MAP: Server({server_port}) >>> {self.client_id}({client_port}) ... [OK]')
            return True

    async def remove_tcp_map(self, server_port):
        client_port = None
        try:
            if server_port in self.tcp_maps:
                client_port = self.tcp_maps[server_port][TcpMapInfo.CLIENT_PORT]
            else:
                raise ProxyError(f'Tcp port({server_port}) is not registered in proxy map.')
            if self.tcp_maps[server_port][TcpMapInfo.SWITCH] and not await self.stop_tcp_map(server_port):
                raise ProxyError(f'Tcp port({server_port}) can not be removed as failed to stop it.')
            del self.tcp_maps[server_port]
        except ProxyError as e:
            log.error(e)
            log.warning(f'REMOVE_TCP_MAP: Server({server_port}) >>> {self.client_id}({client_port}) ... [ERROR]')
            return False
        else:
            log.success(f'REMOVE_TCP_MAP: Server({server_port}) >>> {self.client_id}({client_port}) ... [OK]')
            return True

    async def __udp_service_task(self, server_port):
        self.udp_maps[server_port][UdpMapInfo.SWITCH] = True
        udp_sock = self.udp_maps[server_port][UdpMapInfo.UDP_SOCKET]
        while True:
            try:
                data, address = udp_sock.recvfrom(Protocol.SOCKET_BUFFER_SIZE)
            except BlockingIOError:
                await asyncio.sleep(Protocol.TASK_SCHEDULE_PERIOD)
                continue
            len_data = len(data)
            header = Protocol.make_datagram_header(Protocol.UdpPacketType.DATA, proxy_port=server_port,
                                                   user_address=address, data_length=len_data)
            if self.udp_e_address:
                udp_sock.sendto(header + data, self.udp_e_address)
                self.udp_maps[server_port][UdpMapInfo.STATISTIC] += len_data + len(header)
            log.debug(f'Received UDP Packet, Port:{server_port}, User:{address}, Size:{len(data)}')
        self.udp_maps[server_port][UdpMapInfo.SWITCH] = False
        self.udp_maps[server_port][UdpMapInfo.UDP_SOCKET].close()

    async def add_udp_map(self, server_port, client_port):
        try:
            if server_port in self.udp_maps:
                raise ProxyError(f'Udp port({server_port}) is already registered in proxy map.')
            if not check_udp_port_available(server_port):
                raise ProxyError(f'Udp port({server_port}) can not be bind now.')
            self.udp_maps[server_port] = {
                UdpMapInfo.SERVER_PORT: server_port,
                UdpMapInfo.CLIENT_PORT: client_port,
                UdpMapInfo.UDP_SOCKET: None,
                UdpMapInfo.TASK_TRANSFER: None,
                UdpMapInfo.SWITCH: False,
                UdpMapInfo.STATISTIC: 0,
                UdpMapInfo.CREATE_TIME: datetime.utcnow(),
            }
        except ProxyError as e:
            log.error(e)
            log.warning(f'ADD_UDP_MAP: Server({server_port}) >>> {self.client_id}({client_port}) ... [ERROR]')
            return False
        else:
            log.success(f'ADD_UDP_MAP: Server({server_port}) >>> {self.client_id}({client_port}) ... [OK]')
            return True

    async def start_udp_map(self, server_port):
        client_port = None
        try:
            if server_port in self.udp_maps:
                client_port = self.udp_maps[server_port][UdpMapInfo.CLIENT_PORT]
            else:
                raise ProxyError(f'Udp port({server_port}) is not registered in proxy map.')
            if not check_udp_port_available(server_port):
                raise ProxyError(f'Udp port({server_port}) can not be bind now.')
            udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            udp_sock.setblocking(False)
            udp_sock.bind((self.server_host, server_port))
            self.udp_maps[server_port][UdpMapInfo.UDP_SOCKET] = udp_sock
            self.udp_maps[server_port][UdpMapInfo.TASK_TRANSFER] = \
                asyncio.create_task(self.__udp_service_task(server_port))
            self.udp_maps[server_port][UdpMapInfo.SWITCH] = True
        except ProxyError as e:
            log.error(e)
            log.warning(f'START_UDP_MAP: Server({server_port}) >>> {self.client_id}({client_port}) ... [ERROR]')
            return False
        else:
            log.success(f'START_UDP_MAP: Server({server_port}) >>> {self.client_id}({client_port}) ... [OK]')
            return True

    async def stop_udp_map(self, server_port):
        client_port = None
        try:
            if server_port in self.udp_maps:
                client_port = self.udp_maps[server_port][UdpMapInfo.CLIENT_PORT]
            else:
                raise ProxyError(f'Udp port({server_port}) is not registered in proxy map.')
            if self.udp_maps[server_port][UdpMapInfo.SWITCH]:
                self.udp_maps[server_port][UdpMapInfo.UDP_SOCKET].close()
                self.udp_maps[server_port][UdpMapInfo.TASK_TRANSFER].cancel()
            self.udp_maps[server_port][UdpMapInfo.SWITCH] = False
        except ProxyError as e:
            log.error(e)
            log.warning(f'STOP_UDP_MAP: Server({server_port}) >>> {self.client_id}({client_port}) ... [ERROR]')
            return False
        else:
            log.success(f'STOP_UDP_MAP: Server({server_port}) >>> {self.client_id}({client_port}) ... [OK]')
            return True

    async def remove_udp_map(self, server_port):
        client_port = None
        try:
            if server_port in self.udp_maps:
                client_port = self.udp_maps[server_port][UdpMapInfo.CLIENT_PORT]
            else:
                raise ProxyError(f'Udp port({server_port}) is not registered in proxy map.')
            if self.udp_maps[server_port][UdpMapInfo.SWITCH] and not await self.stop_udp_map(server_port):
                raise ProxyError(f'Udp port({server_port}) can not be removed as failed to stop it.')
            del self.udp_maps[server_port]
        except ProxyError as e:
            log.error(e)
            log.warning(f'REMOVE_UDP_MAP: Server({server_port}) >>> {self.client_id}({client_port}) ... [ERROR]')
            return False
        else:
            log.success(f'REMOVE_UDP_MAP: Server({server_port}) >>> {self.client_id}({client_port}) ... [OK]')
            return True

    async def __process_conn_task(self):
        while True:
            server_port, reader, writer = await self.__conn_queue.get()
            self.__conn_queue.task_done()

            if server_port in self.tcp_maps and self.tcp_maps[server_port][TcpMapInfo.SWITCH]:
                client_port = self.tcp_maps[server_port][TcpMapInfo.CLIENT_PORT]
            else:
                writer.close()
                continue

            try:
                conn_uuid = await self.__protocol.request(
                    Protocol.ServerCommand.ADD_TCP_CONNECTION, client_port, timeout=Protocol.CONNECTION_TIMEOUT)
            except ProtocolError as e:
                log.warning(e)
                continue

            wait_time = 0
            while wait_time < Protocol.CONNECTION_TIMEOUT:
                if conn_uuid in self.tcp_maps[server_port][TcpMapInfo.CONN_POOL]:
                    break
                wait_time += 0.1
                await asyncio.sleep(0.1)
            if conn_uuid not in self.tcp_maps[server_port][TcpMapInfo.CONN_POOL]:
                log.warning(f'Wait for client backing connection timeout: '
                            f'Client({self.client_id}), ClientPort({client_port}), ServerPort({server_port}).')
                continue

            reader2, writer2 = self.tcp_maps[server_port][TcpMapInfo.CONN_POOL][conn_uuid]
            del self.tcp_maps[server_port][TcpMapInfo.CONN_POOL][conn_uuid]

            asyncio.create_task(self.__tcp_data_relay_task(server_port, (reader, writer), (reader2, writer2)))
            log.info(f'Server({server_port}) ----> {self.client_id}({client_port})')

    async def __serve_request_task(self):
        while True:
            uuid, cmd, data = await self.__protocol.get_request()
            result = None
            try:
                if cmd == Protocol.Command.PING:
                    result = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

                elif cmd == Protocol.ClientCommand.CHECK_TCP_PORT:
                    port = int(data)
                    result = Protocol.Result.SUCCESS if check_listening('', port) else Protocol.Result.ERROR

                elif cmd == Protocol.ClientCommand.CHECK_UDP_PORT:
                    port = int(data)
                    result = Protocol.Result.SUCCESS if check_udp_port_available(port) else Protocol.Result.ERROR

                elif cmd == Protocol.ClientCommand.ADD_TCP_MAP:
                    str_pair = data.split(':')
                    c = int(str_pair[0])
                    s = int(str_pair[1])
                    result = Protocol.Result.SUCCESS if await self.add_tcp_map(s, c) else Protocol.Result.ERROR

                elif cmd == Protocol.ClientCommand.START_TCP_MAP:
                    port = int(data)
                    result = Protocol.Result.SUCCESS if await self.start_tcp_map(port) else Protocol.Result.ERROR

                elif cmd == Protocol.ClientCommand.STOP_TCP_MAP:
                    port = int(data)
                    result = Protocol.Result.SUCCESS if await self.stop_tcp_map(port) else Protocol.Result.ERROR

                elif cmd == Protocol.ClientCommand.REMOVE_TCP_MAP:
                    port = int(data)
                    result = Protocol.Result.SUCCESS if await self.remove_tcp_map(port) else Protocol.Result.ERROR

                elif cmd == Protocol.ClientCommand.ADD_UDP_MAP:
                    str_pair = data.split(':')
                    c = int(str_pair[0])
                    s = int(str_pair[1])
                    result = Protocol.Result.SUCCESS if await self.add_udp_map(s, c) else Protocol.Result.ERROR

                elif cmd == Protocol.ClientCommand.START_UDP_MAP:
                    port = int(data)
                    result = Protocol.Result.SUCCESS if await self.start_udp_map(port) else Protocol.Result.ERROR

                elif cmd == Protocol.ClientCommand.STOP_UDP_MAP:
                    port = int(data)
                    result = Protocol.Result.SUCCESS if await self.stop_udp_map(port) else Protocol.Result.ERROR

                elif cmd == Protocol.ClientCommand.REMOVE_UDP_MAP:
                    port = int(data)
                    result = Protocol.Result.SUCCESS if await self.remove_udp_map(port) else Protocol.Result.ERROR

                elif cmd == Protocol.ClientCommand.PAUSE_PROXY:
                    result = Protocol.Result.SUCCESS if await self.pause_client() else Protocol.Result.ERROR

                elif cmd == Protocol.ClientCommand.RESUME_PROXY:
                    result = Protocol.Result.SUCCESS if await self.resume_client() else Protocol.Result.ERROR

                elif cmd == Protocol.ClientCommand.DISCONNECT:
                    result = Protocol.Result.SUCCESS if await self.close_client() else Protocol.Result.ERROR

                else:
                    raise ValueError
            except ProxyError as e:
                result = Protocol.Result.ERROR
                log.error(f'Request "{Protocol.make_req(uuid, cmd, data)}" '
                          f'from Client(CID:{self.client_id}) error: {str(e)}')
            except (ValueError, Exception) as e:
                result = Protocol.Result.INVALID
                log.warning(f'Request "{Protocol.make_req(uuid, cmd, data)}" '
                            f'from Client(CID:{self.client_id}) is invalid: {str(e)}')
            finally:
                await self.__protocol.send_response(uuid, cmd, result)

    async def __watchdog_task(self):
        await self.__protocol.wait_closed()

        for server_port in list(self.tcp_maps.keys()):
            await self.remove_tcp_map(server_port)
        for server_port in list(self.udp_maps.keys()):
            await self.remove_udp_map(server_port)
        while not self.__conn_queue.empty():
            stream = self.__conn_queue.get_nowait()
            stream[1].close()
            self.__conn_queue.task_done()

        self.__task_serve_req.cancel()
        self.__task_process_conn.cancel()

        self.status = _Status.STOPPED
        log.warning(f'Watchdog is activated in {self.client_name}(CID:{self.client_id}), '
                    f'and target client will soon be removed ...')

    async def pause_client(self):
        log.warning(f'Preparing to Pause Client: {self.client_name}(CID:{self.client_id}) ...')
        result = True
        if self.status is _Status.STOPPED:
            return False
        elif self.status is _Status.PENDING:
            return True
        for server_port in self.tcp_maps:
            result &= await self.stop_tcp_map(server_port)
        for server_port in self.udp_maps:
            result &= await self.stop_udp_map(server_port)
        self.status = _Status.PENDING
        return result

    async def resume_client(self):
        log.warning(f'Preparing to Resume Client: {self.client_name}(CID:{self.client_id}) ...')
        result = True
        if self.status is _Status.STOPPED:
            return False
        elif self.status is _Status.RUNNING:
            return True
        for server_port in self.tcp_maps:
            result &= await self.start_tcp_map(server_port)
        for server_port in self.udp_maps:
            result &= await self.start_udp_map(server_port)
        self.status = _Status.RUNNING
        return result

    async def close_client(self):
        log.warning(f'Preparing to Close Client: {self.client_name}(CID:{self.client_id}) ...')
        self.__protocol.close()
        while self.status != _Status.STOPPED:
            await asyncio.sleep(Protocol.TASK_SCHEDULE_PERIOD)
        return True


class ProxyServer:
    """端口代理转发客户端连接管理类
    """
    TCP_QUEUE_SIZE = 500

    def __init__(self, host='0.0.0.0', port=10000, sid=None, name=None):
        if not sid:
            sid = uuid4().hex[-Protocol.DEFAULT_SID_LENGTH:]
        if not name:
            name = 'Main Proxy Server'
        check_python_version(3, 7)

        self.server_id = sid                    #
        self.server_name = name                 #
        self.server_host = host                 # '0.0.0.0'
        self.server_port = port                 #

        self.__clients = {}                     # {client_id: {...}, ...}
        self.__conn_queue = None                # {(reader, writer), ...}
        self.__udp_socket = None                # socket for udp packet relay

        self.__task_tcp_service = None          #
        self.__task_process_conn = None         #
        self.__task_dispatch_datagram = None    #
        self.__task_daemon = None               #
        self.__task_watchdog = None             #

        self.status = _Status.PREPARE           # 运行状态，启动或关闭
        self.timestamp = datetime.utcnow()      # 服务创建时间

    async def __tcp_service_handler(self, reader, writer):
        await self.__conn_queue.put((reader, writer))

    async def __tcp_service_task(self):
        server = await asyncio.start_server(
            self.__tcp_service_handler, self.server_host, self.server_port)
        async with server:
            await server.wait_closed()

    async def __process_conn_task(self):

        async def stream_readline(stream_reader):
            return await stream_reader.readline()

        while True:
            reader, writer = await self.__conn_queue.get()
            self.__conn_queue.task_done()
            try:
                task = asyncio.create_task(stream_readline(reader))
                await asyncio.wait_for(task, timeout=Protocol.CONNECTION_TIMEOUT)
                identify = task.result().decode().strip()
                log.debug(identify)
            except (asyncio.TimeoutError, ConnectionError):
                writer.close()
                continue
            try:
                conn_type, data = identify.split(':', 1)
                val_list = data.split('**')

                if conn_type == Protocol.ConnectionType.MANAGER:
                    log.warning('Still unsupported yet.')
                    raise NotImplementedError
                elif conn_type == Protocol.ConnectionType.PROXY_CLIENT:
                    client_name = val_list[0]
                    while True:
                        client_id = uuid4().hex[-Protocol.CID_LENGTH:].upper()
                        if client_id not in self.__clients:
                            break
                    if not await self.__add_client(client_id, client_name, (reader, writer)):
                        raise ProxyError
                    writer.write((Protocol.Result.SUCCESS + ':' + client_id + '\n').encode())
                elif conn_type == Protocol.ConnectionType.PROXY_TCP_DATA:
                    client_id = val_list[0]
                    server_port = int(val_list[1])
                    conn_id = val_list[2]
                    if client_id not in self.__clients:
                        raise ProxyError
                    client = self.__clients[client_id]
                    client.tcp_maps[server_port][TcpMapInfo.CONN_POOL][conn_id] = (reader, writer)
                    writer.write((Protocol.Result.SUCCESS + '\n').encode())
                else:
                    log.error(f'Invalid connection with identify "{identify}"')
                    raise ProtocolError
            except (ProtocolError, ProxyError, Exception):
                writer.write((Protocol.Result.ERROR + '\n').encode())
                await writer.drain()
                writer.close()
            else:
                await writer.drain()

    async def __dispatch_datagram_task(self):
        while True:
            try:
                data, address = self.__udp_socket.recvfrom(Protocol.SOCKET_BUFFER_SIZE)
            except BlockingIOError:
                await asyncio.sleep(Protocol.TASK_SCHEDULE_PERIOD)
                continue
            pkt_type, values = Protocol.parse_datagram_header(data)
            if pkt_type == Protocol.UdpPacketType.SYNC:
                cid = values[0]
                timestamp = values[1]
                if cid in self.__clients:
                    self.__clients[cid].udp_e_address = address
                    pkt = Protocol.make_datagram_header(Protocol.UdpPacketType.SYNC, info=self.server_id)
                    self.__udp_socket.sendto(pkt, address)
                log.debug(f'Received UDP ping packet from client(CID:{cid}) at: {timestamp}')
            elif pkt_type == Protocol.UdpPacketType.DATA:
                server_port = values[0]
                user_address = values[1]
                cid = None
                for c in self.__clients:
                    if server_port in self.__clients[c].udp_maps:
                        cid = c
                        break
                if cid and self.__clients[cid].udp_maps[server_port][UdpMapInfo.SWITCH]:
                    udp_socket = self.__clients[cid].udp_maps[server_port][UdpMapInfo.UDP_SOCKET]
                    udp_socket.sendto(data[Protocol.UDP_DATA_HEADER_LEN:], user_address)
                    self.__clients[cid].udp_maps[server_port][UdpMapInfo.STATISTIC] += len(data)
                    log.debug(f'Forwards udp data packet from port({server_port}) to {user_address}')
                else:
                    log.warning(f'Drops orphan udp packet on port:{server_port}, user:{user_address}')
            else:
                log.warning(f'Unrecognised udp packet form {address}')
                continue

    async def __daemon_task(self):
        while True:
            #
            # TASK: Remove dead clients.
            #
            for client_id in list(self.__clients.keys()):
                if self.__clients[client_id].status == _Status.STOPPED:
                    await self.remove_client(client_id)
            #
            # Every loop comes with a rest.
            #
            await asyncio.sleep(1)

    async def __watchdog_task(self):

        done, pending = await asyncio.wait({
            self.__task_tcp_service,
            self.__task_process_conn,
            self.__task_dispatch_datagram,
            self.__task_daemon,
        }, return_when=asyncio.FIRST_COMPLETED)

        for task in done:
            log.critical(task)
        for task in pending:
            task.cancel()

        for c in list(self.__clients):
            await self.remove_client(c)
        while not self.__conn_queue.empty():
            stream = self.__conn_queue.get_nowait()
            stream[1].close()
            self.__conn_queue.task_done()
        self.__udp_socket.close()

        self.status = _Status.STOPPED
        raise ProxyError('Watchdog will terminate the Proxy Server!')

    async def start_tcp_map(self, server_port):
        for c in self.__clients:
            if server_port in self.__clients[c].tcp_maps:
                return await self.__clients[c].start_tcp_map(server_port)
        return None

    async def stop_tcp_map(self, server_port):
        for c in self.__clients:
            if server_port in self.__clients[c].tcp_maps:
                return await self.__clients[c].stop_tcp_map(server_port)
        return None

    async def remove_tcp_map(self, server_port):
        for c in self.__clients:
            if server_port in self.__clients[c].tcp_maps:
                return await self.__clients[c].remove_tcp_map(server_port)
        return None

    async def start_udp_map(self, server_port):
        for c in self.__clients:
            if server_port in self.__clients[c].udp_maps:
                return await self.__clients[c].start_udp_map(server_port)
        return None

    async def stop_udp_map(self, server_port):
        for c in self.__clients:
            if server_port in self.__clients[c].udp_maps:
                return await self.__clients[c].stop_udp_map(server_port)
        return None

    async def remove_udp_map(self, server_port):
        for c in self.__clients:
            if server_port in self.__clients[c].udp_maps:
                return await self.__clients[c].remove_udp_map(server_port)
        return None

    async def __add_client(self, client_id, client_name, sock_stream):
        msg = f'ADD_CLIENT: {client_name}(CID:{client_id}) ... '
        if len(client_id) != Protocol.CID_LENGTH:
            log.error(msg + '[ERROR] (the given client id format is invalid.)')
            return None
        if client_id in self.__clients:
            log.error(msg + '[ERROR] (client id already exists.)')
            return None
        self.__clients[client_id] = _PeerClient(self.server_host, sock_stream, client_id, client_name)
        log.success(msg + '[OK]')
        return self.__clients[client_id]

    async def pause_client(self, client_id):
        msg = f'PAUSE_CLIENT: {self.__clients[client_id].client_name}(CID:{client_id}) ... '
        if client_id in self.__clients:
            result = await self.__clients[client_id].pause_client()
            log.success(msg + '[OK]')
            return result
        else:
            log.error(msg + '[ERROR] (client id not found.)')
            return None

    async def resume_client(self, client_id):
        msg = f'RESUME_CLIENT: {self.__clients[client_id].client_name}(CID:{client_id}) ... '
        if client_id in self.__clients:
            result = await self.__clients[client_id].resume_client()
            log.success(msg + '[OK]')
            return result
        else:
            log.error(msg + '[ERROR] (client id not found.)')
            return None

    async def remove_client(self, client_id):
        msg = f'REMOVE_CLIENT: {self.__clients[client_id].client_name}(CID:{client_id}) ... '
        if client_id in self.__clients:
            if self.__clients[client_id].status != _Status.STOPPED:
                await self.__clients[client_id].close_client()
            del self.__clients[client_id]
            log.success(msg + '[OK]')
            return client_id
        else:
            log.error(msg + 'ERROR (client id not found.)')
            return None

    async def startup_service(self):
        self.__conn_queue = asyncio.Queue(ProxyServer.TCP_QUEUE_SIZE)
        self.__udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.__udp_socket.setblocking(False)
        self.__udp_socket.bind((self.server_host, self.server_port))

        self.__task_tcp_service = asyncio.create_task(self.__tcp_service_task())
        self.__task_process_conn = asyncio.create_task(self.__process_conn_task())
        self.__task_dispatch_datagram = asyncio.create_task(self.__dispatch_datagram_task())
        self.__task_daemon = asyncio.create_task(self.__daemon_task())
        self.__task_watchdog = asyncio.create_task(self.__watchdog_task())

        self.status = _Status.RUNNING
        log.info('>>>>>>>> Proxy Service is now STARTED !!! <<<<<<<<')
        log.info(f'Serving on {self.server_host}:{self.server_port}(TCP&UDP) ...')

        while self.status != _Status.STOPPED:
            await asyncio.sleep(Protocol.TASK_SCHEDULE_PERIOD)
        log.warning(self.__task_watchdog.exception())
        log.info('>>>>>>>> Proxy Service is now STOPPED !!! <<<<<<<<')

    async def shutdown_service(self):
        self.__task_daemon.cancel()  # cancel any running task can trigger watchdog
        while self.status != _Status.STOPPED:
            await asyncio.sleep(Protocol.TASK_SCHEDULE_PERIOD)

    def is_running(self):
        return self.status == _Status.RUNNING

    def reset_statistic(self):
        for c in self.__clients:
            for p in self.__clients[c].tcp_maps:
                self.__clients[c].tcp_maps[p][TcpMapInfo.STATISTIC] = 0
            for p in self.__clients[c].udp_maps:
                self.__clients[c].udp_maps[p][UdpMapInfo.STATISTIC] = 0
        log.success('Statistic of TCP and UDP data was reset.')

    def _get_server_info(self):
        tcp_statistic = udp_statistic = 0
        for c in self.__clients:
            for p in self.__clients[c].tcp_maps:
                tcp_statistic += self.__clients[c].tcp_maps[p][TcpMapInfo.STATISTIC]
            for p in self.__clients[c].udp_maps:
                udp_statistic += self.__clients[c].udp_maps[p][UdpMapInfo.STATISTIC]
        servers = {self.server_id: {
            "server_id": self.server_id,
            "server_name": self.server_name,
            "server_host": self.server_host,
            "server_port": self.server_port,
            "start_time": self.timestamp,
            "is_running": self.status is _Status.RUNNING,
            "clients_cnt": len(self.__clients),
            "tcp_statistic": stringify_bytes_val(tcp_statistic),
            "udp_statistic": stringify_bytes_val(udp_statistic),
            "total_statistic": stringify_bytes_val(tcp_statistic + udp_statistic),
        }}
        return servers

    def _get_clients(self, client_id=None):
        clients = {}
        if client_id and client_id not in self.__clients:
            return clients
        for c in (client_id,) if client_id else self.__clients:
            tcp_statistic = udp_statistic = 0
            tcp_ports = {}
            udp_ports = {}
            for p in self.__clients[c].tcp_maps:
                tcp_ports[p] = {
                    "client_port": self.__clients[c].tcp_maps[p]["client_port"],
                    "is_running": self.__clients[c].tcp_maps[p]["is_running"],
                    "statistic": self.__clients[c].tcp_maps[p]["statistic"],
                    "create_time": self.__clients[c].tcp_maps[p]["create_time"],
                    "conn_pool_size": len(self.__clients[c].tcp_maps[p]["connection_pool"]),
                }
                tcp_statistic += tcp_ports[p]["statistic"]
            for p in self.__clients[c].udp_maps:
                udp_ports[p] = {
                    "client_port": self.__clients[c].udp_maps[p]["client_port"],
                    "is_running": self.__clients[c].udp_maps[p]["is_running"],
                    "statistic": self.__clients[c].udp_maps[p]["statistic"],
                    "create_time": self.__clients[c].udp_maps[p]["create_time"],
                }
                udp_statistic += udp_ports[p]["statistic"]
            clients[c] = {
                "client_id": self.__clients[c].client_id,
                "client_name": self.__clients[c].client_name,
                "tcp_e_addr": self.__clients[c].tcp_e_address,
                "udp_e_addr": self.__clients[c].udp_e_address,
                "tcp_maps": tcp_ports,
                "udp_maps": udp_ports,
                "tcp_statistic": tcp_statistic,
                "udp_statistic": udp_statistic,
                "is_running": self.__clients[c].status is _Status.RUNNING,
                "start_time": self.__clients[c].timestamp,
            }
        return clients


def run_proxy_server(host, port, sid, name):
    server = ProxyServer(host, port, sid, name)
    asyncio.run(server.startup_service(), debug=False)


def run_proxy_server_with_web(host, port, sid, name, web_port):
    from threading import Thread
    from flask_app import create_app

    def start_loop(loop):
        asyncio.set_event_loop(loop)
        loop.run_forever()

    def stream_statistic():
        statistic = 0
        while True:
            current = 0
            tcp_maps = _flask_app.proxy_get_tcp_maps()
            for m in tcp_maps:
                current += tcp_maps[m][TcpMapInfo.STATISTIC]
            _flask_app.proxy_tcp_speed = current - statistic
            statistic = current
            time.sleep(1)

    def start_proxy_server():
        asyncio.run_coroutine_threadsafe(_proxy_server.startup_service(), _proxy_loop)
        time.sleep(1)

    def stop_proxy_server():
        try:
            asyncio.run_coroutine_threadsafe(_proxy_server.shutdown_service(), _proxy_loop).result(3)
        except asyncio.TimeoutError:
            log.error('stop_proxy_server timeout')

    def pause_client(client_id):
        try:
            result = asyncio.run_coroutine_threadsafe(_proxy_server.pause_client(client_id), _proxy_loop).result(3)
            if not result:
                log.error(f'pause_client returned `{result}`')
        except asyncio.TimeoutError:
            log.error('pause_client timeout')

    def resume_client(client_id):
        try:
            result = asyncio.run_coroutine_threadsafe(_proxy_server.resume_client(client_id), _proxy_loop).result(3)
            if not result:
                log.error(f'resume_client returned `{result}`')
        except asyncio.TimeoutError:
            log.error('resume_client timeout')

    def remove_client(client_id):
        try:
            result = asyncio.run_coroutine_threadsafe(_proxy_server.remove_client(client_id), _proxy_loop).result(3)
            if not result:
                log.error(f'remove_client returned `{result}`')
        except asyncio.TimeoutError:
            log.error('remove_client timeout')

    def start_tcp_map(server_port):
        try:
            result = asyncio.run_coroutine_threadsafe(_proxy_server.start_tcp_map(server_port), _proxy_loop).result(3)
            if not result:
                log.error(f'start_tcp_map returned `{result}`')
        except asyncio.TimeoutError:
            log.error('start_tcp_map timeout')

    def stop_tcp_map(server_port):
        try:
            result = asyncio.run_coroutine_threadsafe(_proxy_server.stop_tcp_map(server_port), _proxy_loop).result(3)
            if not result:
                log.error(f'stop_tcp_map returned `{result}`')
        except asyncio.TimeoutError:
            log.error('stop_tcp_map timeout')

    def remove_tcp_map(server_port):
        try:
            result = asyncio.run_coroutine_threadsafe(_proxy_server.remove_tcp_map(server_port), _proxy_loop).result(3)
            if not result:
                log.error(f'remove_tcp_map returned `{result}`')
        except asyncio.TimeoutError:
            log.error('remove_tcp_map timeout')

    def start_udp_map(server_port):
        try:
            result = asyncio.run_coroutine_threadsafe(_proxy_server.start_udp_map(server_port), _proxy_loop).result(3)
            if not result:
                log.error(f'start_udp_map returned `{result}`')
        except asyncio.TimeoutError:
            log.error('start_udp_map timeout')

    def stop_udp_map(server_port):
        try:
            result = asyncio.run_coroutine_threadsafe(_proxy_server.stop_udp_map(server_port), _proxy_loop).result(3)
            if not result:
                log.error(f'stop_udp_map returned `{result}`')
        except asyncio.TimeoutError:
            log.error('stop_udp_map timeout')

    def remove_udp_map(server_port):
        try:
            result = asyncio.run_coroutine_threadsafe(_proxy_server.remove_udp_map(server_port), _proxy_loop).result(3)
            if not result:
                log.error(f'remove_udp_map returned `{result}`')
        except asyncio.TimeoutError:
            log.error('remove_udp_map timeout')

    def reset_statistic(server_port):
        try:
            result = asyncio.run_coroutine_threadsafe(_proxy_server.reset_statistic(), _proxy_loop).result(3)
            if not result:
                log.error(f'reset_statistic returned `{result}`')
        except asyncio.TimeoutError:
            log.error('reset_statistic timeout')

    _flask_app = create_app()
    _proxy_loop = asyncio.new_event_loop()
    _proxy_server = ProxyServer(host, port, sid, name)

    _flask_app.proxy_get_clients = _proxy_server._get_clients
    _flask_app.proxy_get_server_info = _proxy_server._get_server_info
    _flask_app.proxy_tcp_speed = 0  # updates in thread `_thread_statistic`
    _flask_app.proxy_is_server_running = _proxy_server.is_running
    _flask_app.proxy_reset_statistic = _proxy_server.reset_statistic

    _flask_app.proxy_start_server = start_proxy_server
    _flask_app.proxy_stop_server = stop_proxy_server

    _flask_app.proxy_pause_client = pause_client
    _flask_app.proxy_resume_client = resume_client
    _flask_app.proxy_remove_client = remove_client
    _flask_app.proxy_start_tcp_map = start_tcp_map
    _flask_app.proxy_stop_tcp_map = stop_tcp_map
    _flask_app.proxy_remove_tcp_map = remove_tcp_map
    _flask_app.proxy_start_udp_map = start_udp_map
    _flask_app.proxy_stop_udp_map = stop_udp_map
    _flask_app.proxy_remove_udp_map = remove_udp_map

    _thread_proxy = Thread(target=start_loop, args=(_proxy_loop,))
    _thread_proxy.setDaemon(True)
    # _thread_statistic = Thread(target=stream_statistic)
    # _thread_statistic.setDaemon(True)
    _thread_proxy.start()
    # _thread_statistic.start()
    start_proxy_server()
    try:
        _flask_app.run(host=host, port=web_port, debug=False)
    except KeyboardInterrupt:
        stop_proxy_server()
        _proxy_loop.stop()


if __name__ == '__main__':
    import sys

    log.remove()
    log.add(sys.stderr, level="INFO")
    # log.add("any-proxy-server.log", level="INFO", rotation="1 month")

    _server_id = '10000000'
    _server_name = 'DemoServer'

    # run_proxy_server('0.0.0.0', 10000, _server_id, _server_name)
    run_proxy_server_with_web('0.0.0.0', 10000, _server_id, _server_name, web_port=9999)
