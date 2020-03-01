import asyncio
import struct
from datetime import datetime
from uuid import uuid4
from xml.etree import ElementTree
from loguru import logger as log


class ProtocolError(Exception):
    pass


class Protocol:
    """
    Data Transfer Protocol for each command:
    ----------------------------------------
        |
        |   [:: Common Commands ::]
        |
        |-- PING: Request('timestamp') >>> Response('timestamp')
        |
        |
        |    [:: Server Side Commands ::]
        |
        |-- ADD_TCP_CONNECTION:  Request('client_port') >>> Response('conn_uuid'/None/'Invalid')
        |
        |
        |   [:: Client Side Commands ::]
        |
        |-- CHECK_TCP_PORT: Request('server_port') >>> Response(True/False)
        |
        |-- CHECK_UDP_PORT: Request('server_port') >>> Response(True/False)
        |
        |-- ADD_TCP_MAP: Request('client_port:server_port') >>> Response('Success'/'Error'/'Invalid')
        |
        |-- PAUSE_TCP_MAP: Request('server_port') >>> Response('Success'/'Error'/'Invalid')
        |
        |-- RESUME_TCP_MAP: Request('server_port') >>> Response('Success'/'Error'/'Invalid')
        |
        |-- REMOVE_TCP_MAP: Request('server_port') >>> Response('Success'/'Error'/'Invalid')
        |
        |-- ADD_UDP_MAP: Request('client_port:server_port') >>> Response('Success'/'Error'/'Invalid')
        |
        |-- PAUSE_UDP_MAP: Request('server_port') >>> Response('Success'/'Error'/'Invalid')
        |
        |-- RESUME_UDP_MAP: Request('server_port') >>> Response('Success'/'Error'/'Invalid')
        |
        |-- REMOVE_UDP_MAP: Request('server_port') >>> Response('Success'/'Error'/'Invalid')
        |
        |-- PAUSE_PROXY: Request() >>> Response('Success'/'Error'/'Invalid')
        |
        |-- RESUME_PROXY: Request() >>> Response('Success'/'Error'/'Invalid')
        |
        `-- DISCONNECT: Request() >>> Response('Success'/'Error'/'Invalid')
    """
    DEFAULT_SID_LENGTH = 8                      # 服务器SID长度
    CID_LENGTH = 8                              # 客户端CID长度
    PARAM_SEPARATOR = '**'                      # 数据项之间的分隔符
    SOCKET_BUFFER_SIZE = 65536  # 1400                   # TODO:套接字socket缓存大小。目前的UDP封包设计可能导致性能下降。
    TASK_SCHEDULE_PERIOD = 0.001                # 任务挂起等待的时间粒度
    UDP_REQUEST_DURATION = 9000                 # UDP请求的记录保留时效(2.5h)
    CLIENT_TCP_PING_PERIOD = 30                 # 客户端TCP保活探测时间
    CLIENT_UDP_PING_PERIOD = 5                  # 客户端UDP保活时间
    UDP_UNREACHABLE_WARNING_PERIOD = 16         # 打印无法接收UDP PING包日志的超时时间
    CONNECTION_TIMEOUT = 30                     # TCP连接超时时间
    MAX_RETRY_PERIOD = 60                       # 客户端在断开重连的情况下，最大重连时间间隔

    class ConnectionType:
        """Connection type."""
        MANAGER = 'Manager'
        PROXY_CLIENT = 'ProxyClient'
        PROXY_TCP_DATA = 'ProxyTcpData'

    class Command:
        """Common command used by both server and client."""
        PING = 'Ping'

    class ServerCommand:
        """Used by server to control client."""
        ADD_TCP_CONNECTION = 'AddTcpConnection'

    class ClientCommand:
        """Used by client to communicate with server."""
        CHECK_TCP_PORT = 'CheckTcpPort'         # 检查TCP服务是否开启
        CHECK_UDP_PORT = 'CheckUdpPort'         # 检查UDP端口是否可用
        ADD_TCP_MAP = 'AddTcpMap'
        PAUSE_TCP_MAP = 'PauseTcpMap'
        RESUME_TCP_MAP = 'ResumeTcpMap'
        REMOVE_TCP_MAP = 'RemoveTcpMap'
        ADD_UDP_MAP = 'AddUdpMap'
        PAUSE_UDP_MAP = 'PauseUdpMap'
        RESUME_UDP_MAP = 'ResumeUdpMap'
        REMOVE_UDP_MAP = 'RemoveUdpMap'
        PAUSE_PROXY = 'PauseProxy'
        RESUME_PROXY = 'ResumeProxy'
        DISCONNECT = 'Disconnect'

    class Result:
        """Standard function call result."""
        ERROR = 'Error'
        SUCCESS = 'Success'
        INVALID = 'Invalid'
        UNKNOWN = 'Unknown'

    def __init__(self, tcp_stream=None):
        self.__reader = tcp_stream[0]
        self.__writer = tcp_stream[1]

        # request queue format: [(uuid, cmd, data), ...]
        self.__request_queue = asyncio.Queue()
        # response pool format: {uuid: (cmd, data), ...}
        self.__response_pool = {}

        self.__status = False
        self.__task_recv_msg = asyncio.create_task(self.__recv_msg_task())

        self.tcp_statistic = [0, 0]             # TCP上下行流量
        self.udp_statistic = [0, 0]             # UDP上下行流量

    @staticmethod
    def make_req(uuid, key, val):
        return f'<request uuid="{uuid}" type="{key}">{val if val else ""}</request>'

    @staticmethod
    def make_resp(uuid, key, val):
        return f'<response uuid="{uuid}" type="{key}">{val if val else ""}</response>'

    @staticmethod
    def parse_msg(msg):
        root = ElementTree.fromstring(msg)
        return root.tag, root.attrib['uuid'], root.attrib['type'], root.text

    async def __recv_msg_task(self):
        self.__status = True
        try:
            while True:
                msg = await self.__reader.readline()

                if msg:
                    self.tcp_statistic[1] += len(msg)
                else:
                    remote = self.__writer.get_extra_info('peername')
                    raise ConnectionError(f'Protocol connection with {remote} is closed or broken.')

                try:
                    msg = msg.decode().strip()
                    log.debug(msg)
                    msg_type, uuid, key, val = Protocol.parse_msg(msg)
                except Exception as e:
                    log.warning(f'protocol error while parsing {msg}: {e}')
                    continue

                if msg_type == 'request':
                    await self.__request_queue.put((uuid, key, val))
                elif msg_type == 'response':
                    self.__response_pool[uuid] = (key, val)
                else:
                    log.warning(f'can not recognise message type of: {msg}')
        except Exception as e:
            log.error(e)
            self.__writer.close()
            self.__status = False

    def close(self):
        self.__task_recv_msg.cancel()
        self.__writer.close()
        self.__status = False

    async def wait_closed(self):
        # TODO: WARNING: 此函数直接写成 `await self.__task_recv_msg` 会导致调用异常，目前不知道原因。
        # try:
        #     await self.__task_recv_msg
        # except asyncio.CancelledError:
        #     self.__writer.close()
        #     self.__status = False
        while self.__status:
            await asyncio.sleep(Protocol.TASK_SCHEDULE_PERIOD)

    def is_healthy(self):
        return self.__status

    async def send_request(self, uuid, cmd, data=None):
        """
        send request
        :param uuid:
        :param cmd:
        :param data:
        :return:
        @throws: ConnectionError(IOError)
        """
        try:
            req = Protocol.make_req(uuid, cmd, data)
            b_req = (req + '\n').encode()
            self.__writer.write(b_req)
            await self.__writer.drain()
            self.tcp_statistic[0] += len(b_req)
            log.debug(req)
        except Exception:
            self.close()
            raise

    async def send_response(self, uuid, cmd, data=None):
        """
        send response
        :param uuid:
        :param cmd:
        :param data:
        :return:
        @throws: ConnectionError(IOError)
        """
        try:
            resp = Protocol.make_resp(uuid, cmd, data)
            b_resp = (resp + '\n').encode()
            self.__writer.write(b_resp)
            await self.__writer.drain()
            self.tcp_statistic[0] += len(b_resp)
            # self.__request_queue.task_done()
            log.debug(resp)
        except Exception:
            self.close()
            raise

    async def get_request(self, timeout=None):
        """
        get request
        :param timeout:
        :return:
        @throws: BlockingIOError, asyncio.TimeoutError
        """
        if not self.__request_queue.empty():
            req = self.__request_queue.get_nowait()
            self.__request_queue.task_done()
            return req
        elif timeout == 0:
            raise BlockingIOError(f'Protocol failed to get request as no request arrived.')
        wait_time = 0
        while True:
            await asyncio.sleep(Protocol.TASK_SCHEDULE_PERIOD)
            wait_time += Protocol.TASK_SCHEDULE_PERIOD
            if not self.__request_queue.empty():
                req = self.__request_queue.get_nowait()
                self.__request_queue.task_done()
                return req
            if timeout is None or timeout < 0:
                continue
            if wait_time >= timeout:
                raise asyncio.TimeoutError(f'Protocol get request timeout after waiting {timeout}s.')

    async def get_response(self, uuid, timeout=None):
        """
        get response
        :param uuid:
        :param timeout:
        :return:
        @throws: BlockingIOError, asyncio.TimeoutError
        """
        if uuid in self.__response_pool:
            resp = self.__response_pool[uuid]
            del self.__response_pool[uuid]
            return resp
        elif timeout == 0:
            raise BlockingIOError(f'Protocol failed to get response with uuid={uuid}.')
        wait_time = 0
        while True:
            await asyncio.sleep(Protocol.TASK_SCHEDULE_PERIOD)
            wait_time += Protocol.TASK_SCHEDULE_PERIOD
            if uuid in self.__response_pool:
                resp = self.__response_pool[uuid]
                del self.__response_pool[uuid]
                return resp
            if timeout is None or timeout < 0:
                continue
            if wait_time >= timeout:
                raise asyncio.TimeoutError(f'Protocol get response timeout after waiting {timeout}s.')

    async def request(self, cmd, data=None, timeout=None):
        """
        make request
        :param cmd:
        :param data:
        :param timeout:
        :return:
        @throws: IOError, asyncio.TimeoutError, ProtocolError
        """
        req_uuid = uuid4().hex
        await self.send_request(req_uuid, cmd, data)
        cmd2, data2 = await self.get_response(req_uuid, timeout)
        if cmd2 == cmd:
            return data2
        else:
            req = Protocol.make_req(req_uuid, cmd, data)
            resp = Protocol.make_resp(req_uuid, cmd2, data2)
            raise ProtocolError(f'Send: {req}; Receive: {resp}.')

    """
    Frame format of UDP Protocol Data Relay.
    
    Case of UdpPacketType.SYNC:
        0        8       16       24       32       40       48       56       64
        +--------+--------+--------+--------+--------+--------+--------+--------+
        |    SYNC_FLAG    |           PACKET_LENGTH           |       CRC       |
        +--------+--------+--------+--------+--------+--------+--------+--------+
        |                                                                       |
        |                     INFO(CLIENT_ID), TIMESTAMP ...                    |
        |                                                                       |
        +--------+--------+--------+--------+--------+--------+--------+--------+
    
    Case of UdpPacketType.DATA:
        0        8       16       24       32       40       48       56       64
        +--------+--------+--------+--------+--------+--------+--------+--------+
        |    DATA_FLAG    |           PACKET_LENGTH           |       CRC       |
        +--------+--------+--------+--------+--------+--------+--------+--------+
        |   PROXY_PORT    |             USER_HOST             |    USER_PORT    |
        +--------+--------+--------+--------+--------+--------+--------+--------+
        |                                                                       |
        |                              USER DATA ...                            |
        |                                                                       |
        +--------+--------+--------+--------+--------+--------+--------+--------+
    """
    UDP_SYNC_HEADER_LEN = 8
    UDP_DATA_HEADER_LEN = 16
    __UDP_SYNC_FLAG = b'\x53\x59'  # 'SY'
    __UDP_DATA_FLAG = b'\x44\x41'  # 'DA'

    class UdpPacketType:
        SYNC = 'SYNC'
        DATA = 'DATA'
        UNKNOWN = 'UNKNOWN'

    class UdpPacketInfo:
        TYPE = 'type'
        TIMESTAMP = 'timestamp'
        SERVER_PORT = 'server_port'
        USER_ADDRESS = 'user_address'
        B_USER_DATA = 'b_user_data'

    @staticmethod
    def pack_udp_sync_packet(**kwargs):  # raise: struct.error
        """
        合成udp sync二进制数据包。
        :param kwargs: 用户需要传递的参数
        :return: udp sync packet
        @throws: ProtocolError, struct.error
        """
        kwargs[Protocol.UdpPacketInfo.TIMESTAMP] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        val_list = (f'{k}={kwargs[k]}' for k in kwargs)
        b_data = Protocol.PARAM_SEPARATOR.join(val_list).encode()
        length = Protocol.UDP_SYNC_HEADER_LEN + len(b_data)
        b_len = struct.pack('>I', length)  # length.to_bytes(length=4, byteorder='big', signed=False)
        b_crc = b'\x55\x55'
        return b''.join((Protocol.__UDP_SYNC_FLAG, b_len, b_crc, b_data))

    @staticmethod
    def pack_udp_data_packet(server_port, user_address, b_user_data, pack_data=True):
        """
        合成udp data二进制数据包。
        :param server_port:
        :param user_address:
        :param b_user_data:
        :param pack_data:
        :return: udp data packet
        @throws: ProtocolError, struct.error
        """
        user_host = user_address[0] if user_address[0] else '127.0.0.1'
        user_port = user_address[1]
        if len(user_host.split('.')) != 4:
            raise ProtocolError(f'Invalid ip address of user_host({user_host}) is given.')
        b_proxy_port = struct.pack('>H', server_port)
        b_user_host = bytes(map(int, user_host.split('.')))
        b_user_port = struct.pack('>H', user_port)
        b_len = struct.pack('>I', Protocol.UDP_DATA_HEADER_LEN + len(b_user_data))
        b_crc = b'\x55\x55'
        return b''.join((Protocol.__UDP_DATA_FLAG, b_len, b_crc, b_proxy_port, b_user_host, b_user_port, b_user_data)) \
            if pack_data else b''.join((Protocol.__UDP_DATA_FLAG, b_len, b_crc, b_proxy_port, b_user_host, b_user_port))

    @staticmethod
    def unpack_udp_packet(packet, unpack_data=False):
        """
        将二进制数据包解析成字典参数返回。
        :param packet:
        :param unpack_data:
        :return: dict result
        @throws: ProtocolError, struct.error
        """
        result = {Protocol.UdpPacketInfo.TYPE: Protocol.UdpPacketType.UNKNOWN}

        len_packet = len(packet)
        if len_packet != struct.unpack('>I', packet[2:6])[0]:
            raise ProtocolError('Checking LENGTH of udp packet failed.')
        if packet[6:8] != b'\x55\x55':
            raise ProtocolError('Checking CRC of udp data packet failed.')

        if packet[0:2] == Protocol.__UDP_SYNC_FLAG:
            if len_packet < Protocol.UDP_SYNC_HEADER_LEN:
                raise ProtocolError('Checking LENGTH of udp sync packet failed.')
            val_list = packet[Protocol.UDP_SYNC_HEADER_LEN:].decode().split(Protocol.PARAM_SEPARATOR)
            for val in val_list:
                val_pair = val.split('=', 1)
                if len(val_pair) != 2:
                    raise ProtocolError('Invalid parameter in udp sync packet.')
                result[val_pair[0]] = val_pair[1]
            result[Protocol.UdpPacketInfo.TYPE] = Protocol.UdpPacketType.SYNC
        elif packet[0:2] == Protocol.__UDP_DATA_FLAG:
            if len_packet < Protocol.UDP_DATA_HEADER_LEN:
                raise ProtocolError('Checking LENGTH of udp data packet failed.')
            result[Protocol.UdpPacketInfo.SERVER_PORT] = struct.unpack('>H', packet[8:10])[0]
            result[Protocol.UdpPacketInfo.USER_ADDRESS] = (
                '.'.join(str(x) for x in list(packet[10:14])), struct.unpack('>H', packet[14:16])[0])
            if unpack_data:
                result[Protocol.UdpPacketInfo.B_USER_DATA] = packet[Protocol.UDP_DATA_HEADER_LEN:]
            result[Protocol.UdpPacketInfo.TYPE] = Protocol.UdpPacketType.DATA
        else:
            raise ProtocolError('Unrecognised udp packet.')
        return result
