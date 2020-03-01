import asyncio
from datetime import datetime
from uuid import uuid4
from xml.etree import ElementTree
from loguru import logger as log


class ProtocolError(Exception):

    def __init__(self, msg='', cmd=None, data=None):
        err = '%s (command: %s, data: %s)' % (msg, cmd, data) if cmd is not None else msg
        Exception.__init__(self, err)
        self.cmd = cmd
        self.data = data


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
        |-- START_TCP_MAP: Request('server_port') >>> Response('Success'/'Error'/'Invalid')
        |
        |-- STOP_TCP_MAP: Request('server_port') >>> Response('Success'/'Error'/'Invalid')
        |
        |-- REMOVE_TCP_MAP: Request('server_port') >>> Response('Success'/'Error'/'Invalid')
        |
        |-- ADD_UDP_MAP: Request('client_port:server_port') >>> Response('Success'/'Error'/'Invalid')
        |
        |-- START_UDP_MAP: Request('server_port') >>> Response('Success'/'Error'/'Invalid')
        |
        |-- STOP_UDP_MAP: Request('server_port') >>> Response('Success'/'Error'/'Invalid')
        |
        |-- REMOVE_UDP_MAP: Request('server_port') >>> Response('Success'/'Error'/'Invalid')
        |
        |-- PAUSE_PROXY: Request('') >>> Response('Success'/'Error'/'Invalid')
        |
        |-- RESUME_PROXY: Request('') >>> Response('Success'/'Error'/'Invalid')
        |
        `-- DISCONNECT: Request('') >>> Response('Success'/'Error'/'Invalid')
    """
    DEFAULT_SID_LENGTH = 8                      # 服务器SID长度
    CID_LENGTH = 8                              # 客户端CID长度
    SOCKET_BUFFER_SIZE = 1500                   # 套接字socket缓存大小
    TASK_SCHEDULE_PERIOD = 0.2                  # 任务挂起等待的时间粒度
    UDP_REQUEST_DEADLINE = 1200                 # UDP请求的记录保留时效
    CLIENT_TCP_PING_PERIOD = 10                 # 客户端TCP保活时间
    CLIENT_UDP_PING_PERIOD = 10                 # 客户端UDP保活时间
    CONNECTION_TIMEOUT = 30                     # TCP连接超时时间
    MAX_RETRY_PERIOD = 60                       # 超时重连的情况下，最大重连间隔时间
    SEPARATOR = '**'                            # 数据项之间的分隔符

    __REQUEST_QUEUE_SIZE = 200                  #

    class Result:
        """Standard function call result."""
        ERROR = 'Error'
        SUCCESS = 'Success'
        INVALID = 'Invalid'
        UNKNOWN = 'Unknown'

    class Command:
        """Common command used by both server and client."""
        PING = 'Ping'

    class ServerCommand:
        """Used by server to control client."""
        ADD_TCP_CONNECTION = 'AddTcpConnection'

    class ClientCommand:
        """Used by client to communicate with server."""
        CHECK_TCP_PORT = 'CheckTcpPort'
        CHECK_UDP_PORT = 'CheckUdpPort'
        ADD_TCP_MAP = 'AddTcpMap'
        START_TCP_MAP = 'StartTcpMap'
        STOP_TCP_MAP = 'StopTcpMap'
        REMOVE_TCP_MAP = 'RemoveTcpMap'
        ADD_UDP_MAP = 'AddUdpMap'
        START_UDP_MAP = 'StartUdpMap'
        STOP_UDP_MAP = 'StopUdpMap'
        REMOVE_UDP_MAP = 'RemoveUdpMap'
        PAUSE_PROXY = 'PauseProxy'
        RESUME_PROXY = 'ResumeProxy'
        DISCONNECT = 'Disconnect'

    class ConnectionType:
        """Connection type."""
        MANAGER = 'Manager'
        PROXY_CLIENT = 'ProxyClient'
        PROXY_TCP_DATA = 'ProxyTcpData'

    class __XmlTag:
        """Standard xml tags."""
        REQUEST = 'request'
        RESPONSE = 'response'
        UUID = 'uuid'
        TYPE = 'type'
        NAME = 'name'

    @staticmethod
    def make_req(uuid, key, val):
        req_node = ElementTree.Element(Protocol.__XmlTag.REQUEST, attrib={Protocol.__XmlTag.UUID: uuid})
        cmd_node = ElementTree.SubElement(req_node, str(key))
        cmd_node.text = str(val) if val else ''
        return ElementTree.tostring(req_node, encoding='unicode')

    @staticmethod
    def make_resp(uuid, key, val):
        resp_node = ElementTree.Element(Protocol.__XmlTag.RESPONSE, attrib={Protocol.__XmlTag.UUID: uuid})
        cmd_node = ElementTree.SubElement(resp_node, str(key))
        cmd_node.text = str(val) if val else ''
        return ElementTree.tostring(resp_node, encoding='unicode')

    @staticmethod
    def parse_msg(message):
        root = ElementTree.fromstring(message)
        return root.tag, root.attrib[Protocol.__XmlTag.UUID], root[0].tag, root[0].text

    def __init__(self, tcp_stream=None):
        self.__reader = tcp_stream[0]
        self.__writer = tcp_stream[1]

        # request queue format: [(cid, cmd, data), ...]
        self.__request_queue = asyncio.Queue(Protocol.__REQUEST_QUEUE_SIZE)
        # response pool format: {cid: (cmd, data), ...}
        self.__response_pool = {}

        self.__status = False
        self.__task_recv_msg = asyncio.create_task(self.__recv_msg_task())

    async def __recv_msg_task(self):
        try:
            self.__status = True

            while True:
                message = await self.__reader.readline()  # This will block until tcp data arrived.

                if message:
                    message = message.decode().strip()
                else:
                    remote = self.__writer.get_extra_info('peername')
                    raise ProtocolError(f'Protocol connection with client {remote} is closed or broken.')

                try:
                    req_or_resp, cid, key, val = Protocol.parse_msg(message)
                except (KeyError, Exception):
                    continue

                if req_or_resp == Protocol.__XmlTag.REQUEST:
                    await self.__request_queue.put((cid, key, val))
                elif req_or_resp == Protocol.__XmlTag.RESPONSE:
                    self.__response_pool[cid] = (key, val)
                else:
                    log.warning(f'Received unrecognised message: {message}')
        finally:
            self.__writer.close()
            self.__status = False

    def is_healthy(self):
        return self.__status

    def close(self):
        self.__task_recv_msg.cancel()

    async def wait_closed(self):
        try:
            await self.__task_recv_msg
        except ProtocolError as e:
            log.error(e)
        except asyncio.CancelledError:
            self.__writer.close()
            self.__status = False

    async def get_request(self, timeout=None):
        """
        从指令池中取出一个指令（用于对指令进行处理和响应）。
        :param timeout:
        :return: 取出的指令，或者抛出'asyncio.TimeoutError'异常
        """
        async def wait_request(request_queue):
            return await request_queue.get()

        task = asyncio.create_task(wait_request(self.__request_queue))
        await asyncio.wait_for(task, timeout=timeout)
        req = task.result()
        log.debug(Protocol.make_req(req[0], req[1], req[2]))
        return req

    async def send_response(self, cid, cmd, data):
        """
        向指令请求方发送响应消息。
        :param cid:
        :param cmd:
        :param data:
        :return: 可能会抛出'ConnectionError'类的异常
        """
        log.debug(Protocol.make_resp(cid, cmd, data))
        try:
            self.__writer.write((Protocol.make_resp(cid, cmd, data) + '\n').encode())
            await self.__writer.drain()
        finally:
            self.__request_queue.task_done()
        return True

    async def request(self, cmd, data, timeout=None):
        """
        发送指令，并获取得到的响应结果。
        :param cmd:
        :param data:
        :param timeout:
        :return: 返回的结果，或者抛出'asyncio.TimeoutError'/'ProtocolError'异常
        """
        async def wait_response(response_pool, cid):
            while True:
                if cid in response_pool.keys():
                    break
                await asyncio.sleep(0.1)

        req_uuid = uuid4().hex
        req = Protocol.make_req(req_uuid, cmd, data)
        self.__writer.write((req + '\n').encode())
        await self.__writer.drain()
        log.debug(req)

        await asyncio.wait_for(wait_response(self.__response_pool, req_uuid), timeout)
        cmd2, data2 = self.__response_pool[req_uuid]
        del self.__response_pool[req_uuid]
        log.debug(Protocol.make_resp(req_uuid, cmd2, data2))

        if cmd2 == cmd:
            return data2
        else:
            raise ProtocolError(f'Requests "{req}" while responses "{Protocol.make_resp(req_uuid, cmd2, data2)}"')

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
    UDP_PACKET_HEADER_LEN = 8
    UDP_DATA_HEADER_LEN = 16
    __UDP_SYNC_FLAG = b'\x53\x59'  # 'SY'
    __UDP_DATA_FLAG = b'\x44\x41'  # 'DA'

    class UdpPacketType:
        UNKNOWN = 0
        SYNC = 1
        DATA = 2

    @staticmethod
    def make_datagram_header(pkt_type, **kwargs):
        """
        构造UDP数据包的包头，根据不同的包类型进行组装。
        :param pkt_type: UdpPacketType
        :param kwargs: UdpPacketType.SYNC(info); UdpPacketType.DATA(proxy_port, user_address, data_length)
        :return: bytes of datagram header
        """
        if pkt_type == Protocol.UdpPacketType.SYNC:
            if 'info' not in kwargs:
                raise ProtocolError('make_datagram_header without parameter `info` given')
            info = kwargs['info']
            timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            b_data = f'{info}{Protocol.SEPARATOR}{timestamp}'.encode()
            length = Protocol.UDP_PACKET_HEADER_LEN + len(b_data)
            b_len = length.to_bytes(length=4, byteorder='big', signed=False)
            b_crc = b'\x55\x55'
            return Protocol.__UDP_SYNC_FLAG + b_len + b_crc + b_data

        elif pkt_type == Protocol.UdpPacketType.DATA:
            for k in ('proxy_port', 'user_address', 'data_length'):
                if k not in kwargs:
                    raise ProtocolError('make_datagram_header without parameter `%s` given' % k)
            proxy_port = kwargs['proxy_port']
            user_host = kwargs['user_address'][0]
            user_port = kwargs['user_address'][1]
            data_length = kwargs['data_length']
            if user_host == '':
                user_host = '127.0.0.1'
            elif len(user_host.split('.')) != 4:
                raise ProtocolError
            b_proxy_port = int(proxy_port).to_bytes(length=2, byteorder='big', signed=False)
            b_user_host = bytes(map(int, user_host.split('.')))
            b_user_port = int(user_port).to_bytes(length=2, byteorder='big', signed=False)
            length = Protocol.UDP_DATA_HEADER_LEN + data_length
            b_len = length.to_bytes(length=4, byteorder='big', signed=False)
            b_crc = b'\x55\x55'
            return Protocol.__UDP_DATA_FLAG + b_len + b_crc + b_proxy_port + b_user_host + b_user_port

        else:
            raise ProtocolError('Unsupported udp packet type.')

    @staticmethod
    def parse_datagram_header(pkt):
        """
        解析UDP数据包的包头，并根据包的类型返回相应的参数。
        :param pkt:
        :return: (UdpPacketType.UNKNOWN, (None,)),
                 or (UdpPacketType.SYNC, (info, timestamp)),
                 or (UdpPacketType.DATA, (proxy_port, user_address)).
        """
        try:
            if pkt[0:2] == Protocol.__UDP_SYNC_FLAG:
                if pkt[6:8] != b'\x55\x55':
                    raise ProtocolError
                # Skip checking packet length ...
                #
                info, timestamp = pkt[Protocol.UDP_PACKET_HEADER_LEN:].decode().rsplit(Protocol.SEPARATOR, 1)
                return Protocol.UdpPacketType.SYNC, (info, timestamp)

            elif pkt[0:2] == Protocol.__UDP_DATA_FLAG:
                if pkt[6:8] != b'\x55\x55':
                    raise ProtocolError
                # Skip checking packet length ...
                #
                proxy_port = int.from_bytes(pkt[8:10], byteorder='big', signed=False)
                user_host = '.'.join(str(x) for x in list(pkt[10:14]))
                user_port = int.from_bytes(pkt[14:16], byteorder='big', signed=False)
                return Protocol.UdpPacketType.DATA, (proxy_port, (user_host, user_port))

            else:
                raise ProtocolError
        except ProtocolError:
            return Protocol.UdpPacketType.UNKNOWN, (None, )
