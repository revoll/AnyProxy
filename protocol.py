# -*- coding: utf-8 -*-
from time import sleep


class ProtocolError(Exception):
    """协议错误
    """
    def __init__(self, msg='', cmd=None, data=None):
        err = '%s (command: %s, data: %s)' % (msg, cmd, data) if cmd is not None else msg
        Exception.__init__(self, err)
        self.cmd = cmd
        self.data = data


class Protocol:
    """内网穿透协议类

    Data Transfer Protocol for each command:
    ----------------------------------------
        |
        |-- PING: Request(timestamp) >>> Response(timestamp)
        |
        |-- IDENTIFY_CONNECTION: Request(proxy_manager_uuid) >>> Response(client_uuid)
        |                                                           or
        |                                                        Response(client_uuid*conn_uuid)
        |
        |-- QUERY_MAPPING_PORTS: Request('') >>> Response(port_mapping_pair to be mapped)
        |-- CONFIRM_MAPPING_PORTS: Request('port_mapping_pair allowed') >>> Response(Result)
        |
        |-- ADD_NEW_CONNECTION:  Request('port_number') >>> Response(connection_uuid)
        `-- CONFIRM_CONNECTION:  Request('received_connection_uuid') >>> Response(Result)
    """
    SOCKET_BUFFER_SIZE = 4096
    MAX_CONNECTIONS = 100
    SERVER_MAX_CONNECTIONS = 1000
    DEFAULT_TIMEOUT = 15
    MAX_RETRY_WAIT_TIME = 60
    UUID_LENGTH = 32
    DATA_SEPARATOR = '*'
    __STR_RESPONSE = 'Response'

    class Command:
        PING = 'Ping'
        IDENTIFY_CONNECTION = 'IdentifyConnection'
        QUERY_MAPPING_PORTS = 'QueryMappingPorts'
        CONFIRM_MAPPING_PORTS = 'ConfirmMappingPorts'
        ADD_NEW_CONNECTION = 'AddNewConnection'
        CONFIRM_CONNECTION = 'ConfirmConnection'

    class Result:
        ERROR = 'Error'
        SUCCESS = 'Success'
        INVALID = 'Invalid'
        UNKNOWN = 'Unknown'

    @staticmethod
    def __req_str(cmd_str, val_str):
        if not cmd_str:
            raise ProtocolError('Protocol command receives empty string.', cmd_str, val_str)
        return '<%s>%s</%s>' % (str(cmd_str), str(val_str), str(cmd_str))

    @staticmethod
    def __resp_str(cmd_str, val_str):
        if not cmd_str:
            raise ProtocolError('Protocol command receives empty string.', cmd_str, val_str)
        return '<%s>%s:%s</%s>' % (Protocol.__STR_RESPONSE, str(cmd_str), str(val_str), Protocol.__STR_RESPONSE)

    @staticmethod
    def __parse_req_str(req):
        p1 = req.find('>', 1, -2)
        p2 = req.find('<', 2, -1)
        if req[0] == '<' and 0 < p1 < p2 and req[-1] == '>' and req[1:p1] == req[p2+2:-1]:
            return req[1:p1], req[p1+1:p2]
        raise ProtocolError('Parsing invalid request: %s' % req)

    @staticmethod
    def __parse_resp_str(resp):
        prefix = '<%s>' % Protocol.__STR_RESPONSE
        suffix = '</%s>' % Protocol.__STR_RESPONSE
        p = resp.find(':', len(prefix), -len(suffix))
        if p > 0 and len(resp) >= len(Protocol.__resp_str('z', '')) and \
                resp[:len(prefix)] == prefix and resp[-len(suffix):] == suffix:
            return resp[len(prefix):p], resp[p+1:-len(suffix)]
        raise ProtocolError('Parsing invalid response: %s' % resp)

    @staticmethod
    def request_client(conn, cmd, data='', timeout=-1):
        """【服务端】 主动询问客户端信息。（注意：socket必须为非阻塞模式！）
        :param conn: 非阻塞模式的TCP套接字
        :param cmd: 需要执行的询问指令
        :param data: 指令附带的数据
        :param timeout: 超时时间
        :return: 客户端响应的结果信息
        """
        req_str = Protocol.__req_str(cmd, data)
        conn.sendall(req_str.encode())
        time_wait = 0.0
        while timeout == -1 or time_wait < timeout:
            try:
                resp_str = conn.recv(Protocol.SOCKET_BUFFER_SIZE).decode()
                resp_cmd, resp_data = Protocol.__parse_resp_str(resp_str)
                if resp_cmd == cmd:
                    return resp_cmd, resp_data
                else:
                    raise ProtocolError('Expected <%s> while received <%s>.' % (cmd, resp_cmd))
            except BlockingIOError:
                pass
            sleep(0.25)
            time_wait += 0.25
        raise ProtocolError('Waiting client response timeout after %ds.' % timeout, cmd, data)

    @staticmethod
    def receive_request(conn, expect_cmd=None, timeout=-1):
        """【客户端】 接收来自服务端的请求。（这是处理请求的第一步。注意：socket必须为非阻塞模式！）
        :param conn: 非阻塞模式的TCP套接字
        :param expect_cmd: 期望得到的指令（如果不匹配将触发异常）
        :param timeout: 超时时间
        :return:
        """
        time_wait = 0.0
        while timeout == -1 or time_wait < timeout:
            try:
                req_str = conn.recv(Protocol.SOCKET_BUFFER_SIZE).decode()
                req_cmd, req_data = Protocol.__parse_req_str(req_str)
                if expect_cmd and req_cmd != expect_cmd:
                    Protocol.response_server(conn, req_cmd, Protocol.Result.INVALID)
                    raise ProtocolError('Expected <%s> while received <%s>.' % (expect_cmd, req_cmd))
                else:
                    return req_cmd, req_data
            except BlockingIOError:
                pass
            sleep(0.25)
            time_wait += 0.25
        raise ProtocolError('Waiting server request timeout after %ds.' % timeout)

    @staticmethod
    def response_server(conn, cmd, data=''):
        """【客户端】 对服务端的请求做出响应。（这是处理请求的第二步。注意：socket必须为非阻塞模式！）
        :param conn: 非阻塞模式的TCP套接字
        :param cmd: 需要响应的询问指令
        :param data: 指令附带的数据
        :return:
        """
        conn.sendall(Protocol.__resp_str(cmd, data).encode())
