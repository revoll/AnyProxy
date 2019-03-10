# -*- coding: utf-8 -*-
import socket
import select
from threading import Thread, current_thread, Lock
from time import sleep
from common import Module
from loguru import logger as log


class ProtocolError(Exception):
    pass


class Protocol(Module):
    """
    内网穿透服务端与客户端通信协议：命令接口、工具函数等
    """
    MODULE_NAME = 'PROTOCOL'
    SOCKET_BUFFER_SIZE = 4096
    MAX_CONNECTIONS = 500

    class Command:
        CHECK_MAPPING_PORTS = 'CheckMappingPorts'
        PING = 'Ping'
        ADD_NEW_CONNECTION = 'AddNewConnection'

    class Response:
        ERROR = 'Error'
        SUCCESS = 'Success'
        INVALID = 'Invalid'
        UNKNOWN = 'Unknown'

    @staticmethod
    def __default_callback(cmd, data):
        log.info('Received "%s:%s", reply "%s" by default.', (cmd, data, Protocol.Response.UNKNOWN))
        return Protocol.Response.UNKNOWN

    @staticmethod
    def __req_str(cmd_str, val_str):
        return '<' + cmd_str + '>' + val_str + '</' + cmd_str + '>'

    @staticmethod
    def __resp_str(cmd_str, val_str):
        return '<Response>' + cmd_str + ':' + val_str + '</Response>'

    def __init__(self, server_host='127.0.0.1', server_port=10000, is_server=False):
        super().__init__(Protocol.MODULE_NAME)
        self.__is_server = True if is_server else False     # 服务端模式/客户端模式
        self.__host = server_host                           # 服务器地址
        self.__port = int(server_port)                      # 服务器端口
        self.__t_flag = False
        self.__t_handler = None
        self.__sock_cmd = None
        self.__epoll = select.epoll()
        if self.__is_server:
            self.__buffer = ''
            self.__buffer_lock = Lock()
        else:
            self.__fn_callback = Protocol.__default_callback
        self.set_status(Module.Status.PREPARE)

    def __identify(self, is_server):
        if self.__is_server != is_server:
            raise ProtocolError('Function not supported in %s mode.' % 'CLIENT' if self.__is_server else 'SERVER')

    def register_process_callback(self, func):
        self.__identify(is_server=False)
        self.__fn_callback = func

    def execute(self, cmd, data):
        self.__identify(is_server=True)
        _req = self.__req_str(cmd, str(data))
        self.__sock_cmd.send(_req.encode())
        while not self.__buffer:
            sleep(0.1)
        with self.__buffer_lock:
            _resp = self.__buffer
            self.__buffer = ''
        if len(_resp) >= len(self.__resp_str(cmd, '')) and _resp[10:10+len(cmd)] == cmd \
                and _resp[:10] == '<Response>' and _resp[-11:] == '</Response>':
            return _resp[10:-11].split(':', 1)[1]
        else:
            raise ProtocolError('Invalid response format: sent %s while received %s.' % (_req, _resp))

    def __thread_server(self):
        """
        服务端服务线程函数：除监视连接状态外，不执行其它任务
        :return:
        """
        self.__t_flag = True
        log.info('%s started.' % current_thread().name)
        try:
            while self.__t_flag:
                events = self.__epoll.poll(1)
                if not events:
                    continue
                for fd, event in events:
                    if event & ~select.EPOLLIN:
                        raise Exception('Connection closed unexpectedly.')
                    else:
                        _data = self.__sock_cmd.recv(Protocol.SOCKET_BUFFER_SIZE)
                        if _data:
                            with self.__buffer_lock:
                                self.__buffer += _data.decode()
        except Exception as e:
            log.critical(e)
        finally:
            self.__epoll.unregister(self.__sock_cmd.fileno())
            self.__sock_cmd.close()
        log.warning('%s exited.' % current_thread().name)
        self.__t_flag = False
        self.set_status(Module.Status.STOPPED)

    def __thread_client(self):
        """
        客户端服务线程函数：处理服务端的请求并返回结果
        :return:
        """
        def process_request(req):
            p1 = req.find('>', 0, -1)
            p2 = req.find('<', 1)
            if 0 < p1 < p2 and req[0] == '<' and req[-1] == '>' and req[1:p1] == req[p2+2:-1]:
                _cmd = req[1:p1]
                _data = req[p1+1:p2]
            else:
                log.error('Invalid command form server: %s', req)
                return ''
            return self.__resp_str(_cmd, self.__fn_callback(_cmd, _data))

        self.__t_flag = True
        log.info('%s started.' % current_thread().name)
        try:
            while self.__t_flag:
                events = self.__epoll.poll(1)
                if not events:
                    continue
                for fd, event in events:
                    if event & ~select.EPOLLIN:
                        raise Exception('Connection closed unexpectedly.')
                    else:
                        _req = self.__sock_cmd.recv(Protocol.SOCKET_BUFFER_SIZE)
                        if _req:
                            _resp = process_request(_req.decode())
                            if _resp:
                                self.__sock_cmd.send(_resp.encode())
        except Exception as e:
            log.critical(e)
        finally:
            self.__epoll.unregister(self.__sock_cmd.fileno())
            self.__sock_cmd.close()
        log.warning('%s exited.' % current_thread().name)
        self.__t_flag = False
        self.set_status(Module.Status.STOPPED)

    def start(self):
        super().start()
        if self.__is_server:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind((self.__host, self.__port))
                sock.listen(1)
                log.info('Listening client connection on port(%d)...' % self.__port)
                self.__sock_cmd, address = sock.accept()
                sock.close()
                self.__sock_cmd.setblocking(False)
                self.__epoll.register(self.__sock_cmd.fileno(), select.EPOLLIN | select.EPOLLRDHUP)
                log.info('Connected from %s:%d' % (address[0], address[1]))
            except socket.error as e:
                log.error('%s. Failed to bind on port(%d).' % (e, self.__port))
                exit()
            self.__t_handler = Thread(None, self.__thread_server, 'Thread-ServerProtocol', (), daemon=False)
            self.__t_handler.start()
        else:
            try:
                self.__sock_cmd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.__sock_cmd.connect((self.__host, self.__port))
                self.__sock_cmd.setblocking(False)
                self.__epoll.register(self.__sock_cmd.fileno(), select.EPOLLIN | select.EPOLLRDHUP)
                log.info('Connected to %s:%d' % (self.__host, self.__port))
            except socket.error as e:
                log.error('%s. Failed to connect to the server.' % e)
                exit()
            self.__t_handler = Thread(None, self.__thread_client, 'Thread-ClientProtocol', (), daemon=False)
            self.__t_handler.start()
        self.set_status(Module.Status.RUNNING)

    def stop(self):
        self.set_status(Module.Status.STOPPED)
        if self.__t_flag:
            self.__t_flag = False
            self.__t_handler.join()
        super().stop()


if __name__ == '__main__':
    def __protocol_callback(cmd, data):
        if cmd == Protocol.Command.CHECK_MAPPING_PORTS:
            port_mapping = eval(data)
            for port in list(port_mapping.keys()):
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect(('', port))
                except ConnectionRefusedError:
                    del port_mapping[port]
            return str(port_mapping)
        else:
            log.error('Invalid command.')
            return Protocol.Response.INVALID
    server_protocol = Protocol(is_server=True)
    client_protocol = Protocol(is_server=False)
    client_protocol.register_process_callback(__protocol_callback)
    server_handler = Thread(None, server_protocol.start, 'Server-Start()', ())
    client_handler = Thread(None, client_protocol.start, 'Client-Start()', ())
    server_handler.start()
    client_handler.start()
    server_handler.join()
    client_handler.join()
    mapping = {80: 10080, 81: 10081, 82: 10082}
    response = server_protocol.execute(Protocol.Command.CHECK_MAPPING_PORTS, str(mapping))
    try:
        while True:
            sleep(1)
    except KeyboardInterrupt:
        pass
    server_protocol.stop()
    client_protocol.stop()
