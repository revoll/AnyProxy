import uuid
import select
from time import sleep
from threading import current_thread
from protocol import Protocol, ProtocolError
from modules import ThreadedModule, ModuleError
from utils import check_listening, tcp_connect
from loguru import logger as log


class ProxyError(ModuleError):
    pass


class ProxyClient(ThreadedModule):
    """端口代理转发类（客户端）
    """

    def __init__(self, server_host='', server_port=10000, port_map=None, client_uuid=None):
        """
        self.__server_host              45.67.89.123
        self.__server_port              10000
        self.__ini_port_map             {80: 10080, 81: 10081, 82: 10082}

        self.__sock_main                socket<fd=9, ...>
        self.__port_map                 {80:10080, 81:10081, 82:10082, ...}
        self.__port_fds                 {80:[4,7,9], 81:[14,16,17,19], 82: [None] ...}
        self.__fd_to_socket             {16123:<sock_4>, 16125:<sock_7>, 16127:<sock_9>, ...}
        self.__fd_to_fd                 {16123:16124, 16125:16126:, 16127:16128, ...}
        self.__epoll                    listens: mapped_ports & connections, exclude `self.__sock_main`
        """
        if type(server_port) != int:
            raise ProxyError('Invalid argument for `server_port`: %s' % server_port)
        if not port_map:  # TODO: variables checking
            raise ProxyError('Invalid argument for `port_map`: %s' % port_map)
        for p in port_map.keys():
            if type(p) != int or type(port_map[p]) != int or list(port_map.values()).count(port_map[p]) != 1:
                raise ProxyError('Invalid argument for `port_map`: %s' % port_map)
        if not client_uuid or len(client_uuid) != Protocol.UUID_LENGTH:
            client_uuid = uuid.uuid4().hex
        super().__init__(module_name='PROXY_'+client_uuid[:8], module_uuid=client_uuid)
        self.__server_host = server_host
        self.__server_port = server_port
        self.__ini_port_map = port_map
        self.register_callback(self.__thread_main)

    def __connect(self):
        """连接服务器开启主连接（控制命令专用），依次响应连接认证、映射端口问询、映射端口确认，将主连接加入监听。"""
        self.__sock_main = None
        self.__port_map = {}
        self.__port_fds = {}
        self.__fd_to_socket = {}
        self.__fd_to_fd = {}
        self.__epoll = select.epoll()

        self.__sock_main = tcp_connect(self.__server_host, self.__server_port, blocking=False)
        try:
            cmd, data = Protocol.receive_request(
                self.__sock_main, Protocol.Command.IDENTIFY_CONNECTION, timeout=Protocol.DEFAULT_TIMEOUT)
            Protocol.response_server(self.__sock_main, cmd, self.get_uuid())

            cmd, data = Protocol.receive_request(
                self.__sock_main, Protocol.Command.QUERY_MAPPING_PORTS, timeout=Protocol.DEFAULT_TIMEOUT)
            ini_port_map = {k: v for k, v in self.__ini_port_map.items() if check_listening('', k)}
            Protocol.response_server(self.__sock_main, cmd, ini_port_map)

            cmd, data = Protocol.receive_request(
                self.__sock_main, Protocol.Command.CONFIRM_MAPPING_PORTS, timeout=Protocol.DEFAULT_TIMEOUT)
            try:
                self.__port_map = eval(data)
            except Exception:
                raise ProtocolError('Invalid argument for `port_map`: %s' % data)
            if not self.__port_map:
                raise ProxyError('Empty port mapping table after filtering.')
            Protocol.response_server(self.__sock_main, cmd, Protocol.Result.SUCCESS)

            self.__port_fds = {x: [] for x in self.__port_map.keys()}
            self.__epoll.register(self.__sock_main.fileno(), select.EPOLLIN | select.EPOLLRDHUP)
        except Exception as e:
            self.__sock_main.close()
            raise e

    def __disconnect(self):
        """取消主连接事件监听，逐个移除现有的数据连接，并关闭主连接。"""
        self.__epoll.unregister(self.__sock_main.fileno())
        for p in self.__port_fds.keys():
            for fd in list(self.__port_fds[p]):
                self.__remove_pair(fd)
        self.__sock_main.close()
        self.__epoll.close()

    def __reconnect(self):
        """先关闭现有的数据连接及主连接，再根据策略重新建立连接。如果服务器在较长时间无法访问，重连时间会逐步延长。"""
        self.__disconnect()
        log.info('Reconnecting...')
        time_wait = wait_inc = 0
        while self._continue_running_thread():
            if not check_listening(self.__server_host, self.__server_port):
                log.warning('Service(%s:%d) is unreachable.' % (self.__server_host, self.__server_port))
                if time_wait < Protocol.MAX_RETRY_WAIT_TIME:
                    if wait_inc < Protocol.MAX_RETRY_WAIT_TIME:
                        wait_inc += 1
                    time_wait += wait_inc
                    if time_wait > Protocol.MAX_RETRY_WAIT_TIME:
                        time_wait = Protocol.MAX_RETRY_WAIT_TIME
                sleep(time_wait)
                continue
            try:
                self.__connect()
                log.info('Client<%s> is now reconnected!' % self.get_name())
                break
            except Exception as e:
                log.error(e)

    def __add_pair(self):
        """根据握手协议要求，在本地建立双向两个连接，并将连接信息记录到数据结构。"""
        conn_uuid = uuid.uuid4().hex
        uuid_pair = '%s%s%s' % (self.get_uuid(), Protocol.DATA_SEPARATOR, conn_uuid)
        conn1 = conn2 = None
        try:
            cmd, data = Protocol.receive_request(
                self.__sock_main, Protocol.Command.ADD_NEW_CONNECTION, timeout=Protocol.DEFAULT_TIMEOUT)
            try:
                port = int(data)
            except ValueError:
                raise ProtocolError('Invalid argument for `port`(to add connection on): %s' % data)
            conn1 = tcp_connect('', port, blocking=False)
            conn2 = tcp_connect(self.__server_host, self.__server_port, blocking=False)
            Protocol.response_server(self.__sock_main, cmd, conn_uuid)

            cmd_2, data_2 = Protocol.receive_request(
                conn2, Protocol.Command.IDENTIFY_CONNECTION, timeout=Protocol.DEFAULT_TIMEOUT)
            Protocol.response_server(conn2, cmd_2, uuid_pair)

            cmd, data = Protocol.receive_request(
                self.__sock_main, Protocol.Command.CONFIRM_CONNECTION, timeout=Protocol.DEFAULT_TIMEOUT)
            if data == conn_uuid:
                Protocol.response_server(self.__sock_main, cmd, Protocol.Result.SUCCESS)
            else:
                Protocol.response_server(self.__sock_main, cmd, Protocol.Result.ERROR)
                raise ProxyError('Expected uuid:<%s> while received uuid:<%s>.' % (conn_uuid, data))
        except Exception as e:
            if conn1:
                conn1.close()
            if conn2:
                conn2.close()
            raise e
        # conn1.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, True)
        # conn2.setsockopt(socket.IPPROTO_TCP, socket.TCP_CORK, True)
        fd1 = conn1.fileno()
        fd2 = conn2.fileno()
        self.__port_fds[port].append(fd1)
        self.__fd_to_socket[fd1] = conn1
        self.__fd_to_socket[fd2] = conn2
        self.__fd_to_fd[fd1] = fd2
        self.__fd_to_fd[fd2] = fd1
        self.__epoll.register(fd1, select.EPOLLIN | select.EPOLLRDHUP)
        self.__epoll.register(fd2, select.EPOLLIN | select.EPOLLRDHUP)
        log.info('Local(%s:%d) ----> Server(%s:%d)' %
                 (conn1.getpeername()[0], port, self.__server_host, self.__port_map[port]))
        return port, conn1, conn2

    def __remove_pair(self, key_fd):
        """根据`key_fd`查找连接对，先取消监听，关闭连接，最后再从数据结构中删除记录。"""
        port = fd1 = fd2 = None
        for k in self.__port_fds:
            fd_list = self.__port_fds[k]
            if key_fd in fd_list:
                port = k
                fd1 = key_fd
                fd2 = self.__fd_to_fd[key_fd]
                break
            if self.__fd_to_fd[key_fd] in fd_list:
                port = k
                fd2 = key_fd
                fd1 = self.__fd_to_fd[key_fd]
                break
        if not port:
            raise ProxyError('Connection(fd=%d) not found.' % key_fd)
        conn1 = self.__fd_to_socket[fd1]
        conn2 = self.__fd_to_socket[fd2]
        del self.__fd_to_fd[fd1]
        del self.__fd_to_fd[fd2]
        del self.__fd_to_socket[fd1]
        del self.__fd_to_socket[fd2]
        self.__port_fds[port].remove(fd1)
        self.__epoll.unregister(fd1)
        self.__epoll.unregister(fd2)
        log.info('Local(%s:%d) --x-> Server(%s:%d)' %
                 (conn1.getpeername()[0], port, self.__server_host, self.__port_map[port]))
        conn1.close()
        conn2.close()

    def __thread_main(self):
        """主线程函数"""
        try:
            self.__connect()
        except Exception as e:
            log.error(e)
            self.set_status(ThreadedModule.Status.STOPPED)
            return
        log.info('Module<%s> Started!' % current_thread().name)
        self.set_status(ThreadedModule.Status.RUNNING)
        try:
            while self._continue_running_thread():
                events = self.__epoll.poll(1)
                if not events:
                    continue
                for fd, event in events:
                    if fd == self.__sock_main.fileno():
                        if event & select.EPOLLIN:
                            try:
                                self.__add_pair()
                            except Exception as e:
                                log.error(e)
                        elif event & select.EPOLLRDHUP:
                            log.critical('Disconnected from server(%s:%d).' %
                                         (self.__server_host, self.__server_port))
                            self.__reconnect()
                    elif event & select.EPOLLRDHUP:
                        try:
                            self.__remove_pair(fd)
                        except Exception as e:
                            log.error(e)
                    elif event & select.EPOLLIN:
                        sock_src = self.__fd_to_socket[fd]
                        sock_dst = self.__fd_to_socket[self.__fd_to_fd[fd]]
                        data = sock_src.recv(Protocol.SOCKET_BUFFER_SIZE)
                        sock_dst.sendall(data)
        except Exception as e:
            log.critical(e)
        finally:
            self.__disconnect()
        self.set_status(ThreadedModule.Status.STOPPED)
        log.warning('Module<%s> Exited!' % current_thread().name)

    def restart(self):
        """不支持重启"""
        raise ProxyError('Module<%s> does NOT Support Restart!' % self.get_name())
