import select
import uuid
from time import sleep
from threading import current_thread, Lock
from protocol import Protocol, ProtocolError
from modules import ThreadedModule, ModuleError
from utils import check_listening, tcp_listen
from loguru import logger as log


class ProxyError(ModuleError):
    pass


class _Proxy(ThreadedModule):
    """端口代理转发类（服务端）
    """

    def __init__(self, client_uuid, sock):
        """
        self.__sock_main                socket<fd=9, ...>
        self.__port_map                 {10080:80, 10081:81, 10082:82, ...}
        self.__port_fd                  {10080:3, 10081:13, 10082:27, ...}
        self.__port_fds                 {10080:[4,7,9], 10081:[14,16,17,19], 10082: [None] ...}
        self.__fd_to_socket             {4:<sock_4>, 7:<sock_7>, 9:<sock_9>, ...}
        self.__fd_to_fd                 {4:16124, 7:16126:, 9:16128, ...}
        self.__epoll                    listens: mapped_ports & connections, exclude `self.__sock_main`

        self.__conn_pool                [<sock1>, <sock2>, <sock3>, ...]
        self.__conn_pool_lock           Lock<>
        """
        super().__init__(module_name='PROXY_'+client_uuid[:8], module_uuid=client_uuid)
        self.__sock_main = sock
        self.register_callback(self.__thread_main)

    def on_connection_received(self, conn_uuid, sock):
        """管理器回调函数：管理器收到新的连接后，调用此方法将新的连接分发到对应的代理器。
        :param conn_uuid: 收到的连接的UUID字符串
        :param sock: 收到的socket连接对象
        """
        with self.__conn_pool_lock:
            self.__conn_pool[conn_uuid] = sock

    def __wait_for_connection(self, conn_uuid, timeout=-1):
        """等待并获取客户端的连接请求"""
        time_wait = 0.0
        while timeout == -1 or time_wait < timeout:
            with self.__conn_pool_lock:
                if conn_uuid in self.__conn_pool.keys():
                    conn = self.__conn_pool[conn_uuid]
                    del self.__conn_pool[conn_uuid]
                    return conn
            sleep(0.25)
            time_wait += 0.25
        raise ProxyError('Waiting client\'s connection timeout after %ds.' % timeout)

    def __startup_service(self):
        """初始化数据结构，协商需要映射的端口，根据端口映射表依次启动对应的端口服务，并将主连接加入监听。

        Working Procedure:
        ------------------
            |
            |-- Step 0: Query connection type with server uuid.     [Server] --> [Client]
            |           Response with proxy client's uuid.          [Server] <-- [Client]
            |
            |-- Step 1: Query list of ports to be mapped.           [Server] --> [Client]
            |           Response with list of ports.                [Server] <-- [Client]
            |
            |-- Step 2: Confirm list of ports that server allows.   [Server] --> [Client]
            |           Response with status string.                [Server] <-- [Client]
            |
            `-- Step 3: Initialize proxy data structure, register listening main_socket, etc.
        """
        self.__port_map = {}
        self.__port_fd = {x: None for x in self.__port_map.keys()}
        self.__port_fds = {x: [] for x in self.__port_map.keys()}
        self.__fd_to_socket = {}
        self.__fd_to_fd = {}
        self.__epoll = select.epoll()
        self.__conn_pool = {}
        self.__conn_pool_lock = Lock()

        cmd, data = Protocol.request_client(self.__sock_main, Protocol.Command.QUERY_MAPPING_PORTS,
                                            '', timeout=Protocol.DEFAULT_TIMEOUT)
        try:
            ini_map = eval(data)
        except Exception:
            raise ProtocolError('Client uploads invalid data for `port_map` to confirm: %s' % data)
        confirmed = {k: v for k, v in ini_map.items() if not check_listening('', ini_map[k])}
        if not confirmed:
            raise ProxyError('Empty port mapping table after filtering.')
        cmd, status = Protocol.request_client(self.__sock_main, Protocol.Command.CONFIRM_MAPPING_PORTS,
                                              confirmed, timeout=Protocol.DEFAULT_TIMEOUT)
        if status != Protocol.Result.SUCCESS:
            raise ProtocolError('Proxy client refused to continue making proxy.')
        self.__port_map = {v: k for k, v in confirmed.items()}
        try:
            for p in self.__port_map:
                self.__start_listening(p)
        except Exception as e:
            for q in self.__port_fd.keys():
                if self.__port_fd[q]:
                    self.__stop_listening(q)
            raise e
        self.__sock_main.setblocking(False)
        self.__epoll.register(self.__sock_main.fileno(), select.EPOLLRDHUP)
        log.info('Client<%s> mapped %s.' % (self.get_uuid(), str(self.__port_map)))

    def __shutdown_service(self):
        """依次关闭所有的监听端口，清理连接池中未认领的连接，取消对主连接的监听并关闭该连接。"""
        for p in self.__port_map.keys():
            try:
                self.__stop_listening(p)
            except Exception as e:
                log.error(e)
        with self.__conn_pool_lock:
            for s in self.__conn_pool:
                self.__conn_pool[s].close()
        self.__epoll.unregister(self.__sock_main.fileno())
        self.__sock_main.close()
        self.__epoll.close()

    def __start_listening(self, port):
        """启动一个需要监听的端口"""
        if port not in self.__port_map:
            raise ProxyError('Port(%d) is not listed in mapping table.' % port)
        sock = tcp_listen(self.__sock_main.getsockname()[0], port,
                          blocking=False, max_conn=Protocol.MAX_CONNECTIONS)
        self.__port_fd[port] = sock.fileno()
        self.__fd_to_socket[sock.fileno()] = sock
        self.__port_fds[port] = []
        self.__epoll.register(sock.fileno(), select.EPOLLIN | select.EPOLLRDHUP)

    def __stop_listening(self, port):
        """关闭一个正在监听的端口"""
        if port not in self.__port_map:
            raise ProxyError('Port(%d) is not listed in mapping table.' % port)
        sock_fd = self.__port_fd[port]
        if sock_fd is None:
            raise ProxyError('Port(%s) is not in listening.' % port)
        for k in list(self.__port_fds[port]):
            self.__remove_pair(k)
        self.__epoll.unregister(sock_fd)
        self.__fd_to_socket[sock_fd].close()
        del self.__fd_to_socket[sock_fd]
        self.__port_fd[port] = None

    def __add_pair(self, port):
        """建立一个新的数据连接通道"""
        conn1 = conn2 = None
        try:
            conn1, _ = self.__fd_to_socket[self.__port_fd[port]].accept()

            _, conn_uuid = Protocol.request_client(self.__sock_main, Protocol.Command.ADD_NEW_CONNECTION,
                                                   self.__port_map[port], timeout=Protocol.DEFAULT_TIMEOUT)
            if len(conn_uuid) != Protocol.UUID_LENGTH:
                raise ProtocolError('Invalid uuid:%s from proxy client.' % conn_uuid)

            conn2 = self.__wait_for_connection(conn_uuid, timeout=Protocol.DEFAULT_TIMEOUT)

            _, status = Protocol.request_client(self.__sock_main, Protocol.Command.CONFIRM_CONNECTION,
                                                conn_uuid, timeout=Protocol.DEFAULT_TIMEOUT)
            if status != Protocol.Result.SUCCESS:
                raise ProtocolError('Invalid confirmation(%s) from proxy client.' % status)
        except Exception as e:
            if conn1:
                conn1.close()
            if conn2:
                conn2.close()
            raise e
        conn1.setblocking(False)
        conn2.setblocking(False)
        fd1 = conn1.fileno()
        fd2 = conn2.fileno()
        self.__port_fds[port].append(fd1)
        self.__fd_to_socket[fd1] = conn1
        self.__fd_to_socket[fd2] = conn2
        self.__fd_to_fd[fd1] = fd2
        self.__fd_to_fd[fd2] = fd1
        self.__epoll.register(fd1, select.EPOLLIN | select.EPOLLRDHUP)
        self.__epoll.register(fd2, select.EPOLLIN | select.EPOLLRDHUP)
        log.info('Client(%s:%d) ----> Server(%s:%d)' %
                 (conn2.getpeername()[0], self.__port_map[port], conn1.getpeername()[0], port))
        return conn1, conn2

    def __remove_pair(self, key_fd):
        """关闭一个已有的数据连接通道"""
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
            raise ProxyError('Remove connection(fd=%d) failed as not found.' % key_fd)
        conn1 = self.__fd_to_socket[fd1]
        conn2 = self.__fd_to_socket[fd2]
        del self.__fd_to_fd[fd1]
        del self.__fd_to_fd[fd2]
        del self.__fd_to_socket[fd1]
        del self.__fd_to_socket[fd2]
        self.__port_fds[port].remove(fd1)
        self.__epoll.unregister(fd1)
        self.__epoll.unregister(fd2)
        log.info('Client(%s:%d) --x-> Server(%s:%d)' %
                 (conn2.getpeername()[0], self.__port_map[port], conn1.getpeername()[0], port))
        conn1.close()
        conn2.close()

    def __thread_main(self):
        """主线程函数"""
        try:
            self.__startup_service()
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
                        #
                        # in case of event: EPOLLRDHUP, wait for proxy manager to call `self.stop()`
                        #
                        while self._continue_running_thread():
                            sleep(0.1)
                        raise ProxyError('Client<%s> connection closed!' % self.get_uuid())
                    elif fd in self.__port_fd.values():
                        sock = self.__fd_to_socket[fd]
                        port = sock.getsockname()[1]
                        if event & select.EPOLLIN:
                            try:
                                self.__add_pair(port)
                            except Exception as e:
                                log.error(e)
                        else:
                            raise ProxyError('Proxy will exit as listening port(%d) failure.' % port)
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
            self.__shutdown_service()
        self.set_status(ThreadedModule.Status.STOPPED)
        log.warning('Module<%s> Exited!' % current_thread().name)

    def restart(self):
        """不支持重启"""
        raise ProxyError('Module<%s> does NOT Support Restart!' % self.get_name())


class ProxyServer(ThreadedModule):
    """端口代理转发客户端连接管理类
    """

    def __init__(self, host, port):
        """
        self.__host                     '0.0.0.0'
        self.__port                     10000
        self.__sock_server              socket(PORT=10000)
        self.__proxy_clients            [uuid1:Client1, uuid2:Client2, ...]
        self.__proxy_fd                 [uuid1:fd1, uuid2:fd2, ...]
        self.__epoll                    listens: `self.__sock_server` & `_Proxy.__sock_main`s
        """
        super().__init__(module_name='PROXY_MANAGER', module_uuid=uuid.uuid4().hex)
        self.__host = host
        self.__port = int(port)
        self.register_callback(self.__thread_main)

    def __startup_service(self):
        """启动代理管理器"""
        self.__sock_server = None
        self.__proxy_clients = {}
        self.__proxy_fd = {}
        self.__epoll = select.epoll(5)  # optimize initial client size = 5
        self.__sock_server = tcp_listen(self.__host, self.__port,
                                        blocking=False, max_conn=Protocol.SERVER_MAX_CONNECTIONS)
        self.__epoll.register(self.__sock_server.fileno(), select.EPOLLIN | select.EPOLLRDHUP)

    def __shutdown_service(self):
        """关闭代理管理器"""
        for k in list(self.__proxy_fd.keys()):
            self.__epoll.unregister(self.__proxy_fd[k])
            self.__proxy_clients[k].stop()
            del self.__proxy_clients[k]
            del self.__proxy_fd[k]
        self.__epoll.unregister(self.__sock_server.fileno())
        self.__sock_server.close()
        self.__epoll.close()

    def __thread_main(self):
        """主线程函数

        Working Procedure:
        ------------------
        Checking `self.__epoll` if event occurs.
            |
            |-- In case of: New connection arrives on server port (e.g.10000).
            |   |
            |   `-- Step 0: Query connection type.
            |       |
            |       |-- In case of: New proxy client arrives.
            |       |       |
            |       |       `-- Step 1: Consult mapping ports. See `self.__consult_mapping_ports()`
            |       |           |
            |       |           `-- Step 2: Startup new client proxy.
            |       |               |
            |       |               `-- Step 3: Watch it by checking socket status.
            |       |                   |
            |       |                   `-- Step 4: Stop client proxy if connection lost.
            |       |
            |       `-- In case of: New proxy client connection arrives.
            |               |
            |               `-- Dispatch the incoming socket according to client proxy's uuid.
            |
            |-- In case of: Client proxy's main connection receives data.
            |   |
            |   `-- (Discards)
            |
            |-- In case of: Client proxy's main connection is lost.
            |   |
            |   `-- Stop and close target proxy client.
            |
            `-- In case of: Listening socket on server port is broken.
                |
                `-- Stop and close all running proxy clients, then raise an exception to exit program.
        """
        try:
            self.__startup_service()
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
                    if fd == self.__sock_server.fileno():
                        if event & select.EPOLLIN:
                            try:
                                conn, address = self.__sock_server.accept()
                                conn.setblocking(False)
                                cmd, data = Protocol.request_client(
                                    conn, Protocol.Command.IDENTIFY_CONNECTION, self.get_uuid(),
                                    timeout=Protocol.DEFAULT_TIMEOUT)
                            except ProtocolError as e:
                                log.error(e)
                                continue
                            if len(data) == Protocol.UUID_LENGTH:
                                try:
                                    client_uuid = data
                                    proxy_server = _Proxy(client_uuid, conn)
                                    proxy_server.start()
                                    self.__epoll.register(conn.fileno(), select.EPOLLRDHUP)
                                    self.__proxy_clients[client_uuid] = proxy_server
                                    self.__proxy_fd[client_uuid] = conn.fileno()
                                except Exception as e:
                                    log.error(e)
                                    conn.close()
                            elif len(data) == Protocol.UUID_LENGTH * 2 + len(Protocol.DATA_SEPARATOR):
                                try:
                                    client_uuid, conn_uuid = data.split(Protocol.DATA_SEPARATOR)
                                    if client_uuid not in self.__proxy_clients.keys():
                                        raise ProxyError('Unclaimed connection<%s> for client<%s>.' %
                                                         (conn_uuid, client_uuid))
                                    self.__proxy_clients[client_uuid].on_connection_received(conn_uuid, conn)
                                except Exception as e:
                                    log.error(e)
                                    conn.close()
                            else:
                                log.warning('Unidentified connection from %s:%d with data: %s' %
                                            (address[0], address[1], data))
                                conn.close()
                        else:
                            raise ProxyError('Server will exit as listening socket(%d) failure.' % self.__port)
                    elif event & select.EPOLLRDHUP:
                        k = [k for k, v in self.__proxy_fd.items() if v == fd][0]
                        self.__epoll.unregister(fd)
                        self.__proxy_clients[k].stop()
                        del self.__proxy_clients[k]
                        del self.__proxy_fd[k]
                        log.error('Client<%s> removed.' % k)
        except Exception as e:
            log.critical(e)
        finally:
            self.__shutdown_service()
        self.set_status(ThreadedModule.Status.STOPPED)
        log.warning('Module<%s> Exited!' % current_thread().name)

    def restart(self):
        """不支持重启"""
        raise ProxyError('Module<%s> does NOT Support Restart!' % self.get_name())
