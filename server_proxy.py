import socket
import select
from time import sleep
from threading import Thread, current_thread, Lock
from protocol import Protocol
from common import Module
from loguru import logger as log


class ConnectionListManager:
    """
    客户端Socket连接队列管理器
    """
    def __init__(self):
        self.__list = []
        self.__lock = Lock()

    def put(self, obj):
        with self.__lock:
            self.__list.append(obj)

    def get(self, host, port, timeout=15):
        while timeout >= 0:
            # TODO: Add handshake protocol to verify each connections.
            # for sock in list(self.__list):
            #     address = sock.getpeername()
            #     if host == address[0] and port == address[1]:
            #         with self.__lock:
            #             self.__list.remove(sock)
            #         return sock
            if len(list(self.__list)) > 0:
                with self.__lock():
                    sock = self.__list[0]
                    self.__list.remove(sock)
                    return sock
            sleep(0.5)
            timeout -= 0.5
        return None


class ServerProxy(Module):
    """
    服务端端口转发代理：映射端口管理器
    """
    MODULE_NAME = 'SERVER_PROXY'

    def __init__(self, port_mapping, server_host='127.0.0.1', server_port=10000):
        super().__init__(ServerProxy.MODULE_NAME)
        self.__server_host = server_host
        self.__server_port = int(server_port)
        if port_mapping:
            self.config_port_mapping = port_mapping
        else:
            log.error('Invalid parameter for port_mapping: %s' % port_mapping)
            exit(0)
        self.__port_mapping = {}
        self.__port_fds = {}
        self.__fd_to_socket = {}
        self.__fd_to_fd = {}
        self.__lock = Lock()
        self.__epoll = select.epoll()
        self.__epoll_service = select.epoll()
        self.__t_flag = False
        self.__t_handler = None
        self.__t_flag_conn = False
        self.__t_handler_conn = None
        self.__manager = ConnectionListManager()
        self.__protocol = Protocol(self.__server_host, self.__server_port, is_server=True)
        self.set_status(Module.Status.PREPARE)

    def __add_listening(self, server_port, client_port):
        if server_port == self.__server_port and self.__server_port in self.__port_fds:
            log.error('Port(%d) for server is already in listening.' % server_port)
            return False
        if server_port in self.__port_mapping.keys():
            log.error('Port(%d) is already listed in listening table.' % server_port)
            return False
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.__server_host, server_port))
        except socket.error:
            pass
        else:
            log.error('Port(%d) is already in use.' % server_port)
            return False
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.__server_host, server_port))
            s.listen(Protocol.MAX_CONNECTIONS)
            s.setblocking(False)
        except socket.error:
            log.error('Port(%d) binding failed.' % server_port)
            return False
        with self.__lock:
            if server_port != self.__server_port:
                self.__port_mapping[server_port] = client_port
            self.__port_fds[server_port] = [s.fileno()]
            self.__fd_to_socket[s.fileno()] = s
        if server_port == self.__server_port:
            self.__epoll_service.register(s.fileno(), select.EPOLLIN | select.EPOLLRDHUP)
        else:
            self.__epoll.register(s.fileno(), select.EPOLLIN | select.EPOLLRDHUP)
        return True

    def __remove_listening(self, server_port):
        if server_port not in self.__port_fds:
            log.warning('Port(%d) is not in listening list.' % server_port)
            return False
        for fd in self.__port_fds[server_port][1:]:
            self.__remove_connection_pair(fd)
        if len(self.__port_fds[server_port]) > 1:
            log.warning('Port(%d) connections was not completely closed.' % server_port)
            return False
        sock_fd = self.__port_fds[server_port][0]
        if server_port == self.__server_port:
            self.__epoll_service.unregister(sock_fd)
        else:
            self.__epoll.unregister(sock_fd)
        self.__fd_to_socket[sock_fd].close()
        with self.__lock:
            self.__fd_to_socket[sock_fd].close()
            del self.__fd_to_socket[sock_fd]
            self.__port_fds[server_port].remove(sock_fd)
            del self.__port_fds[server_port]
            if server_port != self.__server_port:
                del self.__port_mapping[server_port]
        return True

    def __add_connection_pair(self, port):
        try:
            conn1, address = self.__fd_to_socket[self.__port_fds[port][0]].accept()
        except socket.error:
            log.error('Accept user connection failed.(STEP_1, PORT=%d)' % port)
            return None, None
        try:
            resp = self.__protocol.execute(Protocol.Command.ADD_NEW_CONNECTION, self.__port_mapping[port])
            address = resp.split(':')  # e.g. "45.78.31.135:39478"
            if len(address) == 2:
                conn2 = self.__manager.get(host=address[0], port=int(address[1]), timeout=10)
            else:
                raise Exception('Client side establishing failed.(STEP_2, PORT=%d)' % port)
            if not conn2:
                raise Exception('Peer connection not found.(STEP_3, PORT=%d)' % port)
        except Exception as e:
            log.error(e)
            conn1.close()
            return None, None
        fd1 = conn1.fileno()
        fd2 = conn2.fileno()
        conn1.setblocking(False)
        conn2.setblocking(False)
        with self.__lock:
            self.__port_fds[port].append(fd1)
            self.__port_fds[self.__server_port].append(fd2)
            self.__fd_to_socket[fd1] = conn1
            self.__fd_to_socket[fd2] = conn2
            self.__fd_to_fd[fd1] = fd2
            self.__fd_to_fd[fd2] = fd1
        self.__epoll.register(fd1, select.EPOLLIN | select.EPOLLRDHUP)
        self.__epoll.register(fd2, select.EPOLLIN | select.EPOLLRDHUP)
        return conn1, conn2

    def __remove_connection_pair(self, key_fd):
        if key_fd in self.__port_fds[self.__server_port][1:]:
            fd2 = key_fd
            fd1 = self.__fd_to_fd[key_fd]
        elif key_fd in [x for t in self.__port_fds.values() for x in t[1:]]:
            fd1 = key_fd
            fd2 = self.__fd_to_fd[key_fd]
        else:
            log.error('Connection(fd=%d) to be removed not found.' % key_fd)
            return False
        conn1 = self.__fd_to_socket[fd1]
        conn2 = self.__fd_to_socket[fd2]
        port = conn1.getsockname()[1]
        self.__epoll.unregister(fd1)
        self.__epoll.unregister(fd2)
        with self.__lock:
            del self.__fd_to_fd[fd1]
            del self.__fd_to_fd[fd2]
            del self.__fd_to_socket[fd1]
            del self.__fd_to_socket[fd2]
            self.__port_fds[port].remove(fd1)
            self.__port_fds[self.__server_port].remove(fd2)
        conn1.close()
        conn2.close()
        return True

    def __thread_main(self):
        """
        主线程函数：负责数据转发和连接管理
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
                    if fd in [x[0] for x in self.__port_fds.values()]:
                        sock = self.__fd_to_socket[fd]
                        port = sock.getsockname()[1]
                        if event & ~select.EPOLLIN:
                            if self.__remove_listening(port):
                                log.error('Listening port(%d) closed, and connections removed.' % port)
                            else:
                                log.error('Listening port(%d) closed, but connections not removed.' % port)
                        else:
                            conn1, conn2 = self.__add_connection_pair(port)
                            if conn1 and conn2:
                                log.debug('PORT_CHAIN(%d->%d->%d->%d), FILE_NO(%d->%d)' %
                                          (conn1.getpeername()[1], conn1.getsockname()[1],
                                           conn2.getsockname()[1], conn2.getpeername()[1],
                                           conn1.fileno(), conn2.fileno()))
                                log.info('Local(%s), Remote(%s)' % (conn2.getsockname(), conn2.getpeername()))
                            else:
                                log.error('Failed to establish connection pair on port(%d)' % port)
                    elif event & ~select.EPOLLIN:
                        if self.__remove_connection_pair(fd):
                            log.debug('Connection(fd=%d) closed, and connection pair removed.' % fd)
                        else:
                            log.error('Connection(fd=%d) closed, but connection pair not removed.' % fd)
                    else:
                        sock_src = self.__fd_to_socket[fd]
                        sock_dst = self.__fd_to_socket[self.__fd_to_fd[fd]]
                        data = sock_src.recv(Protocol.SOCKET_BUFFER_SIZE)
                        if data:
                            sock_dst.sendall(data)
        except Exception as e:
            log.critical(e)
        finally:
            for server_port in list(self.__port_mapping.keys()):
                self.__remove_listening(server_port)
            self.__remove_listening(self.__server_port)
        log.warning('%s exited.' % current_thread().name)
        self.__t_flag = False
        self.set_status(Module.Status.STOPPED)

    def __thread_conn(self):
        """
        服务端口监听线程：负责在服务端口监听来自客户端的新连接
        :return:
        """
        self.__t_flag_conn = True
        log.info('%s started.' % current_thread().name)
        try:
            sock = self.__fd_to_socket[self.__port_fds[self.__server_port][0]]
            while self.__t_flag_conn:
                events = self.__epoll_service.poll(1)
                if not events:
                    continue
                for fd, event in events:
                    if event & ~select.EPOLLIN:
                        sock.close()
                        raise Exception('Connection closed unexpectedly.')
                    else:
                        conn, address = sock.accept()
                        self.__manager.put(conn)
        except Exception as e:
            log.critical(e)
        log.warning('%s exited.' % current_thread().name)
        self.__t_flag_conn = False
        self.set_status(Module.Status.STOPPED)

    def start(self):
        super().start()
        self.__protocol.start()

        self.__add_listening(self.__server_port, None)
        log.info('Prepare to Map: %s' % self.config_port_mapping)
        port_mapping = dict(self.config_port_mapping)
        for client_port in self.config_port_mapping:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((self.__server_host, port_mapping[client_port]))
            except socket.error:
                pass
            else:
                del port_mapping[client_port]
        log.info('Server can Map: %s' % port_mapping)
        resp = self.__protocol.execute(Protocol.Command.CHECK_MAPPING_PORTS, str(port_mapping))
        port_mapping = eval(resp)
        for client_port, server_port in port_mapping.items():
            self.__add_listening(server_port, client_port)
        log.info('Finally Mapped: %s' % port_mapping)
        
        self.__t_handler = Thread(None, self.__thread_main, 'Thread-ProxyMain', (), daemon=False)
        self.__t_handler_conn = Thread(None, self.__thread_conn, 'Thread-ProxyConn', (), daemon=False)
        self.__t_handler_conn.start()
        self.__t_handler.start()
        self.set_status(Module.Status.RUNNING)

    def stop(self):
        self.set_status(Module.Status.STOPPED)
        if self.__t_flag:
            self.__t_flag = False
            self.__t_handler.join()
        if self.__t_flag_conn:
            self.__t_flag_conn = False
            self.__t_handler_conn.join()
        self.__protocol.stop()
        super().stop()

    def is_healthy(self):
        return super().is_healthy() and self.__protocol.is_healthy()
