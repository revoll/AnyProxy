import select
import socket
from time import sleep
from threading import Thread, current_thread, Lock
from protocol import Protocol
from common import Module
from loguru import logger as log


class ClientProxy(Module):
    """
    客户端端口转发代理：连接映射管理器
    """
    MODULE_NAME = 'CLIENT_PROXY'

    def __init__(self, server_host='127.0.0.1', server_port=10000):
        super().__init__(ClientProxy.MODULE_NAME)
        self.__server_host = server_host
        self.__server_port = int(server_port)
        self.__port_mapping = {}
        self.__port_fds = {}
        self.__fd_to_socket = {}
        self.__fd_to_fd = {}
        self.__lock = Lock()
        self.__epoll = select.epoll()
        self.__t_flag = False
        self.__t_handler = None
        self.__protocol = Protocol(self.__server_host, self.__server_port, is_server=False)
        self.__protocol.register_process_callback(self.__protocol_cb)
        self.set_status(Module.Status.PREPARE)

    def __add_connection_pair(self, port):
        try:
            conn1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn1.connect(('', port))
        except socket.error as e:
            log.error('%s. Failed to connect local_port(%d).' % (e, port))
            return None, None
        try:
            conn2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn2.connect((self.__server_host, self.__server_port))
        except socket.error as e:
            log.error('%s. Failed to connect %s:%d.', (e, self.__server_host, self.__server_port))
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
        port = conn1.getpeername()[1]
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

    def __protocol_cb(self, cmd, data):
        """
         客户端与服务端的通信服务线程函数：处理服务端的请求
        :param cmd:
        :param data:
        :return:
        """
        if cmd == Protocol.Command.PING:
            log.info('PING!')
            return Protocol.Response.SUCCESS
        elif cmd == Protocol.Command.CHECK_MAPPING_PORTS:
            port_mapping = eval(data)
            for port in list(port_mapping.keys()):
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect(('', port))
                except ConnectionRefusedError:
                    del port_mapping[port]
            with self.__lock:
                self.__port_fds[self.__server_port] = [None]
                for port in port_mapping.keys():
                    self.__port_fds[port] = [None]
                self.__port_mapping = port_mapping
            return str(port_mapping)
        elif cmd == Protocol.Command.ADD_NEW_CONNECTION:
            conn1, conn2 = self.__add_connection_pair(port=int(data))
            log.debug('PORT_CHAIN(%d->%d->%d->%d), FILE_NO(%d->%d)' %
                      (conn2.getpeername()[1], conn2.getsockname()[1],
                       conn1.getsockname()[1], conn1.getpeername()[1],
                       conn2.fileno(), conn1.fileno()))
            log.info('Local(%s), Remote(%s)' % (conn2.getsockname(), conn2.getpeername()))
            return '%s:%d' % conn2.getsockname()
        else:
            return Protocol.Response.INVALID

    def __thread_main(self):
        """
        数据传输线程函数
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
                        if self.__remove_connection_pair(fd):
                            log.debug('Connection(fd=%d) closed, and connection pair removed.' % fd)
                        else:
                            log.warning('Connection(fd=%d) closed, but connection pair not removed.' % fd)
                    else:
                        sock_src = self.__fd_to_socket[fd]
                        sock_dst = self.__fd_to_socket[self.__fd_to_fd[fd]]
                        data = sock_src.recv(Protocol.SOCKET_BUFFER_SIZE)
                        if data:
                            sock_dst.sendall(data)
        except Exception as e:
            log.critical(e)
        finally:
            for key in self.__port_fds[self.__server_port][1:]:
                self.__remove_connection_pair(key)
        log.warning('%s exited.' % current_thread().name)
        self.__t_flag = False
        self.set_status(Module.Status.STOPPED)

    def start(self):
        super().start()
        self.__protocol.start()

        log.info('Waiting for the server...')
        while not self.__port_mapping:
            sleep(1)
        log.info('Confirmed mapping ports with the server: %s' % self.__port_mapping)

        self.__t_handler = Thread(None, self.__thread_main, 'Thread-ProxyMain', (), daemon=False)
        self.__t_handler.start()
        self.set_status(Module.Status.RUNNING)

    def stop(self):
        self.set_status(Module.Status.STOPPED)
        if self.__t_flag:
            self.__t_flag = False
            self.__t_handler.join()
        self.__protocol.stop()
        super().stop()

    def is_healthy(self):
        return super().is_healthy() and self.__protocol.is_healthy()
