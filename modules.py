import uuid
from time import sleep
from threading import Thread


class ModuleError(Exception):
    pass


class ThreadedModule:

    class Status:
        DEFAULT = 0
        PENDING = 1
        RUNNING = 2
        STOPPED = 3

    @staticmethod
    def __default_thread():
        pass

    def __init__(self, module_name=None, module_uuid=None):
        self.__uuid = module_uuid if module_uuid else uuid.uuid4().hex
        self.__name = module_name if module_name else self.__uuid
        self.__thread_handler = None
        self.__thread_callback = ThreadedModule.__default_thread
        self.__status = ThreadedModule.Status.PENDING

    def get_uuid(self):
        return self.__uuid

    def set_uuid(self, module_uuid):
        if len(module_uuid) != 32:
            raise ModuleError('UUID should be 32 bits hex string.')
        self.__uuid = module_uuid

    def get_name(self):
        return self.__name

    def set_name(self, name):
        self.__name = name

    def get_status(self):
        return self.__status

    def set_status(self, status):
        self.__status = status

    def register_callback(self, fn_cb):
        self.__thread_callback = fn_cb

    def start(self):
        if self.get_status() != ThreadedModule.Status.PENDING:
            raise ModuleError('Module<%s> starts before initialization.' % self.get_name())
        self.__thread_handler = Thread(target=self.__thread_callback,
                                       name=self.__name,
                                       args=(),
                                       daemon=False)
        self.__thread_handler.start()
        self.set_status(ThreadedModule.Status.RUNNING)

    def stop(self):
        if self.get_status() != ThreadedModule.Status.RUNNING:
            return
        self.set_status(ThreadedModule.Status.STOPPED)
        self.__thread_handler.join()
        self.__thread_handler = None

    def restart(self):
        self.stop()
        self.__status = ThreadedModule.Status.PENDING
        self.start()

    def is_running(self):
        return self.__status == ThreadedModule.Status.RUNNING

    def wait_exit(self, timeout=-1):
        time_wait = 0.0
        while timeout == -1 or time_wait < timeout:
            if self.__status == ThreadedModule.Status.STOPPED:
                break
            sleep(0.5)
            time_wait += 0.5

    def _continue_running_thread(self):
        """用于线程函数中，判断是否需要适时退出线程执行"""
        return self.__status == ThreadedModule.Status.RUNNING
