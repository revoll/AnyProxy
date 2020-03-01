class ProxyError(Exception):
    pass


class RunningStatus:
    """状态转移图： PREPARE ----> RUNNING (<==> PENDING) ----> STOPPED """
    PREPARE = 0
    RUNNING = 1
    PENDING = 3
    STOPPED = 5
