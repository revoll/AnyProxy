import json
import datetime
from . import api_blueprint as api
from flask import current_app, request
from flask_login import login_required


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")
        else:
            return json.JSONEncoder.default(self, obj)


@api.route('/get-clients-info')
def get_clients_info():
    return json.dumps(current_app.proxy_clients_info(), cls=DateEncoder)


@api.route('/get-server-info')
def get_server_info():
    return json.dumps(current_app.proxy_server_info(), cls=DateEncoder)


@api.route('/start-server')
@login_required
def start_server():
    current_app.proxy_execute(current_app.proxy_api.STARTUP_SERVER)
    return str(True)


@api.route('/stop-server')
@login_required
def stop_server():
    current_app.proxy_execute(current_app.proxy_api.SHUTDOWN_SERVER)
    return str(True)


@api.route('/restart-server')
@login_required
def restart_server():
    if current_app.proxy_is_running():
        current_app.proxy_execute(current_app.proxy_api.SHUTDOWN_SERVER)
    current_app.proxy_execute(current_app.proxy_api.STARTUP_SERVER)
    return str(True)


@api.route('/pause-client/<string:client_id>')
@login_required
def pause_client(client_id):
    current_app.proxy_execute(current_app.proxy_api.PAUSE_CLIENT, client_id)
    return str(True)


@api.route('/resume-client/<string:client_id>')
@login_required
def resume_client(client_id):
    current_app.proxy_execute(current_app.proxy_api.RESUME_CLIENT, client_id)
    return str(True)


@api.route('/remove-client/<string:client_id>')
@login_required
def remove_client(client_id):
    current_app.proxy_execute(current_app.proxy_api.REMOVE_CLIENT, client_id)
    return str(True)


@api.route('/pause-tcp/<int:server_port>')
@login_required
def pause_tcp_map(server_port):
    current_app.proxy_execute(current_app.proxy_api.PAUSE_TCP_MAP, server_port)
    return str(True)


@api.route('/resume-tcp/<int:server_port>')
@login_required
def resume_tcp_map(server_port):
    current_app.proxy_execute(current_app.proxy_api.RESUME_TCP_MAP, server_port)
    return str(True)


@api.route('/remove-tcp/<int:server_port>')
@login_required
def remove_tcp_map(server_port):
    current_app.proxy_execute(current_app.proxy_api.REMOVE_TCP_MAP, server_port)
    return str(True)


@api.route('/pause-udp/<int:server_port>')
@login_required
def pause_udp_map(server_port):
    current_app.proxy_execute(current_app.proxy_api.PAUSE_UDP_MAP, server_port)
    return str(True)


@api.route('/resume-udp/<int:server_port>')
@login_required
def resume_udp_map(server_port):
    current_app.proxy_execute(current_app.proxy_api.RESUME_UDP_MAP, server_port)
    return str(True)


@api.route('/remove-udp/<int:server_port>')
@login_required
def remove_udp_map(server_port):
    current_app.proxy_execute(current_app.proxy_api.REMOVE_UDP_MAP, server_port)
    return str(True)


@api.route('/reset-statistic')
@login_required
def reset_statistic():
    current_app.proxy_reset_statistic()
    return str(True)
