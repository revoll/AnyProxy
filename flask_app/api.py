import json
import datetime
from . import api_blueprint as api
from flask import current_app
from flask_login import login_required


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")
        else:
            return json.JSONEncoder.default(self, obj)


@api.route('/get_clients')
def get_clients():
    return json.dumps(current_app.proxy_get_clients(), cls=DateEncoder)


@api.route('/start-server')
@login_required
def start_server():
    current_app.proxy_start_server()
    return str(True)


@api.route('/stop-server')
@login_required
def stop_server():
    current_app.proxy_stop_server()
    return str(True)


@api.route('/restart-server')
@login_required
def restart_server():
    if current_app.proxy_is_server_running():
        current_app.proxy_stop_server()
    current_app.proxy_start_server()
    return str(True)


@api.route('/pause-client/<string:client_id>')
@login_required
def pause_client(client_id):
    current_app.proxy_pause_client(client_id)
    return str(True)


@api.route('/resume-client/<string:client_id>')
@login_required
def resume_client(client_id):
    current_app.proxy_resume_client(client_id)
    return str(True)


@api.route('/remove-client/<string:client_id>')
@login_required
def remove_client(client_id):
    current_app.proxy_remove_client(client_id)
    return str(True)


@api.route('/start-tcp-map/<int:server_port>')
@login_required
def start_tcp_map(server_port):
    current_app.proxy_start_tcp_map(server_port)
    return str(True)


@api.route('/stop-tcp-map/<int:server_port>')
@login_required
def stop_tcp_map(server_port):
    current_app.proxy_stop_tcp_map(server_port)
    return str(True)


@api.route('/remove-tcp-map/<int:server_port>')
@login_required
def remove_tcp_map(server_port):
    current_app.proxy_remove_tcp_map(server_port)
    return str(True)


@api.route('/start-udp-map/<int:server_port>')
@login_required
def start_udp_map(server_port):
    current_app.proxy_start_udp_map(server_port)
    return str(True)


@api.route('/stop-udp-map/<int:server_port>')
@login_required
def stop_udp_map(server_port):
    current_app.proxy_stop_udp_map(server_port)
    return str(True)


@api.route('/remove-udp-map/<int:server_port>')
@login_required
def remove_udp_map(server_port):
    current_app.proxy_remove_udp_map(server_port)
    return str(True)


@api.route('/reset_statistic')
@login_required
def reset_statistic():
    current_app.proxy_reset_statistic()
    return str(True)
