from . import server_blueprint as server
from flask import render_template, current_app, request
from flask_login import login_required
from utils import stringify_bytes_val


server.add_app_template_global(len, 'len')
server.add_app_template_global(stringify_bytes_val, 'stringify_bytes_val')


@server.route('/')
@login_required
def index():
    server_info = current_app.proxy_server_info()
    return render_template('index.html', server_info=server_info)


@server.route('/clients')
@login_required
def clients():
    clients_info = current_app.proxy_clients_info()
    return render_template('clients.html', clients=clients_info)


@server.route('/mappings')
@login_required
def mappings():
    cid = request.args.get('cid', None, type=str)
    clients_info = current_app.proxy_clients_info(cid)
    pos_index = []
    for cid in clients_info:
        for port in clients_info[cid]['tcp_maps']:
            pos_index.append((port, cid, 'tcp'))
        for port in clients_info[cid]['udp_maps']:
            pos_index.append((port, cid, 'udp'))
    pos_index.sort(key=lambda m: m[0], reverse=False)
    return render_template('mappings.html', clients=clients_info, index=pos_index)
