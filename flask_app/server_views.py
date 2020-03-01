from . import server_blueprint as server
from flask import render_template, current_app, request
from flask_login import login_required
from utils import stringify_bytes_val


server.add_app_template_global(len, 'len')
server.add_app_template_global(stringify_bytes_val, 'stringify_bytes_val')


@server.route('/')
@login_required
def index():
    servers = current_app.proxy_get_server_info()
    if len(servers) == 1:
        server_info = {}
        for s in servers:
            server_info = servers[s]
            break
    else:
        return "Error!"
    return render_template('index.html', server_info=server_info)


@server.route('/clients')
@login_required
def clients():
    return render_template('clients.html', clients=current_app.proxy_get_clients(None))


@server.route('/mappings')
@login_required
def mappings():
    cid = request.args.get('cid', None, type=str)
    return render_template('mappings.html', clients=current_app.proxy_get_clients(cid))
