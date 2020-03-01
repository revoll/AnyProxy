from flask import Blueprint, Flask, request, render_template, redirect, url_for, flash
from flask_login import LoginManager, UserMixin, login_required, login_user, logout_user
from flask_moment import Moment
from flask_mail import Mail
from flask_bootstrap import Bootstrap
from flask_app.config import Config


server_blueprint = Blueprint('server', __name__, url_prefix='/manage')
api_blueprint = Blueprint('api', __name__, url_prefix='/manage/api')

from . import server_views, api


bootstrap = Bootstrap()
mail = Mail()
moment = Moment()
login_manager = LoginManager()
login_manager.session_protection = 'strong'
login_manager.login_view = 'login'


class User(UserMixin):
    pass


users = {
    'root': {'username': 'root', 'password': 'root'},
    'admin': {'username': 'admin', 'password': 'admin'},
}


def create_app():
    app = Flask(__name__)

    app.config.from_object(Config)
    Config.init_app(app)

    bootstrap.init_app(app)
    mail.init_app(app)
    login_manager.init_app(app)
    moment.init_app(app)

    @login_manager.user_loader
    def user_loader(user_id):
        if user_id in users:
            current_user = User()
            current_user.id = user_id
            current_user.username = users[user_id]['username']
            return current_user
        else:
            return None

    @app.route('/login', methods=['GET', 'POST'])
    def login():
        url_next = request.args.get('next', url_for('index'), str)
        if request.method == 'POST':
            username = request.form.get('username')
            password = request.form.get('password')
            for key in users:
                if users[key]['username'] == username and users[key]['password'] == password:
                    current_user = User()
                    current_user.id = key
                    current_user.username = username
                    login_user(current_user)
                    return redirect(url_next)
            flash('用户名或密码错误！')
        return render_template('login.html', url_next=url_next)

    @app.route('/logout')
    @login_required
    def logout():
        logout_user()
        return redirect(url_for('login'))

    @app.route('/')
    def index():
        return redirect(url_for('server.index'))

    app.register_blueprint(server_blueprint)
    app.register_blueprint(api_blueprint)

    return app
