import os


class Config:
    """ Application Configurations """
    SECRET_KEY = os.environ.get(u'SECRET_KEY') or os.urandom(24)
    SSL_DISABLE = True

    MAIL_SERVER = os.environ.get(u'MAIL_SERVER')
    MAIL_PORT = int(os.environ.get(u'MAIL_PORT') or u'25')
    MAIL_USERNAME = os.environ.get(u'MAIL_USERNAME')
    MAIL_PASSWORD = os.environ.get(u'MAIL_PASSWORD')
    MAIL_SUBJECT_PREFIX = u'[PySite]'
    MAIL_SENDER = u'PySite Admin <%s>' % os.environ.get(u'MAIL_USERNAME')
    MAIL_NOTIFICATION = u'MAIL NOTIFICATION BOYD HERE....'

    @staticmethod
    def init_app(app):
        pass
