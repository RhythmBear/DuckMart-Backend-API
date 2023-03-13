import os
from dotenv import load_dotenv

# Load Environment variables
load_dotenv()

# Find the absolute file path to the top level directory
basedir = os.path.abspath(os.path.dirname(__file__))


class Config:
    """
    Base configuration class. Contains default configuration settings + configuration settings applicable to all environments.
    """
    # Default settings
    FLASK_ENV = 'development'
    DEBUG = False
    TESTING = False
    WTF_CSRF_ENABLED = True

    # Settings applicable to all environments
    SECRET_KEY = os.getenv('SECRET_KEY')

    MAIL_SERVER = 'smtp.googlemail.com'
    MAIL_PORT = 465
    MAIL_USE_TLS = False
    MAIL_USE_SSL = True
    # MAIL_USERNAME = os.getenv('MAIL_USERNAME', default='')
    # MAIL_PASSWORD = os.getenv('MAIL_PASSWORD', default='')
    # MAIL_DEFAULT_SENDER = os.getenv('MAIL_USERNAME', default='')
    # MAIL_SUPPRESS_SEND = False

    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL')
    # RESULT_BACKEND = os.getenv('RESULT_BACKEND')


class DevelopmentConfig(Config):
    DEBUG = True
    # SQLALCHEMY_DATABASE_URI = "sqlite:///" + os.path.join(basedir, 'development.db')


class TestingConfig(Config):
    TESTING = True
    WTF_CSRF_ENABLED = False
    MAIL_SUPPRESS_SEND = True
    # SQLALCHEMY_DATABASE_URI = "sqlite:///" + os.path.join(basedir, 'test.db')


class ProductionConfig(Config):
    FLASK_ENV = 'production'
    # SQLALCHEMY_DATABASE_URI = os.getenv('DATABASE_URL', default="sqlite:///" + os.path.join(basedir, 'instance/git.db'))