from sqlalchemy.orm.session import Session
from sqlalchemy import create_engine
from api.models import db
from flask import Flask
import os
import config


def create_api():

    # Create and Configure Flask App
    app = Flask(__name__)
    CONFIG_TYPE = os.getenv('CONFIG_TYPE', default=config.DevelopmentConfig)
    app.config.from_object(CONFIG_TYPE)

    return app


# def initialize_db():
#     global db
#     # Read the CSV files and convert to a pandas dataframe
#     user_df = pd.read_csv("../datasets/Duckmart-user.csv")
#     user_events_df = pd.read_csv("../datasets/Duckmart-User-Events.csv")
#
#     # # Create a connection to the duckdb Engine
#     # eng = create_engine("duckdb://")
#     # db.metadata.create_all(eng)
#     #
#     # # Create Session Object for queries
#     # session = Session(bind=eng)
#     #
#     # # write the DataFrame to the DuckDB databasethon
#     # # user_df.to_sql('User', eng, if_exists='replace', index=False)
#     # user_events_df.to_sql('UserEvents', eng, if_exists='replace', index=False)
#
#     return session

app = create_api()

from . import views
