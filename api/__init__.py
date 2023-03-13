from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import Session
from sqlalchemy import create_engine
from api.tasks import user_df, user_events_df
from flask import Flask
import os
import config
import pandas as pd


# Create and Configure Flask App
app = Flask(__name__)
CONFIG_TYPE = os.getenv('CONFIG_TYPE', default=config.DevelopmentConfig)
app.config.from_object(CONFIG_TYPE)

# Create and Connect to database
db = declarative_base()

# Create a connection to the duckdb Engine
eng = create_engine("duckdb://")
db.metadata.create_all(eng)

# Create Session Object for queries
session = Session(bind=eng)

# write the DataFrame to the DuckDB databasethon
user_df.to_sql('User', eng, if_exists='replace', index=False)
user_events_df.to_sql('UserEvents', eng, if_exists='replace', index=False)

from api import views
