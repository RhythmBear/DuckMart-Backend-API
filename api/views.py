import requests
from flask import jsonify, request
from api import app, session
from api.models import User, UserEvents


@app.route('/')
def home():
    # Read all the data from the database
    all_data = session.query(User).all()

    # print(all_data[:5])
    # Sends in the first 10 items in the database
    return jsonify([data.serialize for data in all_data[:10]])

