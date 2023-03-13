import requests
from flask import jsonify, request
from api import app, session
from api.models import User, UserEvents

@app.route('/')
def home():
    all_data = session.query(User).all()
    print(all_data)

    return jsonify([data.serialize for data in all_data])

