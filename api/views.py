import requests
from flask import jsonify, request
from main import app
from api.tasks import con


@app.route('/', methods=['GET'])
def home():
    # Read all the data from the database
    return "Welcome to my API. Please refer to the Documentation to learn more."


@app.route('/v1/users', methods=['GET'])
def get_users_data():
    all_users = con.execute("""
    SELECT * FROM users
    """).fetchdf()

    result = [data._asdict() for data in all_users.itertuples(index=False)]

    return jsonify(result)


@app.route('/v1/user/<id>', methods=['GET'])
def get_user(id):
    user = con.execute(f"""
        SELECT * 
        FROM users 
        WHERE ID={id};
    """).fetchdf()

    user_data = user.to_dict(orient='records')

    return jsonify(user_data)


@app.route('/v1/userevents/', methods=['GET'])
def get_user_events():
    args = request.args.to_dict()

    for key, value in args.items():
        print(f"{key} : {value}")

    # Check to make sure that the values are columns in the database

    user_events = con.execute(f"""
        SELECT * 
        FROM UserEvents 
        ORDER BY event_date ASC;
    """).fetchdf()

    user_events_data = user_events.to_dict(orient='records')

    return jsonify(user_events_data)
