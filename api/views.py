import requests
from flask import jsonify, request
from main import app
from api.tasks import con, segment_user


@app.route('/', methods=['GET'])
def home():
    # Read all the data from the database
    return """<html>
  <head>
    <title>Welcome to my API</title>
  </head>
  <body>
    <h1>Welcome to my API</h1>
    <p>Please refer to the <a href="https://documenter.getpostman.com/view/21203353/2s93RNyaTX">documentation</a> for more information.</p>
  </body>
</html>"""


@app.route('/v1/users/', methods=['GET'])
def get_users_data():
    user_id = request.json['user_id']
    print(user_id)
    print(type(user_id))

    if user_id == '':
        all_users = con.execute("""
        SELECT * FROM users
        """).fetchdf()

    elif isinstance(user_id, list):
        all_users = con.execute(f"""
        SELECT * FROM users 
        WHERE id IN {tuple(user_id)};""").df()
    elif isinstance(user_id, int):
        all_users = con.execute(f"""
        SELECT * FROM users 
        WHERE id={user_id};""").fetchdf()
    else:
        return {'status': 400,
               'message': 'Type Error. user_id must be an integer or a list of integers'}

    result = {'status': 200,
              "data": [data._asdict() for data in all_users.itertuples(index=False)]
              }

    return jsonify(result), 200


@app.route('/v1/UserEvents/', methods=['GET'])
def get_user_events():

    user_id = request.json['user_id']
    # print(user_id)
    if isinstance(user_id, list):
        user = con.execute(f"""
            SELECT * 
            FROM UserEvents 
            WHERE user_id IN {tuple(user_id)};
        """).fetchdf()

    elif isinstance(user_id, int):
        user = con.execute(f"""
            SELECT * 
            FROM UserEvents 
            WHERE user_id={user_id};
        """).fetchdf()

    else:
        return {'status': 400,
                'message': 'Type Error. user_id must be an integer or a list of integers'}, 400

    result = {'status': 200,
              'data': [data._asdict() for data in user.itertuples(index=False)]}

    return jsonify(result), 200


@app.route('/v1/Fetch-Users/', methods=['GET'])
def fetch_users():

    # Check to make sure that the values are columns in the database

    print(request.json)

    result = segment_user(request.json)

    return jsonify(result)
