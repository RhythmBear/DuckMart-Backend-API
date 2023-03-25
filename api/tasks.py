import duckdb
from datetime import datetime

con = duckdb.connect(database=':memory:', read_only=False)


def validate_date(raw_date):
    print(raw_date)
    try:
        start_date_obj = datetime.strptime(raw_date, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect date format, should be YYYY-MM-DD")

    return start_date_obj


def load_data_from_db():
    """Loads data from the database and returns the connection to the database"""

    # Create User Table and Add the age column from the date_of_birth column
    con.execute("""
        CREATE TABLE IF NOT EXISTS Users(id INTEGER, 
                full_name VARCHAR,
                date_of_birth DATE,
                gender VARCHAR,
                location VARCHAR,
                sign_up_date DATE,
                subscription_plan VARCHAR,
                device_type VARCHAR, );
        COPY Users FROM 'datasets/Duckmart-user.csv' (DELIMITER ',', HEADER );
        ALTER TABLE Users ADD COLUMN age INTEGER ;
        UPDATE Users SET age = DATE_PART('year', age(date_of_birth));

    """)

    # Create UserEvents Table
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS UserEvents(id INTEGER,
                user_id INTEGER,
                event_name VARCHAR,
                event_date DATE, 
    ); 
        COPY UserEvents FROM 'datasets/Duckmart-User-Events.csv' (DELIMITER ',', HEADER );
    
    """)

    return con


def segment_user(user_json_segment: dict):

    json_payload = {
        "age": {'min': 0,
                'max': 1000
                },
        "gender": 'all',
        "location": 'all',
        "sign_up_date": {"start_date": validate_date('1953-1-1'),
                         "end_date": datetime.now().date()
                         },
        "subscription_plan": 'all',
        "device_type": 'all',
        "events": {'type': {'min': 0, 'max': 10}}

    }

    # Here I'm checking to make sure that the format of the json provided matches the required format i.e the
    # keys should all be similar
    for key, value in user_json_segment.items():
        if key not in json_payload.keys():
            return {"error": "Invalid key",
                    "status": 400,
                    "message": f"{key} is not a valid key. Please select from the list of valid keys",
                    "valid_keys": list(json_payload.keys())}

        # This line of code might look a bit confusing but here is what is happening, we are looping through the values
        # in the provided json_segment that are dictionaries and changing the provided values to fit our json segment.
        if type(value) == dict:

            # If i do not add this if statement to check whenever the user sends an events dictionary, the code will add
            # it to the existing dictionary instead of replacing the default
            if key == "events":
                json_payload[key] = value
            else:
                for key_i, value_i in value.items():
                    json_payload[key][key_i] = value_i

        else:
            json_payload[key] = value

    print(json_payload)

    user_query = con.execute("SELECT * FROM users;").df()

    for key, value in json_payload.items():
        # Check to make sure the value is not 'all'
        if value == 'all':
            continue

        # Register Previous query as a view
        con.register("previous_query", user_query)

        if key in ["gender", "location", "device_type", "subscription_plan"]:
            # Get valid values
            valid_values = list(con.execute(f"SELECT DISTINCT({key}) FROM users; ").fetchdf()[key])
            if value in valid_values or set(value).issubset(valid_values):
                if isinstance(value, str):
                    user_query = con.execute(f"SELECT * from previous_query WHERE {key}='{value}'").df()
                elif type(value) == list:
                    print("list here")
                    user_query = con.execute(f"""SELECT * from previous_query WHERE {key} IN {tuple(value)}""").df()
                else:
                    return {"error": "Invalid DataType",
                            "status": 400,
                            "message": f"Invalid Data type, Please pass in a String or a List of Strings {key}"}

            else:
                return {"error": "Invalid value",
                        "status": 400,
                        "message": f"Invalid value, Please select from the list of valid values for {key}",
                        "valid_values": valid_values}

        elif key == "sign_up_date":
            # Check to make sure that a dictionary is provided
            if type(value) is dict:

                if value['start_date'] is not None and value['end_date'] is not None:
                    user_query = con.execute(f"""SELECT * 
                                             FROM previous_query 
                                             WHERE sign_up_date BETWEEN '{value['start_date']}' AND '{value['end_date']}'""").df()

            else:
                return {'message': "Invalid DataType, please make sure you enter a dictionary",
                        "status": 400,

                }

        elif key == "age":
            if type(value) == dict:
                user_query = con.execute(f"""
                                         SELECT * 
                                         FROM previous_query 
                                         WHERE age BETWEEN '{value["min"]}' AND '{value["max"]}'""").df()

            else:
                return {'message': "Invalid DataType, please make sure you enter a Dictionary",
                        'status': 400}

        elif key == "events":
            if 'type' in value.keys():
                continue

            event_user_ids = []

            for event_key, event_value in value.items():
                try:
                    min_val = event_value["min"]
                except KeyError as err:
                    min_val = 0

                try:
                    max_val = event_value["max"]
                except KeyError as err:
                    max_val = 1000

                # print(event_key, event_value)
                # print("I got here")
                user_ids = con.execute(f"""
                                    SELECT user_id, count(*) As event_count
                                    FROM UserEvents
                                    WHERE event_name='{event_key}'
                                    GROUP BY user_id
                                    HAVING event_count BETWEEN {min_val} AND {max_val}
                                    ORDER BY user_id;
                                    """).df()
                print(user_ids)
                event_user_ids.append(list(user_ids['user_id']))

            print(event_user_ids)
            # Get all the User ID's that appear in all the lists
            common_ids = set(event_user_ids[0])
            for sublist in event_user_ids:
                common_ids = common_ids.intersection(set(sublist))

            user_query = con.execute(f"""SELECT p.ID 
                                                 FROM previous_query AS p 
                                                 WHERE p.ID IN {tuple(common_ids)} """).df()
        else:
            return {'message': "Invalid DataType, please make sure you enter a Dictionary",
                    'status': 400}

    result = {'message': "Success",
              'status': 200,
              'data': list(user_query['id'])}
    return result


con = load_data_from_db()
