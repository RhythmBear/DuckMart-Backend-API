import duckdb
from datetime import datetime

con = duckdb.connect(database=':memory:', read_only=False)


def validate_date(raw_date):
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
                event_type VARCHAR,
                event_date DATE, 
    ); 
        COPY UserEvents FROM 'datasets/Duckmart-user-events.csv' (DELIMITER ',', HEADER );
    
    """)

    return con


def segment_user(json_segment: dict):
    json_payload = {
        "age": 'all',
        "gender": 'all',
        "location": 'all',
        "sign_up_date": {
            "start_date": "",
            "end_date": ""
        },
        "subscription_plan": 'all',
        "device_type": 'all',
        "events": 'all'

    }

    for key, value in json_segment.items():
        json_payload[key] = value

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
            if value in valid_values:
                user_query = con.execute(f"SELECT * from previous_query WHERE {key}='{value}'").df()
            else:
                return {"error": "Invalid value",
                        "status": 400,
                        "message": f"Invalid value, Please select from the list of valid values for {key}",
                        "valid_values": valid_values}

        elif key == "sign_up_date":
            # Check to make sure that a dictionary is provided
            if type(value) is dict:

                try:
                    start_date = validate_date(value["start_date"])

                except KeyError:
                    start_date = validate_date('1753-1-1')

                try:
                    end_date = validate_date(value["end_date"])

                except KeyError:
                    end_date = datetime.now().date()

                # if Both start and end dates are provided:
                if start_date is not None and end_date is not None:
                    user_query = con.execute(f"""SELECT * 
                                             FROM previous_query 
                                             WHERE sign_up_date BETWEEN '{start_date}' AND '{end_date}'""").df()

        elif key == "age":
            pass

    return user_query


con = load_data_from_db()
