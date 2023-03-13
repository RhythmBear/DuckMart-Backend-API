from api import db
from sqlalchemy import Column, Integer, Sequence, String, create_engine, DateTime


class User(db):
    __tablename__ = "User"

    id = Column(Integer, primary_key=True, nullable=False)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    email = Column(String, nullable=False)
    gender = Column(String, nullable=False)
    location = Column(String, nullable=False)
    sign_up_date = Column(DateTime, nullable=False)
    subscription_plan = Column(String, nullable=False)
    device_type = Column(String, nullable=False)

    @property
    def serialize(self):
        """ Return Object data in json format """
        return {
            'id': self.id,
            'Full Name': f"{self.first_name} {self.last_name}",
            'email': self.email,
            'gender': self.gender,
            'location': self.location,
            'sign_up_date': self.sign_up_date,
            'subscription_plan': self.subscription_plan,
            'device_type': self.device_type
        }

    # def __repr__(self):
    #     return f"User {self.first_name} {self.last_name})"


class UserEvents(db):
    __tablename__ = "UserEvents"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=False)
    event_name = Column(String, nullable=False)
    timestamp = Column(DateTime)

    def serialize(self):
        """ Returns data in a json format"""
        return {
            'id': self.id,
            'user_id': self.user_id,
            'event_name': self.event_name,
            'timestamp': self.timestamp
        }

    def __repr__(self):
        return f"< UserEvent {self.user_id} event_name {self.event_name} >"
