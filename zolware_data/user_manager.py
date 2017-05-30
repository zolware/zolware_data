import json

from bson.objectid import ObjectId

from zolware_data.data import database
from zolware_data.models import user


class UserManager:

    def __init__(self):
        self.db = database.Database().get_db()
        self.users = self.db.users

    def find_user_by_id(self, user_id):
        user_dict = self.users.find_one({'_id': ObjectId(user_id)})
        return user.User(user_dict)

    def find_user_by_email(self, email):
        user_dict = self.users.find_one({'local.email': email})
        return user.User(user_dict)

    def find_user_by_username(self, username):
        user = self.users.find_one({'local.displayName': username})
        return user

   # def __make_user__(self, user_dict):
