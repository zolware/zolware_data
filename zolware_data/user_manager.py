

from bson.objectid import ObjectId

from zolware_data.data import database


class UserManager:

    def __init__(self):
        self.db = database.Database().get_db()
        self.users = self.db.users

    def find_user_by_id(self, user_id):
        user = self.users.find_one({'_id': ObjectId(user_id)})
        return user

    def find_user_by_email(self, email):
        user = self.users.find_one({'local.email': email})
        return user

    def find_user_by_username(self, username):
        user = self.users.find_one({'local.displayName': username})
        return user
