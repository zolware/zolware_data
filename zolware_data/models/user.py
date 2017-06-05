from bson.objectid import ObjectId

from zolware_data.data import database


class User:
    
    def __init__(self, user=None):
        if user is not None:
            self.user = user
            self.username = self.user["local"]["displayName"]
            self.token = self.user["token"]

        self.db = None
        self.users_table = None

    def fetch(self, user_id):
        self.db = database.Database().get_db()
        self.users_table = self.db.users
        self.user = self.users_table.find_one({'_id': ObjectId(user_id)})
