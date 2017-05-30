from bson.objectid import ObjectId

from zolware_data.data import database


class User:
    
    def __init__(self, user_id):
        self.db = database.Database().get_db()
        self.users = self.db.users
        self.user = self.users.find_one({'_id': ObjectId(user_id)})

    def username(self):
        return self.datasource["local"]["disaplyName"]

    def findByUsername(self, username):
        return self.datasource["local"]["disaplyName"]