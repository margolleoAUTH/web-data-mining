from pymongo import MongoClient
from myMongoListener import MyMongoListener


class MyDBHandler:

    def __init__(self, my_connection_info, my_connection_info_replica):
        self.myClient = MongoClient(my_connection_info, event_listeners=[MyMongoListener(self, 30)])
        self.myClientReplica = MongoClient(my_connection_info_replica)
        self.myDb = self.myClient.twitter_data
        self.myDbReplica = self.myClientReplica.twitter_data
        print("Database Connection established successfully")

    def insert_data(self, collection, inserted_data):
        result = self.myDb[collection].insert_one(inserted_data)
        return result

    def get_data(self, collection):
        cursor = self.myDbReplica[collection].find().limit(150)
        result = []
        for document in cursor:
            result.append(document)
        return result
