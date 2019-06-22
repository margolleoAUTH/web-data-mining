from pymongo import MongoClient

# Handles the communication with the Mongo DB


class MyDBHandler:

    # Constructor - Initiates the connection with Mongo/twitter_data DB as client
    def __init__(self, my_connection_info):
        self.myClient = MongoClient(my_connection_info)
        print("Database Connection established successfully")

    # Inserts data/a tweet in collection
    def insert_data(self, collection, inserted_data):
        result = self.myClient.twitter_data[collection].insert_one(inserted_data)
        return result

    # Deletes all data from collection
    def delete_all_data(self, collection):
        self.myClient.twitter_data[collection].delete_many({})

    # Deletes data from collection based on time limit
    def delete_data(self, collection, timeLimit):
        self.myClient.twitter_data[collection].delete_many({"timing": {"$lte": timeLimit}})

    # Fetch data from collection
    def get_data(self, collection):
        cursor = self.myClient.twitter_data[collection].find({})
        result = []
        for document in cursor:
            result.append(document)
        return result

    # Fetch geo/sentiment results from collection
    def get_trends_data(self, collection):
        cursor = self.myClient.twitter_data[collection].find()
        result = []
        for document in cursor:
            del document["_id"]
            result.append(document)
        return result

    # Fetch sentiment results from collection group by username
    def get_vote_result_data(self, collection):
        pipeline = [
            {"$group": {"_id": "$user_name", "user_vote": {"$sum": "$sentiment_result"}}}
        ]
        cursor = self.myClient.twitter_data[collection].aggregate(pipeline)
        result = []
        for document in cursor:
            result.append(document)
        return result
