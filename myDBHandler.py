from pymongo import MongoClient


class MyDBHandler:

    def __init__(self, my_connection_info):
        self.myClient = MongoClient(my_connection_info)
        print("Database Connection established successfully")

    def insert_data(self, collection, inserted_data):
        result = self.myClient.twitter_data[collection].insert_one(inserted_data)
        return result

    def delete_all_data(self, collection):
        self.myClient.twitter_data[collection].delete_many({})

    def delete_data(self, collection, timeLimit):
        self.myClient.twitter_data[collection].delete_many({"timing": {"$lte": timeLimit}})

    def get_data(self, collection):
        cursor = self.myClient.twitter_data[collection].find({})
        result = []
        for document in cursor:
            result.append(document)
        return result

    def get_sentiment_trends_data(self, collection):
        cursor = self.myClient.twitter_data[collection].find()
        result = []
        for document in cursor:
            result.append({"votes": document["votes"], "timing": document["timing"]})
        return result

    def get_trends_data(self, collection):
        cursor = self.myClient.twitter_data[collection].find()
        result = []
        for document in cursor:
            del document["_id"]
            result.append(document)
        return result

    def get_vote_result_data(self, collection):
        pipeline = [
            {"$group": {"_id": "$user_name", "user_vote": {"$sum": "$sentiment_result"}}}
        ]
        cursor = self.myClient.twitter_data[collection].aggregate(pipeline)
        result = []
        for document in cursor:
            result.append(document)
        return result
