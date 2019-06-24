from tweepy.streaming import StreamListener
import json
import time

# Handles the communication with the Tweeter and stores the raw data in the database


class MyStreamHandler(StreamListener):

    # Constructor - Initiates an instance of Database Handler
    # and extends the StreamListener to perform the Tweeter communication
    def __init__(self, databaseHandler):
        super().__init__()
        self.databaseHandler = databaseHandler

    # Event listener that fires
    # when a tweet matches with the filter of Stream instance that Stream Listener has been initiated
    def on_data(self, data):
        try:
            # Additional checks: These attributes are here for future purposes.
            # We plan to take the advantage of them to improve our results
            data = json.loads(data)
            tweet_language = data["lang"]
            timestamp = data["timestamp_ms"]
            user_name = data["user"]["name"]
            user_location = data["user"]["location"]
            user_description = data["user"]["description"]

            if tweet_language == "en" and timestamp is not None and user_name is not None and user_location is not None \
                    and user_description is not None and (int(timestamp) > (time.time() - 1728000000)):

                # Web Solution for full text of the tweet ==============================================================
                # If data have retweeted_status => inside retweeted_status may exist the full_text
                if "retweeted_status" in data:
                    data = data["retweeted_status"]

                # In data we may have the pure data or as data we have the retweeted_status
                # Firstly as tweet we establish the simple text.
                # If there is extended_tweet in data, there is the full_text
                raw_text = data["text"]
                if "extended_tweet" in data:
                    raw_text = data["extended_tweet"]["full_text"]
                # ======================================================================================================

                result = {"raw_text": raw_text, "text": raw_text, "user_name": user_name,
                          "user_location": user_location, "user_description": user_description,
                          "timing": int(round(time.time() * 1000))}

                self.databaseHandler.insert_data("tweets", result)

            return True
        except BaseException as error:
            print("===================================================================================================")
            print("Error on_data: %s" % str(error))
            print("===================================================================================================")
        return True

    # Event listener that fires when an error take place of Stream instance that Stream Listener has been initiated
    def on_error(self, status):
        if status == 420:
            return False
        print("___________________________________________________________________________________________________")
        print(status)
        print("___________________________________________________________________________________________________")