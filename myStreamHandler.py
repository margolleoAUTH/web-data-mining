from tweepy.streaming import StreamListener
import json
import time


class MyStreamHandler(StreamListener):

    def __init__(self, databaseHandler):
        super().__init__()
        self.databaseHandler = databaseHandler
        self.count = 0

    def on_data(self, data):
        try:
            # Additional checks: Country somehow must be USA
            data = json.loads(data)
            tweet_language = data["lang"]
            timestamp = data["timestamp_ms"]
            user_name = data["user"]["name"]
            user_location = data["user"]["location"]
            user_description = data["user"]["description"]

            if tweet_language == "en" and timestamp is not None and user_name is not None and user_location is not None \
                    and user_description is not None and (int(timestamp) > (time.time() - 1728000000)):

                # self.count = self.count + 1

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

                # ACTION ITEM: STOP-WORDS with extended list to support the removal of articles, pronouns, etc
                # stop_words = set(stopwords.words("english"))
                # porter_stemmer = PorterStemmer()
                # lemmatizer = WordNetLemmatizer()
                # data = data_pre_processing_extended(stop_words, porter_stemmer, lemmatizer, raw_text)
                # user_name = data_pre_processing(stop_words, user_name)
                # user_name_clean = ""
                # for it in user_name:
                #     user_name_clean = user_name_clean + it
                # user_location = data_pre_processing(stop_words, user_location)
                # user_description = data_pre_processing(stop_words, user_description)

                result = {"raw_text": raw_text, "text": raw_text, "user_name": user_name,
                          "user_location": user_location, "user_description": user_description,
                          "timing": int(round(time.time() * 1000))}

                # https://stackoverflow.com/questions/53326879/twitter-streaming-api-urllib3-exceptions-protocolerror-connection-broken-i
                self.databaseHandler.insert_data("tweets", result)

                # print(result)

            return True
        except BaseException as error:
            print("===================================================================================================")
            print("Error on_data: %s" % str(error))
            print("===================================================================================================")
        return True

    def on_error(self, status):
        if status == 420:
            return False
        print("___________________________________________________________________________________________________")
        print(status)
        print("___________________________________________________________________________________________________")