from typing import Optional, Awaitable, Union
from tweepy import OAuthHandler
from tweepy import Stream
from myDBHandler import MyDBHandler
from myGeoHandler import MyGeoHandler
from myPreProcessor import MyPreProcessor
from myStreamHandler import MyStreamHandler
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from gensim import corpora
from tornado import websocket
import credentials
import gensim
import tornado.web
import tornado.ioloop
import threading
import time
import asyncio
import json

subscribers = set()
_self = {}


def push_client_info():
    while True:
        try:
            # Web Socket Thread push back the result data to every subscriber
            # Release Note: There is bug in the server application - ONLY ONE connection can be established with the
            #  current software to the server socket session
            for subscriber in subscribers:
                response = {"geo_trends": _self["databaseHandler"].get_trends_data("geo_trends"),
                            "sentiment_trends": _self["databaseHandler"].get_trends_data("sentiment_trends"),
                            "topic_trends": _self["databaseHandler"].get_trends_data("topic_trends")}
                subscriber.write_message(json.dumps(response))
        except BaseException as error:
            print("===================================================================================================")
            print("Error MyWSThread_push_client_info: %s" % str(error))
            print("===================================================================================================")
        # Wait!
        time.sleep(_self["analysisTimeFrame"])


class MyWSHandler(tornado.websocket.WebSocketHandler):

    def on_message(self, message: Union[str, bytes]) -> Optional[Awaitable[None]]:
        pass

    def data_received(self, chunk: bytes) -> Optional[Awaitable[None]]:
        pass

    def check_origin(self, origin):
        return True

    def open(self):
        subscribers.add(self)
        push_client_info()

    def on_close(self):
        subscribers.clear()


class MyWSThread(threading.Thread):

    # Socket sub-server starts
    def run(self):
        asyncio.set_event_loop(asyncio.new_event_loop())
        app = tornado.web.Application(handlers=[("/", MyWSHandler)])
        app.listen(9000)
        print("Web Socket API Server PDM is serving at port")
        tornado.ioloop.IOLoop.instance().start()


class MyTrendsThread(threading.Thread):

    def run(self):
        while True:
            try:
                # Wait!
                time.sleep(_self["analysisTimeFrame"])
                collection = _self["databaseHandler"].get_data("tweets")
                tweets_analysis_no = len(collection) - 1
                topic_results = []
                for it in collection:
                    raw_text = it["raw_text"]

                    # Geolocation Analysis =============================================================================
                    geo_result = _self["geoHandler"].geo(raw_text)
                    # ==================================================================================================

                    # Sentiment Analysis ===============================================================================
                    scores = _self["sentimentIntensityAnalyzer"].polarity_scores(raw_text)
                    compound_score = scores["compound"]
                    sentiment_result = 0
                    sentiment_result_description = "neutral"
                    if compound_score > 0.05:
                        sentiment_result = 1
                        sentiment_result_description = "positive"
                    elif compound_score < -0.05:
                        sentiment_result = -1
                        sentiment_result_description = "negative"
                    # ==================================================================================================

                    # Add results in database
                    geo_sentiment_results = {"lat": geo_result["lat"], "lon": geo_result["lon"], "country": geo_result["country"],
                                             "sentiment_result": sentiment_result, "sentiment_result_description": sentiment_result_description,
                                             "timing": int(round(time.time() * 1000)),
                                             "tweets_analysis_no": tweets_analysis_no}
                    _self["databaseHandler"].insert_data("geo_sentiment_results", geo_sentiment_results)

                    # Topic pre-processing =============================================================================
                    # Topic data not need it to be stored in the database. It is more efficient to be handled by memory
                    topic_results.append(_self["preProcessor"].data_pre_processing_extended(it["raw_text"])[0])
                    # ==================================================================================================

                # Code above take care the database limitation and based on the milliseconds/data attribute[timing]
                # clean the database from the data the have been part of the final results, as we introduce a Real Time
                #  Application

                _self["databaseHandler"].delete_data("tweets", collection[tweets_analysis_no]["timing"])

                votes = 0
                us = 0
                ous = 0
                # Geo/Sentiment Results ================================================================================
                collection = _self["databaseHandler"].get_data("geo_sentiment_results")
                geo_sentiment_results = _self["databaseHandler"].get_vote_result_data("geo_sentiment_results")
                geo_sentiment_results_analysis_no = len(geo_sentiment_results) - 1
                for it in geo_sentiment_results:
                    votes = votes + it["user_vote"]

                for it in collection:
                    if "United States of America" in it["country"]:
                        us = us + 1
                    else:
                        ous = ous + 1

                geo_trends = {"us": us, "ous": ous, "timing": int(round(time.time() * 1000))}
                sentiment_trends = {"votes": votes, "timing": int(round(time.time() * 1000))}

                _self["databaseHandler"].insert_data("geo_trends", geo_trends)
                _self["databaseHandler"].insert_data("sentiment_trends", sentiment_trends)
                _self["databaseHandler"].delete_data("geo_sentiment_results", collection[geo_sentiment_results_analysis_no]["timing"])
                # ======================================================================================================

                # Topic Analysis n Results =============================================================================
                dictionary = corpora.Dictionary(topic_results)
                corpus = [dictionary.doc2bow(it) for it in topic_results]
                ldaModel = gensim.models.ldamodel.LdaModel(corpus, num_topics=3, id2word=dictionary, passes=15)
                topic_trends = {}
                for i, row_list in enumerate(ldaModel[corpus]):
                    row = row_list[0] if ldaModel.per_word_topics else row_list
                    row = sorted(row, key=lambda x: (x[1]), reverse=True)
                    for j, (topic_num, prop_topic) in enumerate(row):
                        if j == 0:
                            topic_details = ldaModel.show_topic(topic_num)
                            topic_keywords = ", ".join([word for word, prop in topic_details])
                            index = str(topic_num)
                            if index in topic_trends:
                                topic_trends[index]["numberOfTweets"] = topic_trends[index]["numberOfTweets"] + 1
                            else:
                                topic_trends[index] = {"numberOfTweets": 1, "topic": topic_keywords}
                        else:
                            break

                topic_trends["timing"] = int(round(time.time() * 1000))
                _self["databaseHandler"].insert_data("topic_trends", topic_trends)
                # ======================================================================================================

            except BaseException as error:
                print("===================================================================================================")
                print("Error MyTrendsThread_run: %s" % str(error))
                print("===================================================================================================")


if __name__ == "__main__":

    # Application init [Credentials and Tweeter Handler for Listen Streaming and Manipulate data]
    auth = OAuthHandler(credentials.CONSUMER_KEY, credentials.CONSUMER_SECRET_KEY)
    auth.set_access_token(credentials.ACCESS_TOKEN, credentials.ACCESS_SECRET_TOKEN)

    # Initialization of Controller's Instances
    # database Handler
    # Geolocation Handler
    # Sentiment Analyzer
    # Pre-Processor
    connection_string = "mongodb+srv://marman:5marman@cluster0-cin8u.mongodb.net/twitter_data?retryWrites=true&w=majority"
    _self["databaseHandler"] = MyDBHandler(connection_string)
    _self["geoHandler"] = MyGeoHandler()
    _self["sentimentIntensityAnalyzer"] = SentimentIntensityAnalyzer()
    _self["preProcessor"] = MyPreProcessor()
    _self["analysisTimeFrame"] = 30

    # Database clean from previous run. This step is going to be skipped in release stage
    _self["databaseHandler"].delete_all_data("tweets")
    _self["databaseHandler"].delete_all_data("geo_sentiment_results")
    _self["databaseHandler"].delete_all_data("geo_trends")
    _self["databaseHandler"].delete_all_data("sentiment_trends")
    _self["databaseHandler"].delete_all_data("topic_trends")

    # Controller Start 3 Basic - Pillar Threads
    # Web Socket Thread that handles the communication with the web client and push back the result data
    # The Trends/Results thread, which performs the data fetching from database and basic operations to
    #  define and store the results
    # The Tweeter's Streaming Thread
    thread = MyWSThread()
    thread.start()
    thread = MyTrendsThread()
    thread.start()

    try:
        streamListener = MyStreamHandler(_self["databaseHandler"])
        hashTagList = ["Donald Trump", "donald trump"]
        stream = Stream(auth, streamListener)
        stream.filter(track=hashTagList)
    except BaseException as error:
        print("===================================================================================================")
        print("Error on_main: %s" % str(error))
        print("===================================================================================================")
        streamListener = MyStreamHandler(_self["databaseHandler"])
        hashTagList = ["Donald Trump", "donald trump"]
        stream = Stream(auth, streamListener)
        stream.filter(track=hashTagList)

# Extra Comments
#
# git config --replace-all user.name "margolleoAUTH"
# git config --replace-all user.email "margolleo@csd.auth.com"
# git commit --author="margolleoAUTH <margolleo@csd.auth.com>"
#
# Database(Atlas):
# https://cloud.mongodb.com/user?n=%2Fv2%2F5ce5e9a1ff7a252fdb34b8b1&nextHash=%23clusters#/atlas/login
# margolleo@csd.auth.com
# 5Kleanthis@
#
# https://www.tutorialspoint.com/googlecharts/googlecharts_line_charts.htm
# https://impythonist.wordpress.com/2015/08/02/build-a-real-time-data-push-engine-using-python-and-rethinkdb/
# https://stackoverflow.com/questions/53326879/twitter-streaming-api-urllib3-exceptions-protocolerror-connection-broken-i
