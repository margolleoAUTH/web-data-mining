from _thread import start_new_thread
from typing import Optional, Awaitable, Union

from tornado.platform.asyncio import AnyThreadEventLoopPolicy
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
import socket
import sys

subscribers = set()
_self = {}


def push_client_info():
    while True:
        try:
            for subscriber in subscribers:
                response = {"geo_trends": _self["databaseHandler"].get_trends_data("geo_trends"),
                            "sentiment_trends": _self["databaseHandler"].get_trends_data("sentiment_trends"),
                            "topic_trends": _self["databaseHandler"].get_trends_data("topic_trends")}
                subscriber.write_message(json.dumps(response))
        except BaseException as error:
            print("===================================================================================================")
            print("Error MyWSThread_push_client_info: %s" % str(error))
            print("===================================================================================================")
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


# def client_thread(conn):
#     conn.send("Welcome to the server. Type something and hit enter".encode("utf-8"))
#     while True:
#         # data = conn.recv(1024)
#         reply = "OK..."
#         # if not data:
#         #     break
#         conn.sendall(reply.encode("utf-8"))
#     conn.close()


class MyWSThread(threading.Thread):

    def run(self):
        # try:
        #     # s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #     # s.bind(("", 9000))
        #     # s.listen(10)
        #
        #     s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #     s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        #     s.bind(("0.0.0.0", 9000))
        #     s.listen(10)
        #
        #     print("Web Socket API Server PDM is serving at port")
        #     while 1:
        #         conn, addr = s.accept()
        #         start_new_thread(client_thread, (conn,))
        # except socket.error as msg:
        #     print("===================================================================================================")
        #     print("WS failed. Error Code : " + str(msg))
        #     print("===================================================================================================")
        #     sys.exit()

        # asyncio.set_event_loop(asyncio.new_event_loop())
        asyncio.set_event_loop_policy(AnyThreadEventLoopPolicy())

        app = tornado.web.Application(handlers=[("/", MyWSHandler)])
        app.listen(9000)
        print("Web Socket API Server PDM is serving at port")
        tornado.ioloop.IOLoop.instance().start()


class MyTrendsThread(threading.Thread):

    def run(self):
        while True:
            try:
                time.sleep(_self["analysisTimeFrame"])
                collection = _self["databaseHandler"].get_data("tweets")
                tweets_analysis_no = len(collection) - 1
                topic_results = []
                for it in collection:
                    raw_text = it["raw_text"]
                    user_name = it["user_name"]

                    geo_result = _self["geoHandler"].geo(raw_text)

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

                    geo_sentiment_results = {"user_name": user_name,
                                             "lat": geo_result["lat"], "lon": geo_result["lon"], "country": geo_result["country"],
                                             "sentiment_result": sentiment_result, "sentiment_result_description": sentiment_result_description,
                                             "timing": int(round(time.time() * 1000)),
                                             "tweets_analysis_no": tweets_analysis_no}
                    _self["databaseHandler"].insert_data("geo_sentiment_results", geo_sentiment_results)
                    topic_results.append(_self["preProcessor"].data_pre_processing_extended(it["raw_text"])[0])

                _self["databaseHandler"].delete_data("tweets", collection[tweets_analysis_no]["timing"])

                votes = 0
                us = 0
                ous = 0
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

            except BaseException as error:
                print("===================================================================================================")
                print("Error MyTrendsThread_run: %s" % str(error))
                print("===================================================================================================")


if __name__ == "__main__":

    # Application init [Credentials and Tweeter Handler for Listen Streaming and Manipulate data]
    auth = OAuthHandler(credentials.CONSUMER_KEY, credentials.CONSUMER_SECRET_KEY)
    auth.set_access_token(credentials.ACCESS_TOKEN, credentials.ACCESS_SECRET_TOKEN)

    connection_string = "mongodb+srv://marman:5marman@cluster0-cin8u.mongodb.net/twitter_data?retryWrites=true&w=majority"
    databaseHandler = MyDBHandler(connection_string)

    # databaseHandler.delete_all_data("tweets")
    # databaseHandler.delete_all_data("geo_sentiment_results")
    # databaseHandler.delete_all_data("geo_trends")
    # databaseHandler.delete_all_data("sentiment_trends")
    # databaseHandler.delete_all_data("topic_trends")

    _self["analysisTimeFrame"] = 30
    _self["databaseHandler"] = databaseHandler
    # _self["geoHandler"] = MyGeoHandler()
    # _self["sentimentIntensityAnalyzer"] = SentimentIntensityAnalyzer()
    # _self["preProcessor"] = MyPreProcessor()

    thread = MyWSThread()
    thread.start()
    # thread = MyTrendsThread()
    # thread.start()
    #
    # try:
    #     streamListener = MyStreamHandler(databaseHandler)
    #     hashTagList = ["Donald Trump", "donald trump"]
    #     stream = Stream(auth, streamListener)
    #     stream.filter(track=hashTagList)
    # except BaseException as error:
    #     print("===================================================================================================")
    #     print("Error on_main: %s" % str(error))
    #     print("===================================================================================================")
    #     streamListener = MyStreamHandler(databaseHandler)
    #     hashTagList = ["Donald Trump", "donald trump"]
    #     stream = Stream(auth, streamListener)
    #     stream.filter(track=hashTagList)

# Commnets for SK: 1/2 of June 2019
#
# git config --replace-all user.name "margolleoAUTH"
# git config --replace-all user.email "margolleo@csd.auth.com"
# git commit --author="margolleoAUTH <margolleo@csd.auth.com>"
#
# Marios:
# Webosckets stin instostelida - We server
# Semtiment Analysis me GoogleCharts
# Provlima me listeners me result ktl
#
# Manos:
# Code for emerging topic analysis
# Geolocation
#
# Database:
# margolleo@csd.auth.com
# 5Kleanthis@
# Atlas
# https://cloud.mongodb.com/user?n=%2Fv2%2F5ce5e9a1ff7a252fdb34b8b1&nextHash=%23clusters#/atlas/login
# https://www.tutorialspoint.com/googlecharts/googlecharts_line_charts.htm
# https://impythonist.wordpress.com/2015/08/02/build-a-real-time-data-push-engine-using-python-and-rethinkdb/