from tweepy import OAuthHandler
from tweepy import Stream
from myDBHandler import MyDBHandler
from myStreamHandler import MyStreamHandler
import credentials

# Commnets for SK: 1/2 of June 2019
#
# git commit --author="margolleoAUTH <margolleo@csd.auth.com>"
# git config --replace-all user.name "margolleoAUTH"
# git config --replace-all user.email "margolleo@csd.auth.com"
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

if __name__ == "__main__":

    # Application init [Credentials and Tweeter Handler for Listen Streaming and Manipulate data]
    auth = OAuthHandler(credentials.CONSUMER_KEY, credentials.CONSUMER_SECRET_KEY)
    auth.set_access_token(credentials.ACCESS_TOKEN, credentials.ACCESS_SECRET_TOKEN)

    # classifier

    connection_string = "mongodb://marman:5marman@cluster0-shard-00-00-cin8u.mongodb.net:27017/twitter_data?ssl=true" \
                        "&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true&w=majority"
    connection_string_replica = "mongodb://marman:5marman@cluster0-shard-00-01-cin8u.mongodb.net:27017/twitter_data" \
                                "?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true&w=majority"

    databaseHandler = MyDBHandler(connection_string, connection_string_replica)

    streamListener = MyStreamHandler(databaseHandler)

    # Stream tweets and filter
    hashTagList = ["Trump", "trump"]
    stream = Stream(auth, streamListener)
    stream.filter(track=hashTagList)
