from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
from nltk.stem import WordNetLemmatizer
from tweepy.streaming import StreamListener
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import preprocessor as p
import json
import time


def data_pre_processing_nltk(stop_words, text):
    word_tokens = word_tokenize(text)
    word_tokens = [w.lower() for w in word_tokens if w.isalpha()]
    text = [w for w in word_tokens if not w in stop_words]
    return text


def data_pre_processing_nltk_extended(stop_words, porter_stemmer, lemmatizer, text):
    word_tokens = word_tokenize(text)
    word_tokens = [w.lower() for w in word_tokens if w.isalpha()]
    word_tokens = [w for w in word_tokens if not w in stop_words]
    word_tokens = [porter_stemmer.stem(w) for w in word_tokens]
    text = [lemmatizer.lemmatize(w) for w in word_tokens]
    return text


def data_pre_processing(stop_words, text):
    p.set_options(p.OPT.URL, p.OPT.MENTION, p.OPT.EMOJI, p.OPT.SMILEY)
    text = p.clean(text)
    text = data_pre_processing_nltk(stop_words, text)
    pre_processing_result = text
    return pre_processing_result


def data_pre_processing_extended(stop_words, porter_stemmer, lemmatizer, text):
    parsed = p.parse(text)
    urls = [x.match for x in parsed.urls] if not parsed.urls is None else []
    hashtags = [x.match for x in parsed.hashtags] if not parsed.hashtags is None else []
    mentions = [x.match for x in parsed.mentions] if not parsed.mentions is None else []
    emojis = [x.match for x in parsed.emojis] if not parsed.emojis is None else []
    smileys = [x.match for x in parsed.smileys] if not parsed.smileys is None else []
    p.set_options(p.OPT.URL, p.OPT.MENTION, p.OPT.EMOJI, p.OPT.SMILEY)
    text = p.clean(text)
    text = data_pre_processing_nltk_extended(stop_words, porter_stemmer, lemmatizer, text)
    pre_processing_result = [text, urls, hashtags, mentions, emojis, smileys]
    return pre_processing_result


class MyStreamHandler(StreamListener):

    def __init__(self, databaseHandler):
        super().__init__()
        self.databaseHandler = databaseHandler
        self.analyser = SentimentIntensityAnalyzer()

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

                # Web Solution for full text of the tweet ==============================================================
                # If data have retweeted_status => inside retweeted_status may exist the full_text
                if "retweeted_status" in data:
                    data = data["retweeted_status"]

                # In data we may have the pure data or as data we have the retweeted_status
                # Firstly as tweet we establish the simple text.
                # If there is extended_tweet in data, there is the full_text
                text = data["text"]
                if "extended_tweet" in data:
                    text = data["extended_tweet"]["full_text"]
                # ======================================================================================================

                # ACTION ITEM: STOP-WORDS with extended list to support the removal of articles, pronouns, etc
                stop_words = set(stopwords.words("english"))
                porter_stemmer = PorterStemmer()
                lemmatizer = WordNetLemmatizer()
                text = data_pre_processing_extended(stop_words, porter_stemmer, lemmatizer, text)
                user_name = data_pre_processing(stop_words, user_name)
                user_location = data_pre_processing(stop_words, user_location)
                user_description = data_pre_processing(stop_words, user_description)

                result = {"text": text, "user_name": user_name,
                          "user_location": user_location, "user_description": user_description}

                self.databaseHandler.insert_data("tweets", result)

                print(result)

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


# boston = load_boston()
# X, y = boston.data, boston.target
# forest = MondrianForestRegressor()
# forest.fit(X, y)
# y_mean, y_std = forest.predict(X, return_std=True)
# print(y_mean)
# print("===============================================================================================")
# print(y_std)
# print("")

# iris = load_iris()
# data = pd.DataFrame({
#     "sepal length": iris.data[:, 0],
#     "sepal width": iris.data[:, 1],
#     "petal length": iris.data[:, 2],
#     "petal width": iris.data[:, 3],
#     "species": iris.target
# })
# data.head()
# X = data[["sepal length", "sepal width", "petal length", "petal width"]]  # Features
# y = data["species"]  # Labels
# X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)
# clf = RandomForestClassifier(n_estimators=100)
# clf.fit(X_train,y_train)
# y_pred=clf.predict(X_test)
# print("Accuracy:", metrics.accuracy_score(y_test, y_pred))
# print("")

#  RandomForestClassifier(n_estimators=100, warm_start=True)
# global_train_data = dict()
# for i in customRange:
#     get_data()
#     global_train_data.append(new_train_data)  # Appending new train data
#     clf.fit(global_train_data)  # Fitting on global train data
#     clf.predict(new_test_data)
