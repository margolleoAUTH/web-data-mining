from gensim import corpora
from pymongo import monitoring
import time
import gensim


class MyMongoListener(monitoring.CommandListener):

    def __init__(self, databaseHandler, secondsFrame):
        super().__init__()
        self.databaseHandler = databaseHandler
        self.secondsFrame = secondsFrame
        self.limit = time.time() + self.secondsFrame

    def started(self, event):
        pass

    def succeeded(self, event):
        if time.time() > self.limit:

            # https://medium.com/analytics-vidhya/simplifying-social-media-sentiment-analysis-using-vader-in-python-f9e6ec6fc52f
            # score = self.analyser.polarity_scores(text)
            # print(score)

            print("===================================================================================================")
            collection = self.databaseHandler.get_data("tweets")
            textakia = [it["text"][0] for it in collection]

            dictionary = corpora.Dictionary(textakia)
            corpus = [dictionary.doc2bow(it) for it in textakia]
            # pickle.dump(corpus, open("corpus.pkl", "wb"))
            # dictionary.save("dictionary.gensim")

            NUM_TOPICS = 5
            ldamodel = gensim.models.ldamodel.LdaModel(corpus, num_topics=NUM_TOPICS, id2word=dictionary, passes=15)
            # ldamodel.save("model5.gensim")
            topics = ldamodel.print_topics(num_words=4)
            for topic in topics:
                print(topic)

            print("===================================================================================================")

            self.limit = time.time() + self.secondsFrame

    def failed(self, event):
        pass