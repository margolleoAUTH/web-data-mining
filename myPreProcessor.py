from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
from nltk.stem import WordNetLemmatizer
import preprocessor as p

# Handles the data pre-processing before the analysis
# Even if this class has generic functions that can be used generally, this class is applied in only in topic analysis


class MyPreProcessor:

    # Constructor - Initiates the the appropriate instances regarding the stop-words, stemming and lemmatization
    def __init__(self):
        self.stop_words = set(stopwords.words("english"))
        # self.stop_words.extend(
        #     ["from", "subject", "re", "edu", "use", "not", "would", "say", "could", "_", "be", "know", "good", "go",
        #      "get", "do", "done", "try", "many", "some", "nice", "thank", "think", "see", "rather", "easy", "easily",
        #      "lot", "lack", "make", "want", "seem", "run", "need", "even", "right", "line", "even", "also", "may",
        #      "take", "come"])
        self.porter_stemmer = PorterStemmer()
        self.lemmatizer = WordNetLemmatizer()

    # Performs tokenization and stop-word cleaning - Private function
    def data_pre_processing_nltk(self, text):
        word_tokens = word_tokenize(text)
        word_tokens = [w.lower() for w in word_tokens if w.isalpha()]
        text = [w for w in word_tokens if not w in self.stop_words]
        return text

    # Performs tokenization and stop-word cleaning, stemming and lemmatization - Private function
    def data_pre_processing_nltk_extended(self, text):
        word_tokens = word_tokenize(text)
        word_tokens = [w.lower() for w in word_tokens if w.isalpha()]
        word_tokens = [w for w in word_tokens if not w in self.stop_words]
        word_tokens = [self.porter_stemmer.stem(w) for w in word_tokens]
        # text = [self.lemmatizer.lemmatize(w) for w in word_tokens]
        return word_tokens

    # Performs cleaning of URLs/Mentions/Emoji/Smileys before tokenization and stop-word cleaning
    def data_pre_processing(self, text):
        p.set_options(p.OPT.URL, p.OPT.MENTION, p.OPT.EMOJI, p.OPT.SMILEY)
        text = p.clean(text)
        text = self.data_pre_processing_nltk(text)
        pre_processing_result = text
        return pre_processing_result

    # Performs manipulation and separation of URLs/Mentions/Emoji/Smileys before tokenization and stop-word cleaning
    def data_pre_processing_extended(self, text):
        parsed = p.parse(text)
        urls = [x.match for x in parsed.urls] if not parsed.urls is None else []
        hashtags = [x.match for x in parsed.hashtags] if not parsed.hashtags is None else []
        mentions = [x.match for x in parsed.mentions] if not parsed.mentions is None else []
        emojis = [x.match for x in parsed.emojis] if not parsed.emojis is None else []
        smileys = [x.match for x in parsed.smileys] if not parsed.smileys is None else []
        p.set_options(p.OPT.URL, p.OPT.MENTION, p.OPT.EMOJI, p.OPT.SMILEY)
        text = p.clean(text)
        text = self.data_pre_processing_nltk_extended(text)
        pre_processing_result = [text, urls, hashtags, mentions, emojis, smileys]
        return pre_processing_result
