from functools import partial
from scipy.sparse import coo_matrix
import gzip
import logging
import numpy
import os
import pickle


class MyGeoHandler:

    def __init__(self):
        """ Given a directory, loads the saved (pickled and gzipped) geolocation model into memory."""
        model_dir = "./models/lrworld"
        pickle.load = partial(pickle.load, encoding="latin1")
        pickle.Unpickler = partial(pickle.Unpickler, encoding="latin1")
        self.coordinate_address = pickle.load(gzip.open(os.path.join(model_dir, "coordinate_address.pkl.gz"), "rb"))
        self.label_coordinate = pickle.load(gzip.open(os.path.join(model_dir, "label_coordinate.pkl.gz"), "rb"))
        self.vectorizer = pickle.load(gzip.open(os.path.join(model_dir, "vectorizer.pkl.gz"), "rb"))
        self.vectorizer.features = self.vectorizer.get_feature_names()
        self.clf = pickle.load(gzip.open(os.path.join(model_dir, "clf.pkl.gz"), "rb"))
        self.model_loaded = True

    def get_top_k_features(self, label):
        """
        given a label (str) return the top k important features as a list
        """
        topk_feature_indices = numpy.argsort(self.clf.coef_[label].toarray())[0, -50:].tolist()[::-1]
        topk_features = [self.vectorizer.features[i] for i in topk_feature_indices]
        topk_features = [f for f in topk_features if "user_" not in f]
        return topk_features

    def get_location_info(self, label):
        """
        given a label (str) returns a dictionary containing information about the corresponding location.
        """
        lat, lon = self.label_coordinate[label]
        location = self.coordinate_address[(lat, lon)]
        country = location["address"].get("country", "")
        state = location["address"].get("state", "")
        city = location["address"].get("city", "")
        return {"lat": lat, "lon": lon, "country": country, "state": state, "city": city}

    def geo_iterable(self, texts, return_lbl_dist):
        """
        given an iterable (e.g. a list) of texts (str/unicode), vectorize them, classify them into one of the regions using clf
        (a pre-trained classifier) and returns results a list of dictionaries with the same order as texts corresponding
        to each text item with info about the predicted location(s).

        Args:
            texts (list/tuple): a list of strings/unicodes which should be geolocated.
            return_lbl_dist: if True returns the probability distribution over all the classes.
        Returns:
            a dictionary containing the predicted geolocation information about text.
        """
        label_distribution_dict = None
        results = []
        test_samples = texts
        num_samples = len(test_samples)
        X_test = self.vectorizer.transform(test_samples)
        label_distributions = self.clf.predict_proba(X_test)
        for i in range(num_samples):
            label_distribution = label_distributions[i]
            if return_lbl_dist:
                label_distribution = coo_matrix(label_distribution)
                label_distribution_dict = {}
                for j, lbl, prob in zip(label_distribution.row, label_distribution.col, label_distribution.data):
                    label_distribution_dict[lbl] = prob
                label_distribution = label_distribution.toarray()

            prediction = numpy.argmax(label_distribution)
            confidence = label_distribution[prediction]
            top50_features = ", ".join(self.get_top_k_features(prediction))
            location_info = self.get_location_info(prediction)
            if return_lbl_dist:
                result = {"top50": top50_features, "label_distribution": label_distribution_dict}
            else:
                result = {"top50": top50_features, "label_distribution": {prediction: confidence}}
            result.update(location_info)
            results.append(result)
        return results

    def geo(self, text):
        """
        given a piece of text (str/unicode), vectorize it, classify it into one of the regions using
        clf (a pre-trained classifier) and return a json which has info about the predicted location(s).
        If the input is a list of texts it calls geo_iterable.

        Efficiency Note: It is not efficient to call geo(text) several times. The best is to call it with a list of texts
        as input.

        Args:
            text (str/unicode): a string which should be geolocated. It can be a piece of text or one single Twitter screen name e.g. @user.
            return_lbl_dist: if True returns the probability distribution over all the classes.
            topk (int): default(1), if higher than 1, return the top K locations ordered by classifier"s confidence.
        Returns:
            a dictionary containing the predicted geolocation information about text.
        """
        label_distribution_dict = None
        topK_label_dist = None
        topK_location_info = {}
        return_lbl_dist = False
        topK = 1
        if not text:
            return
        if isinstance(text, list) or isinstance(text, tuple):
            return self.geo_iterable(text, return_lbl_dist)
        test_samples = [text]
        X_test = self.vectorizer.transform(test_samples)
        label_distribution = self.clf.predict_proba(X_test)

        if return_lbl_dist:
            label_distribution = coo_matrix(label_distribution)
            label_distribution_dict = {}
            for lbl in range(0, label_distribution.shape[1]):
                label_distribution_dict[lbl] = label_distribution[0, lbl]
        elif 1 < topK <= label_distribution.shape[1]:
            topK_labels = numpy.argsort(label_distribution)[0][::-1][:topK].tolist()
            topK_probabilities = [label_distribution[0, i] for i in topK_labels]
            topK_label_dist = dict(zip(topK_labels, topK_probabilities))
            for i, lbl in enumerate(topK_labels):
                location_info = self.get_location_info(lbl)
                topK_location_info["lat" + str(i)] = location_info["lat"]
                topK_location_info["lon" + str(i)] = location_info["lon"]
                topK_location_info["city" + str(i)] = location_info["city"]
                topK_location_info["state" + str(i)] = location_info["state"]
                topK_location_info["country" + str(i)] = location_info["country"]

        prediction = numpy.argmax(label_distribution)
        confidence = label_distribution[0, prediction]

        top50_features = ", ".join(self.get_top_k_features(prediction))
        location_info = self.get_location_info(prediction)
        if return_lbl_dist:
            result = {"top50": top50_features, "label_distribution": label_distribution_dict}
        elif 1 < topK <= label_distribution.shape[1]:
            result = {"top50": top50_features, "label_distribution": topK_label_dist}
            result.update(topK_location_info)
        else:
            result = {"top50": top50_features, "label_distribution": {prediction: confidence}}
        result.update(location_info)
        logging.debug(result)
        return result
