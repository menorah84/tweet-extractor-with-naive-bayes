"""
Author : menorah84
Created: 2016-19-02
Description: Uses Python's NLTK and Naive Bayes Algorithm
             to compute for sentiment of a short text, i.e., a tweet   
"""

import nltk
import pickle
import random
import re
import string

from nltk.classify import ClassifierI
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
# from pkg_resources import ResourceManager
from statistics import mode
from utils import Util

class Analyzer(object):

    def __init__(self):
        self.regex = re.compile('[%s]' % re.escape(string.punctuation))
        self.classifier, self.word_features = Analyzer.__load_pickle()

    @staticmethod
    def __load_pickle():
        
        '''
        ## Using pkg_resource.ResourceManager
        ## works on Eclipse, but not in command line
        rm = ResourceManager()
        
        short_pos = rm.resource_string('resources', 'positive_words.txt')
        short_neg = rm.resource_string('resources', 'negative_words.txt')
        '''
        short_pos = open(Util.MODEL_DICT_DIR + "/positive_words.txt","r").read()
        short_neg = open(Util.MODEL_DICT_DIR + "/negative_words.txt","r").read()
        documents = []

        print "Appending dictionary of positive words"    
        for r in short_pos.split('\n'):
            documents.append( (r, "Positive") )

        print "Appending dictionary of negative words"
        for r in short_neg.split('\n'):
            documents.append( (r, "Negative") )

        all_words = []
        short_pos_words = short_pos.split('\n')
        short_neg_words = short_neg.split('\n')

        for w in short_pos_words:
            all_words.append(w.lower())

        for w in short_neg_words:
            all_words.append(w.lower())

        all_words = nltk.FreqDist(all_words)
        word_features = list(all_words.keys())[:5500]

        print("All words from dictionaries appended")

        featuresets = [(Analyzer.__find_features(rev, word_features), category) for (rev, category) in documents]
        random.shuffle(featuresets)
        size = int(len(featuresets) * 0.1)
        training_set = featuresets[:10500]
        testing_set =  featuresets[10500:]

        print("Loading our machine learning data model (pickle file) for sentiment analysis")
        
        # classifier_f = rm.resource_stream('resources', 'sentiment_model.pickle')
        classifier_f = open(Util.MODEL_DICT_DIR + "/sentiment_model.pickle","rb")
        classifier = pickle.load(classifier_f)
        classifier_f.close()

        print("Using original Naive Bayes algorithm")

        return classifier, word_features

    @staticmethod
    def __find_features(document, word_features):
        wordsvector = []

        words = word_tokenize(document)

        for index, r in enumerate(words):
            if r == "not" and index < (len(words) - 1):
                words[index + 1] = "z" + words[index + 1]

        for item in nltk.bigrams(document.split()):
            wordsvector.append(' '.join(item))

        features = {}

        for w in word_features:
            features[w] = (w.lower() in words or w.lower() in wordsvector)

        return features

    def sentiment(self, raw_text):

        text = self.regex.sub('', raw_text)

        bigramvector = []

        tk = word_tokenize(text.lower())

        for index, r in enumerate(tk):
            if r == "not" and index < (len(tk) - 1):
                tk[index + 1] = "z" + tk[index + 1]

        for item in nltk.bigrams(text.split()):
            bigramvector.append(' '.join(item))

        a = (set(tk) & set(self.word_features))
        b = (set(bigramvector) & set(self.word_features))

        if b:
            wordMatch = list(b)
        else:
            wordMatch = list(a)

        feats = Analyzer.__find_features(text.lower(), self.word_features)

        # print text

        if a or b:
            return (self.classifier.classify(feats), wordMatch)
        else:
            filtered = Analyzer.__clean_neutral_tweet(raw_text)
            return ("Neutral", filtered)

    @staticmethod
    def __clean_neutral_tweet(text):

        wordarray = text.split()

        regex = re.compile('[%s]' % re.escape(string.punctuation))
        stop_words = set(stopwords.words("english"))        

        filtered = []
    
        for word in wordarray:
            if word != 'RT' and not word.startswith('http') and not word.startswith('@') and not word.startswith('#') and word.lower() not in stop_words and not word.isdigit():
                
                word = regex.sub('', word)

                if len(word.strip()) > 2:
                    filtered.append(word)

        return filtered
