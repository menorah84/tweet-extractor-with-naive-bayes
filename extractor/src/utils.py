"""
Author : menorah84
Created: 2016-15-02
Description: Utilizes the Twitter Search API to search for tweets and writes to file if specified in options.
"""

from datetime import datetime
import csv
import os
import sys

class Util:

    chars_to_rm = ['\t', '\n', '\r']
    TMP_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../tmp"))
    RESOURCES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../resources"))
    MODEL_DICT_DIR = RESOURCES_DIR
    # TMP_DIR = os.environ.get('TMP_PATH')
    # MODEL_DICT_PATH = os.environ.get('MODEL_DICT_PATH')
    TWEETS = 'tweets'
    WORDCLOUD = 'wordcloud'
    MIN_ROLL_SIZE = 102400          # 100 KB
    MAX_ROLL_SIZE = 104857600       # 100 MB
    DEFAULT_ROLL_SIZE = 1048576     # 1 MB

    # to get the approximate centerpoint, we get 
    # the mean of the optimums (lat & long separately) of the bounding box
    @staticmethod
    def get_alternative_coordinates(coordinates):

        for index, point in enumerate(coordinates):
            if index == 0:
                min_longitude = point[0]
                max_longitude = point[0]
                min_latitude = point[1]
                max_latitude = point[1]
            else:
                min_longitude = point[0] if point[0] < min_longitude else min_longitude
                max_longitude = point[0] if point[0] > max_longitude else max_longitude
                min_latitude = point[1] if point[1] < min_latitude else min_latitude
                max_latitude = point[1] if point[1] > max_latitude else max_latitude

        mean_longitude = (min_longitude + max_longitude) / 2.0
        mean_latitude = (min_latitude + max_latitude) / 2.0

        return mean_longitude, mean_latitude


    @staticmethod
    def clean_text(s):
        return s.translate(None, ''.join(Util.chars_to_rm))


    @staticmethod                              
    def empty_str_if_none(s):
        if s is None:
            return ''
        if isinstance(s, str):
            return s
        else:
            return str(s)

             
    @staticmethod
    def write_tweet(tweet, fp):
        
        tsv_str = tweet.to_tsv_str() + '\n'
    
        tsv_str_utf8 = tsv_str.encode('utf-8')

        bytes_written = len(tsv_str_utf8)

        fp.write(tsv_str_utf8)

        return bytes_written


    @staticmethod
    def write_wordcloud(wordcloud, fp):
        
        tsv_str = wordcloud.to_tsv_str() + '\n'

        tsv_str_utf8 = tsv_str.encode('utf-8')

        bytes_written = len(tsv_str_utf8)

        fp.write(tsv_str_utf8)

        return bytes_written

    @staticmethod
    def vprint(is_verbose, message):
        if is_verbose:
            print message
        pass


        
