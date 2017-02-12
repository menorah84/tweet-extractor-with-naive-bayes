"""
Author : menorah84
Created: 2016-19-02
Description: Utilizes the Twitter Search API to search for tweets and writes to file if specified in options.
"""

from datetime import datetime
from models import Tweet, Wordcloud
from options import Option
from analyzer import Analyzer
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from utils import Util

import hadoopy
import json
import os
import sys
import shutil
import time
import tweepy

class Listener(StreamListener):

    def __init__(self, analyzer, hdfs_path, local_path, roll_size):
        self.analyzer = analyzer
        self.bytes_written = 0    
        self.file_closed = True        
        self.fp_tweets = None
        self.fp_wordcloud = None
        self.hdfs_path = hdfs_path
        self.local_path = local_path
        self.roll_size = roll_size
        self.write_to_file = True if hdfs_path or local_path else False

    def on_data(self, data):

        tweet_obj, wordarray = Tweet.tweet_wordcloud_from_json(json.loads(data), self.analyzer)

        wordcloud_list = Wordcloud.list_from_array(tweet_obj.tweet_id, wordarray)

        print tweet_obj.to_tsv_str()

        if self.write_to_file:
            print "Writing to file"
            
            if self.file_closed:
                print "Open new temporary files to write to"
                now = datetime.utcnow()
                self.fp_tweets = open(Util.TMP_DIR + '/' + Util.TWEETS + '/' + now.strftime('%Y-%m-%dT%H.%M.%SZ') + '.tmp', 'a')
                self.fp_wordcloud = open(Util.TMP_DIR + '/' + Util.WORDCLOUD + '/' + now.strftime('%Y-%m-%dT%H.%M.%SZ') + '.tmp', 'a')
                self.bytes_written = 0

            self.bytes_written += Tweet.write_to_file(tweet_obj, self.fp_tweets)
            Wordcloud.write_to_file(wordcloud_list, self.fp_wordcloud)
            self.file_closed = False

            print "bytes_written: " + str(self.bytes_written)
            print "roll_size: " + str(self.roll_size)

            if (self.bytes_written >= self.roll_size):
                self.fp_tweets.close()
                self.fp_wordcloud.close()

                filename_tweets = self.fp_tweets.name
                filename_wordcloud = self.fp_wordcloud.name
                
                print "Moving temporary files " + filename_tweets[-24:-4] + ".csv"
                print "filename_tweets: " + filename_tweets
                print "filename_wordcloud: " + filename_wordcloud

                if self.hdfs_path:
                    hadoopy.put(filename_tweets, self.hdfs_path + Util.TWEETS + '/' + filename_tweets[-24:-4] + '.csv')
                    hadoopy.put(filename_wordcloud, self.hdfs_path + Util.WORDCLOUD + '/' + filename_wordcloud[-24:-4] + '.csv')

                if self.local_path:
                    shutil.copy(filename_tweets, self.local_path + '/' + Util.TWEETS + '/' + filename_tweets[-24:-4] + '.csv')
                    shutil.copy(filename_wordcloud, self.local_path + '/' + Util.WORDCLOUD + '/' + filename_wordcloud[-24:-4] + '.csv')

                os.remove(filename_tweets)
                os.remove(filename_wordcloud)
                self.file_closed = True

        return True

    def on_error(self, status):
        print status

def main(argv):

    opt = Option.validate(argv)
        
    analyzer = Analyzer()

    listener = Listener(analyzer, opt.hdfs_path, opt.local_path, opt.roll_size)
    auth = OAuthHandler(opt.consumer_key, opt.consumer_secret)
    auth.set_access_token(opt.access_token_key, opt.access_token_secret)

    if not os.path.exists(Util.TMP_DIR + '/' + Util.TWEETS):
        os.makedirs(Util.TMP_DIR + '/' + Util.TWEETS)

    if not os.path.exists(Util.TMP_DIR + '/' + Util.WORDCLOUD):
        os.makedirs(Util.TMP_DIR + '/' + Util.WORDCLOUD)

    # create new local paths
    if opt.local_path:
        if not os.path.exists(opt.local_path + '/' + Util.TWEETS):
            os.makedirs(opt.local_path + '/' + Util.TWEETS)
        if not os.path.exists(opt.local_path + '/' + Util.WORDCLOUD):
            os.makedirs(opt.local_path + '/' + Util.WORDCLOUD)

    stream = Stream(auth, listener)
    stream.filter(track=opt.keywords)
        
if __name__ == '__main__':
    main(sys.argv[1:])