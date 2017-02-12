"""
Author : menorah84
Created: 2016-15-02
Description: Utilizes the Twitter Search API to search for tweets and writes to file if specified in options.
"""

from models import Tweet, Wordcloud
from options import Option
from utils import Util

from datetime import datetime
from models import Tweet
from utils import Util

import analyzer as an
import errno
import hadoopy
import os
import shutil
import sys
import tweepy

def main(opt):

    # set twitter auth credentials
    auth = tweepy.OAuthHandler(opt.consumer_key, opt.consumer_secret)
    auth.set_access_token(opt.access_token_key, opt.access_token_secret)

    # get api instance
    if opt.https_proxy:
        api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, proxy=opt.https_proxy)
    else:
        api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    # write_to_file, roll_size or roll_count
    write_to_file = True if opt.hdfs_path or opt.local_path else False

    # instantiate the analyzer
    analyzer = an.Analyzer()
    
    # override opt.roll_size for testing
    # opt.roll_size = 20480

    if write_to_file:
        tmp_tweet_dir = Util.TMP_DIR + '/' + Util.TWEETS
        tmp_wordcloud_dir = Util.TMP_DIR + '/' + Util.WORDCLOUD

        if not os.path.exists(tmp_tweet_dir):
            os.makedirs(tmp_tweet_dir)

        if not os.path.exists(tmp_wordcloud_dir):
            os.makedirs(tmp_wordcloud_dir)

        # create new hdfs paths
        if opt.hdfs_path:
            hadoopy.put(tmp_tweet_dir, opt.hdfs_path)
            hadoopy.put(tmp_wordcloud_dir, opt.hdfs_path)
    
        # create new local paths
        if opt.local_path:
            try:
                os.makedirs(opt.local_path + Util.TWEETS)
                os.makedirs(opt.local_path + Util.WORDCLOUD)
            except OSError as e:
                if e.errno != errno.EEXIST :
                    raise e
                pass
            
    # join our keywords as a single query
    # query = ' OR '.join(opt.keywords)
    queries = [' OR '.join(opt.keywords[i : i + 10]) for i in xrange(0, len(opt.keywords), 10)]

    for query in queries:

        file_closed = True

        # Cursor params
        # since_id=tweet_id, max_id=tweet_id, lang="en"
        # include_entities=True, rpp=100, count=1000
        if opt.since_tweet_id:
            cursor = tweepy.Cursor(api.search, q=query, result_type="recent", since_id=opt.since_tweet_id, rpp=100)
        else:
            cursor = tweepy.Cursor(api.search, q=query, result_type="recent", rpp=100)

        try:

            for tweet in cursor.items():
                
                tweet_obj, wordarray = Tweet.tweet_wordcloud_from_json(tweet._json, analyzer)

                wordcloud_list = Wordcloud.list_from_array(tweet_obj.tweet_id, wordarray)

                Util.vprint(opt.verbose, "Tweet_id: " + str(tweet_obj.tweet_id))
                # print "Tweet_id: " + str(tweet_obj.tweet_id)
                        
                # determine if we are flagged to write to file
                if write_to_file:

                    # start of loop
                    # fp will either return an existing .tmp file or open a new one
                    # bytes_written will be automatically set to zero
                    # if new file created
                    if file_closed:
                        now = datetime.utcnow()
                        fp_tweets = open(tmp_tweet_dir + '/' + now.strftime('%Y-%m-%dT%H.%M.%SZ') + '.tmp', 'a')
                        fp_wordcloud = open(tmp_wordcloud_dir + '/' + now.strftime('%Y-%m-%dT%H.%M.%SZ') + '.tmp', 'a')
                        bytes_written = 0
                        print "Create new temporary file to write to: " + fp_tweets.name[-24:]

                    bytes_written += Tweet.write_to_file(tweet_obj, fp_tweets)
                    Wordcloud.write_to_file(wordcloud_list, fp_wordcloud)
                    file_closed = False
                    Util.vprint(opt.verbose, "bytes_written: " + str(bytes_written))

                    # close the file if reached limit
                    # rename (remove .tmp) and move to specified local / HDFS path
                    if (bytes_written >= opt.roll_size):
                        __close_tmp_mv(fp_tweets, fp_wordcloud, opt.hdfs_path, opt.local_path)    
                        file_closed = True
                
            print "Finished searching tweets for queries: "
            print query
    
        except tweepy.error.TweepError as te:
            print "Tweepy throws error"
            print te.reason
            print te.response
        
        except (KeyboardInterrupt, SystemExit):
            if write_to_file and not file_closed:
                print "Closing temporary files"
                fp_tweets.close()
                fp_wordcloud.close()
                __cleanup_tmp_dir(tmp_tweet_dir, tmp_wordcloud_dir, opt.hdfs_path, opt.local_path)
            
        # post loop
        # close the file, just in case it is not closed within the loop
        finally:
            if write_to_file and not file_closed:
                print "Closing temporary files"
                fp_tweets.close()
                fp_wordcloud.close()
                __cleanup_tmp_dir(tmp_tweet_dir, tmp_wordcloud_dir, opt.hdfs_path, opt.local_path)
                file_closed = True
                
    if write_to_file and not file_closed:
        print "Closing temporary files"
        fp_tweets.close()
        fp_wordcloud.close()
        __cleanup_tmp_dir(tmp_tweet_dir, tmp_wordcloud_dir, opt.hdfs_path, opt.local_path)

    print "Ending tweet searching"  
            

def __close_tmp_mv(fp_tweets, fp_wordcloud, hdfs_path=None, local_path=None):
    fp_tweets.close()
    fp_wordcloud.close()
    filename_tweets = fp_tweets.name
    filename_wordcloud = fp_wordcloud.name
    print "Will create new file " + filename_tweets[-24:-4] + ".csv"
    
    # Put to HDFS if specified to write to HDFS
    if hdfs_path:
        hadoopy.put(filename_tweets, hdfs_path + Util.TWEETS + '/' + filename_tweets[-24:-4] + '.csv')
        hadoopy.put(filename_wordcloud, hdfs_path + Util.WORDCLOUD + '/' +filename_wordcloud[-24:-4] + '.csv')
        
    # Put to local path if specified to write to local file system    
    if local_path:
        shutil.copy(filename_tweets, local_path + Util.TWEETS + '/' + filename_tweets[-24:-4] + '.csv')
        shutil.copy(filename_wordcloud, local_path + Util.WORDCLOUD + '/' + filename_wordcloud[-24:-4] + '.csv')
        
    os.remove(filename_tweets)
    os.remove(filename_wordcloud)


def __cleanup_tmp_dir(tmp_tweet_dir, tmp_wordcloud_dir, hdfs_path=None, local_path=None):

    tweet_files = os.listdir(tmp_tweet_dir)
    for tf in tweet_files:
        if hdfs_path:
            hadoopy.put(tmp_tweet_dir + '/' + tf, hdfs_path + Util.TWEETS + '/' + tf[-24:-4] + '.csv')
        if local_path:
            shutil.copy(tmp_tweet_dir + '/' + tf, local_path + Util.TWEETS + '/' + tf[-24:-4] + '.csv')
        os.remove(tmp_tweet_dir + '/' + tf)

    wordcloud_files = os.listdir(tmp_wordcloud_dir)
    for wf in wordcloud_files:
        if hdfs_path:
	       hadoopy.put(tmp_wordcloud_dir + '/' + wf, hdfs_path + Util.WORDCLOUD + '/' + wf[-24:-4] + '.csv')
        if local_path:
	       shutil.copy(tmp_wordcloud_dir + '/' + wf, local_path + Util.WORDCLOUD + '/' + wf[-24:-4] + '.csv')
        os.remove(tmp_wordcloud_dir + '/' + wf)


# our program's main entry point
if __name__ == '__main__':
    opt = Option.validate(sys.argv[1:])
    main(opt)
    # main(sys.argv[1:]) 

