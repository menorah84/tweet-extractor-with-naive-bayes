"""
Author : menorah84
Created: 2016-15-02
Description: Utilizes the Twitter Search API to search for tweets and writes to file if specified in options.
"""

import hadoopy
import json
import getopt
import sys

from os import path
from urlparse import urlparse
from utils import Util

class Option(object):

    def __init__(self, keywords=None, verbose=False,
                    hdfs_path=None, local_path=None, 
                    roll_size=Util.DEFAULT_ROLL_SIZE,
                    consumer_key=None, consumer_secret=None,
                    access_token_key=None, access_token_secret=None,
                    since_tweet_id=None, https_proxy=None):
        
        if keywords is None:
            keywords = []
        self.keywords = keywords
        self.verbose = verbose
        self.hdfs_path = hdfs_path
        self.local_path = local_path
        self.roll_size = roll_size
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.access_token_key = access_token_key
        self.access_token_secret = access_token_secret
        self.since_tweet_id = since_tweet_id
        self.https_proxy = https_proxy

    @staticmethod
    def validate(argv):
    
        keywords_selected = False
        keywords = []

        verbose = False
        hdfs_path = None
        local_path = None

        roll_size_selected = False
        roll_size = None
        
        consumer_key = None
        consumer_secret = None
        access_token_key = None
        access_token_secret = None

        credential_file_selected = False
        keywords_credential_file_selected = False
        credential_file_path = ''
        
        since_tweet_id = None
        https_proxy = None

        try:
            # second arg is (short) options, should be separated by :
            # third arg is long options, as an array
            opts, args = getopt.getopt(argv, "", 
                ["keywords=", "verbose", 
                 "write-to-hdfs=", "write-to-local=", "roll-size=", 
                 "consumer-key=", "consumer-secret=", 
                 "access-token-key=", "access-token-secret=", 
                 "credential-file=", "keywords-credential-file=",
                 "since-tweet-id=", "https-proxy="])

        except getopt.GetoptError:
            print Option.print_help()
            sys.exit(2)

        for opt, arg in opts:
            if opt == '--help':
                Option.print_help()

            elif opt == '--keywords':
                if keywords_credential_file_selected:
                    print "Error: You cannot choose --keywords and --keywords-credential-file at the same time"
                    sys.exit(2)
                else:
                    keywords_selected = True
                    keywords = arg.split(",")

            elif opt == '--verbose':
                verbose = True

            elif opt == '--write-to-hdfs':
                # validate and parse hdfs path
                hdfs_path = Option.parse_hdfs_path(arg)
                # if not hdfs_path or not hadoopy.exists(hdfs_path):
                if not hdfs_path:
                    print "Error: URL should be valid. Ex. hdfs://<host>:<port>/hdfs/dir"
                    sys.exit(2)
                elif not hadoopy.exists(hdfs_path):
                    print "Error: HDFS path does not exist"
                    sys.exit(2)
                elif not hdfs_path.endswith("/"):
                    hdfs_path = hdfs_path + "/"

            elif opt == '--write-to-local':
                # validate local path
                if not path.isdir(arg):
                    print "Error: Local path is not a directory or does not exist."
                    sys.exit(2)
                else:
                    local_path = arg if arg.endswith('/') else arg + '/'

            elif opt == '--roll-size':
                right_format, total_size, message = Option.parse_roll_size(arg, Util.MIN_ROLL_SIZE, Util.MAX_ROLL_SIZE) 
                if right_format:
                    roll_size_selected = True
                    roll_size = total_size
                else:
                    print message
                    sys.exit(2)

            elif opt == '--credential-file':
                if keywords_credential_file_selected:
                    print "Error: You cannot choose --credential-file and --keywords-credential-file at the same time"
                    sys.exit(2)
                else:
                    credential_file_selected = True
                    credential_file_path = arg
                
            elif opt == '--keywords-credential-file':
                if keywords_selected or credential_file_selected:
                    print "Error: You cannot choose --keywords-credential-file with --keywords and/or --keywords-credential-file"
                    sys.exit(2)
                else:
                    keywords_credential_file_selected = True
                    credential_file_path = arg
                    
            elif opt == '--consumer-key':
                consumer_key = arg
                
            elif opt == '--consumer-secret':
                consumer_secret = arg
                
            elif opt == '--access-token-key':
                access_token_key = arg
                
            elif opt == '--access-token-secret':
                access_token_secret = arg
                    
            elif opt == '--since-tweet-id':
                if len(str(arg)) < 18:
                    print "Warning: Invalid tweet id; ignoring set value."
                else:
                    since_tweet_id = arg 
                    
            elif opt == '--https-proxy':
                if not Option.parse_https_proxy(arg):
                    print "Warning: Possibly invalid HTTPS PROXY URL string; ignoring set value."
                else:    
                    https_proxy = arg


        if not keywords_selected and not keywords_credential_file_selected:
            print "Error: Keywords are required"
            sys.exit(2)

        if credential_file_selected:
            valid, error_message, consumer_key, consumer_secret, access_token_key, access_token_secret, temp_keywords = Option.validate_keywords_credential_file(credential_file_path, False)
            if not valid:
                print error_message
                sys.exit(2)
    
        if keywords_credential_file_selected:
            valid, error_message, consumer_key, consumer_secret, access_token_key, access_token_secret, keywords = Option.validate_keywords_credential_file(credential_file_path, True)
            if not valid:
                print error_message
                sys.exit(2)
                
        if not (consumer_key and consumer_secret and access_token_key and access_token_secret):
            print str(consumer_key) + ', ' + str(consumer_secret) + ', ' + str(access_token_key) + ', ' + str(access_token_secret)
            print "Error: Incomplete Twitter credentials."
            sys.exit(2)

        if not roll_size_selected:
            if hdfs_path or local_path:    
                print "Info: --roll-size not specified. Will default to roll size = 1048576 bytes (1 MB)." 
                roll_size_selected = True
                roll_size = Util.DEFAULT_ROLL_SIZE
        else:
            if not hdfs_path and not local_path:
                print "Warning: --roll-size flag ignored. No file to save to."
                roll_size = None
        
        print 'keywords: ' + ",".join(keywords)
        print 'verbose: ' + str(verbose)
        print 'hdfs_path: ' + str(hdfs_path)
        print 'local_path: ' +  str(local_path)
        print 'roll_size_selected: ' + str(roll_size_selected)
        print 'roll_size: ' + str(roll_size)
        print 'consumer_key: ' + str(consumer_key)
        print 'consumer_secret: ' + str(consumer_secret)
        print 'access_token_key: ' + str(access_token_key)
        print 'access_token_secret: ' + str(access_token_secret)
        print 'since_tweet_id: ' + str(since_tweet_id)
        print 'https_proxy: ' + str(https_proxy)

        return Option(keywords, verbose, hdfs_path, local_path, 
                      roll_size, consumer_key, consumer_secret, 
                      access_token_key, access_token_secret,
                      since_tweet_id, https_proxy)

    @staticmethod
    def print_help():
        print "search_tweets | stream_tweets.py"
        print "    [--keywords <comma separated keywords> (req.)] [--verbose] "
        print "    [--write-to-hdfs <existing hdfs dir>]" 
        print "    [--write-to-local <existing local dir>]"
        print "    [--roll-size <roll size>]"
        print "    [--consumer-key <twitter consumer key>]"
        print "    [--consumer-secret <twitter consumer secret>]"
        print "    [--access-token-key <access token key>]"
        print "    [--access-token-secret <access token secret>]"
        print "    [--credential-file <json file containing twitter credential>]"
        print "    [--keywords-credential-file <json file with keywords & twitter credential>]"
        print "    [--since-tweet-id <get tweets after this tweet_id (only for search api)]"
        print "    [--https-proxy <HTTPS proxy url string, with port>]"
        print "    [--help]"
        
    
    # parses and validates the HTTPS PROXY URL STRING
    # returns True if valid, else False    
    @staticmethod
    def parse_https_proxy(https_proxy_str):
        
        parsed = urlparse(https_proxy_str)
        
        if parsed is None:
            return False
        
        if parsed.scheme != 'https':
            return False
        
        try:
            parsed.netloc.split(':')
        except ValueError:
            return False
        
        # there should be no path after the URL str
        if parsed.path not in ('', '/'):
            return False
        
        return True

    # returns same argument, hdfs_path
    # returns None if URL invalid
    @staticmethod
    def parse_hdfs_path(hdfs_path):

        parsed = urlparse(hdfs_path)

        if parsed is None:
            return None
    
        # check if there is a HDFS on url scheme
        if parsed.scheme != 'hdfs':
            return None

        # check if port indicated
        try:
            host, port = parsed.netloc.split(':')
        except ValueError:
            return None

        # port should be a number and should be 4-5 digit long
        if not port.isdigit() or not len(port) in (4, 5):
            return None

        if parsed.path == '':
            return hdfs_path + '/'

        return hdfs_path


    # returns True, converted_bytes if valid
    # returns False, converted_bytes if wrong format or exceeding (min, max)
    @staticmethod
    def parse_roll_size(roll_size, min_bytes, max_bytes):

        denom = roll_size[-1:].lower()
        size = roll_size[:-1]

        mega = 1048576
        kilo = 1024
    
        if denom in ('m', 'k') and size.isdigit():
        
            int_size = int(size)
            total_size = 0
        
            if denom == 'm':
                total_size = int_size * mega
            elif denom == 'k':
                total_size = int_size * kilo

            if min_bytes <= total_size <= max_bytes:
                return True, total_size, ""
            else:
                message = "Error: Size not in range. Minimum of " + str(min_bytes) + ", maximum of " + str(max_bytes) + " bytes."
                return False, kilo, message

        else:
            message = "Error: Unable to parse roll size! Ex. 30M, 180K."
            return False, kilo, message


    # validates if json file containing Twitter keywords and credentials exists, and 
    # returns True, error message (empty string), consumer_key, consumer_secret, access_token_key, access_token_secret[, keywords]
    # returns False, error message
    @staticmethod
    def validate_keywords_credential_file(file_path, include_keywords):

        # check if such file exists
        if not path.isfile(file_path):
            print file_path
            print path.isfile(file_path)
            if include_keywords:
                return False, "Error: File containing keywords and credentials does not exist", None, None, None, None, None
            else:
                return False, "Error: File containing credentials does not exist", None, None, None, None, None
    
        fp = open(file_path, "r")
        try:
            credentials = json.load(fp)
        except ValueError:
            return False, "Error: Possibly invalid json format", None, None, None, None, None
        fp.close()

        try:    
            consumer_key = credentials['consumer_key']
            if consumer_key is None:
                return False, "Error: Missing consumer key", None, None, None, None, None
        except KeyError:
            return False, "Error: Missing consumer key", None, None, None, None, None

        try:    
            consumer_secret = credentials['consumer_secret']
            if consumer_secret is None:
                return False, "Error: Missing consumer secret", None, None, None, None, None
        except KeyError:
            return False, "Error: Missing consumer secret", None, None, None, None, None

        try:    
            access_token_key =    credentials['access_token_key']
            if access_token_key is None:
                return False, "Error: Missing access token key", None, None, None, None, None
        except KeyError:
            return False, "Error: Missing access token key", None, None, None, None, None

        try:    
            access_token_secret = credentials['access_token_secret']
            if access_token_secret is None:
                return False, "Error: Missing access token secret", None, None, None, None, None
        except KeyError:
            return False, "Error: Missing access token secret", None, None, None, None, None
        
        if include_keywords:
            try:
                keywords_str = credentials['keywords']
                if keywords_str is not None:
                    keywords = [word.encode('utf-8') for word in keywords_str]
                else:
                    return False, "Error: Missing keywords", None, None, None, None, None
            except KeyError:
                return False, "Error: Missing keywords", None, None, None, None, None
        
            return True, "", consumer_key, consumer_secret, access_token_key, access_token_secret, keywords
        else: 
            return True, "", consumer_key, consumer_secret, access_token_key, access_token_secret, None

