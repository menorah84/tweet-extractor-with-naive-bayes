"""
Author : menorah84
Created: 2016-15-02
Description: Models a tweet (Tweet) and words (Wordcloud) it contains as object
"""

from datetime import datetime
from utils import Util

class Tweet(object):

    def __init__(self, 
            tweet_id, 
            created_at,
            created_at_utc_date,
            created_at_utc_time,
            user_name, 
            screen_name, 
            user_desc, 
            user_location,
            text, 
            favorite_count, 
            retweet_count,
            latitude, 
            longitude,
            place_name, 
            place_full_name,
            place_country, 
            place_country_code,
            alt_latitude, 
            alt_longitude,
            sentiment):

        self.tweet_id = tweet_id
        self.created_at = created_at
        self.created_at_utc_date = created_at_utc_date
        self.created_at_utc_time = created_at_utc_time
        self.user_name = user_name
        self.screen_name = screen_name
        self.user_desc = user_desc
        self.user_location = user_location
        self.text = text
        self.favorite_count = favorite_count
        self.retweet_count = retweet_count
        self.latitude = latitude
        self.longitude = longitude
        self.place_name = place_name
        self.place_full_name = place_full_name
        self.place_country = place_country
        self.place_country_code = place_country_code
        self.alt_latitude = alt_latitude
        self.alt_longitude = alt_longitude
        self.sentiment = sentiment
    
    @staticmethod
    def tweet_wordcloud_from_json(tweet_json, analyzer):
            
        decoded = tweet_json

        tweet_id = decoded['id']
        created_at = decoded['created_at']
        dt = datetime.strptime(created_at[4:], "%b %d %H:%M:%S +0000 %Y")    
        created_at_utc_date = dt.strftime('%Y-%m-%d')
        created_at_utc_time = dt.strftime('%H:%M:%S')

        user_name = None
        if decoded['user']['name']:
            user_name = Util.clean_text(decoded['user']['name'].strip().encode('ascii', 'ignore'))

        screen_name = decoded['user']['screen_name']

        user_desc = None
        if decoded['user']['description']:
            user_desc = Util.clean_text(decoded['user']['description'].strip().encode('ascii', 'ignore'))
    
        user_location = None
        if decoded['user']['location']:
            user_location = Util.clean_text(decoded['user']['location'].strip().encode('ascii', 'ignore'))
        # text = Util.clean_text(decoded['text'].strip().encode('utf-8')).decode('utf-8')

        text = Util.clean_text(decoded['text'].strip().encode('ascii', 'ignore'))
    
        # text for the meantime
        sentiment, wordarray = analyzer.sentiment(text)

        favorite_count = decoded['favorite_count']
        retweet_count = decoded['retweet_count']
    
        # latitude, longitude
        latitude = None
        longitude = None
        if decoded['geo'] is not None:
            latitude = decoded['geo']['coordinates'][0]
            longitude = decoded['geo']['coordinates'][1]

        # place_name, place_full_name, place_country, place_country_code, alt_longitude, alt_latitude
        place_name = None
        place_full_name = None
        place_country = None
        place_country_code = None
        alt_latitude = None
        alt_longitude = None
        if decoded['place'] is not None:
            place_name = decoded['place']['name'].encode('ascii', 'ignore')
            place_full_name = decoded['place']['full_name'].encode('ascii', 'ignore')
            place_country = decoded['place']['country'].encode('ascii', 'ignore')
            place_country_code = decoded['place']['country_code'].encode('ascii', 'ignore')
            alt_longitude, alt_latitude = Util.get_alternative_coordinates(decoded['place']['bounding_box']['coordinates'][0])

        tweet_obj = Tweet(tweet_id, 
                created_at,
                created_at_utc_date,
                created_at_utc_time,
                user_name, 
                screen_name, 
                user_desc, 
                user_location,
                text, 
                favorite_count, 
                retweet_count,
                latitude, 
                longitude,
                place_name, 
                place_full_name,
                place_country, 
                place_country_code,
                alt_latitude, 
                alt_longitude,
                sentiment)

        return tweet_obj, [word.lower() for word in wordarray]

    def print_to_console(self):
        print 'tweet_id: ' + str(self.tweet_id)
        print 'created_at: ' + self.created_at
        print 'created_at_utc_date: ' + self.created_at_utc_date
        print 'created_at_utc_time: ' + self.created_at_utc_time        
        print 'user_name: ' + self.user_name
        print 'screen_name: ' + self.screen_name
        print 'user_desc: ' + self.user_desc
        print 'user_location: ' + self.user_location
        print 'text: ' + self.text
        print 'favorite_count: ' + str(self.favorite_count)
        print 'retweet_count: ' + str(self.retweet_count)
        print 'latitude: ' + str(self.latitude)
        print 'longitude: ' + str(self.longitude)
        print 'place_name: ' + str(self.place_name)
        print 'place_full_name: ' + str(self.place_full_name)
        print 'place_country: ' + str(self.place_country)
        print 'alt_latitude: ' + str(self.alt_latitude)
        print 'alt_longitude: ' + str(self.alt_longitude)
        print 'sentiment: ' + self.sentiment

    # converts this object to string delimited by tab
    def to_tsv_str(self):

        str_val = str(self.tweet_id) + '\t' + \
            self.created_at + '\t' + \
            self.created_at_utc_date + '\t' + \
            self.created_at_utc_time + '\t' + \
            Util.empty_str_if_none(self.user_name) + '\t' + \
            self.screen_name + '\t' + \
            Util.empty_str_if_none(self.user_desc) + '\t' + \
            Util.empty_str_if_none(self.user_location) + '\t' + \
            self.text + '\t' + \
            str(self.favorite_count) + '\t' + \
            str(self.retweet_count) + '\t' + \
            Util.empty_str_if_none(self.latitude) + '\t' + \
            Util.empty_str_if_none(self.longitude) + '\t' + \
            Util.empty_str_if_none(self.place_name) + '\t' + \
            Util.empty_str_if_none(self.place_full_name) + '\t' + \
            Util.empty_str_if_none(self.place_country) + '\t' + \
            Util.empty_str_if_none(self.place_country_code) + '\t' + \
            Util.empty_str_if_none(self.alt_latitude) + '\t' + \
            Util.empty_str_if_none(self.alt_longitude) + '\t' + \
            self.sentiment

        return str_val

    @staticmethod
    def write_to_file(tweet, fp):
        
        tsv_str = tweet.to_tsv_str() + '\n'
    
        tsv_str_utf8 = tsv_str.encode('utf-8')

        bytes_written = len(tsv_str_utf8)

        fp.write(tsv_str_utf8)

        return bytes_written


class Wordcloud(object):
    
    def __init__(self, tweet_id, word):
        self.tweet_id = tweet_id
        self.word = word

    @staticmethod
    def list_from_array(tweet_id, wordarray):

        wordcloud_list = []
        
        for word in wordarray:
            
            wordcloud_list.append(Wordcloud(tweet_id, word))

        return wordcloud_list
        
    def to_tsv_str(self):
        
        return str(self.tweet_id) + '\t' + self.word

    @staticmethod
    def write_to_file(wordcloud_list, fp):

        tsv_str_utf8 = ''

        for wordcloud in wordcloud_list:
            tsv_str = wordcloud.to_tsv_str() + '\n'
            tsv_str_utf8 += tsv_str.encode('utf-8')

        if tsv_str_utf8:
            fp.write(tsv_str_utf8)
