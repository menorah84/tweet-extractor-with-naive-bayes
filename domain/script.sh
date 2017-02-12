#!/bin/bash

TWEEPY_PATH=$1
HTTPS_PROXY=$2
DOMAIN_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DATA_PATH=$DOMAIN_PATH/data

timestamp=`date -u +"%FT%H.%M.%SZ"`

mkdir $DOMAIN_PATH/tmp

# DEBUG
debug_ts=`date -u +"%FT%TZ"`
echo "$debug_ts"
echo "TWEEPY PATH: $TWEEPY_PATH" >> $DOMAIN_PATH/cron.log
echo "HTTPS PROXY: $HTTPS_PROXY" >> $DOMAIN_PATH/cron.log
# ENDDEBUG

# check our path for a search.log file
if [ -f $DOMAIN_PATH/search.log ]; 
then
	last_tweet_id=`tail -n1 $DOMAIN_PATH/search.log | awk '{print $2}'`
	
	# DEBUG
	echo "last tweet id: $last_tweet_id" >> $DOMAIN_PATH/cron.log
	echo "executing python scripts" >> $DOMAIN_PATH/cron.log
	echo "python $TWEEPY_PATH/src/search_tweets.py --since-tweet-id $last_tweet_id --keywords-credential-file $DOMAIN_PATH/config.json --https-proxy $HTTPS_PROXY --write-to-local $DOMAIN_PATH/tmp" >> $DOMAIN_PATH/cron.log
	# ENDDEBUG

	python $TWEEPY_PATH/src/search_tweets.py --since-tweet-id $last_tweet_id --keywords-credential-file $DOMAIN_PATH/config.json --https-proxy $HTTPS_PROXY --write-to-local $DOMAIN_PATH/tmp

else
	
	# DEBUG
	echo "no search log, no tweet id" >> $DOMAIN_PATH/cron.log
	echo "python $TWEEPY_PATH/src/search_tweets.py --keywords-credential-file config.json --https-proxy $HTTPS_PROXY --write-to-local $DOMAIN_PATH/tmp"  >> $DOMAIN_PATH/cron.log
	# END DEBUG

	python $TWEEPY_PATH/src/search_tweets.py --keywords-credential-file $DOMAIN_PATH/config.json --https-proxy $HTTPS_PROXY --write-to-local $DOMAIN_PATH/tmp
fi

echo "Catting" >> $DOMAIN_PATH/cron.log
cat $DOMAIN_PATH/tmp/tweets/* > $DOMAIN_PATH/tmp/merged.csv

# DEBUG
echo "timestamp: $timestamp" >> $DOMAIN_PATH/cron.log
# END DEBUG

sort -n -u -k 1,1 $DOMAIN_PATH/tmp/merged.csv > $DATA_PATH/$timestamp.csv

tweet_id=`tail -n1 $DATA_PATH/$timestamp.csv | awk '{print $1}'`

# DEBUG
echo "tail tweet id: $tweet_id" >> $DOMAIN_PATH/cron.log
echo "to write to search log: $timestamp	$tweet_id" >> $DOMAIN_PATH/cron.log
echo " " >> $DOMAIN_PATH/cron.log
printf "\n"
# END DEBUG

echo "$timestamp	$tweet_id" >> $DOMAIN_PATH/search.log

rm -r $DOMAIN_PATH/tmp


