#!/bin/bash

# check if first argument, the source files, exists
# exits if does not exist 
if [ $# -eq 0 ];
then
	echo "Error: Requires path where src/search_tweets.py is located"
	exit 1
else
	if [ ! -f $1/src/search_tweets.py ];
	then 
		echo "Error: Requires path where src/search_tweets.py is located"
		exit 1
	fi
fi

# check if second argument, the proxy, exists
# provides warning if does not exist
if [ -z "$2" ];
then
	echo "Warning: No HTTPS proxy indicated. May not connect to Twitter API and throw error if you're on a firewall"
fi

DOMAIN_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

crontab -l > mycron

# Runs every weekdays (1-5) every 11:00 AM (0 11) system time
echo "0 11 * * 1-5 $DOMAIN_PATH/script.sh $1 $2" >> mycron

# FOR TESTING ONLY! Runs every 30 minutes during Fridays (system time).
# echo "*/30 * * * 5 $DOMAIN_PATH/script.sh $1 $2" >> mycron

crontab mycron

rm mycron

