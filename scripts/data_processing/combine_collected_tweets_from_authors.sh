# add source data to target data
SOURCE_DIR=../../data/mined_tweets/loanword_author_tweets_elasticsearch/
TARGET_DIR=../../data/mined_tweets/loanword_author_tweets/
ORIGINAL_AUTHOR_DATA=../../data/mined_tweets/loanword_verb_posts_CLUSTER=twitter_posts_STARTDATE=2017_7_9_ENDDATE=2019_4_6.tsv
FILE_MATCHER=".*_tweets.gz"

python3 combine_collected_tweets_from_authors.py $SOURCE_DIR $TARGET_DIR --original_author_data $ORIGINAL_AUTHOR_DATA --file_matcher $FILE_MATCHER