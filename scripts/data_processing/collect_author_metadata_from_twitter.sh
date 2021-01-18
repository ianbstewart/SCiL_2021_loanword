# collect metadata for loanword authors from Twitter
LOANWORD_DATA=../../data/mined_tweets/loanword_verb_posts_CLUSTER=twitter_posts_STARTDATE=2017_7_9_ENDDATE=2019_4_6.tsv

python collect_author_metadata_from_twitter.py $LOANWORD_DATA