# collect tweets from loanword authors, collected via ES
# loanword authors
# AUTHOR_DATA=../../data/mined_tweets/loanword_tweets_CLUSTER\=twitter_posts_STARTDATE\=2017_7_9_ENDDATE\=2019_4_6.tsv
# AUTHOR_DATA=../../data/mined_tweets/loanword_integrated_verb_author_counts_CLUSTER=twitter_posts.tsv
# AUTHOR_DATA=../../data/mined_tweets/loanword_light_verb_author_counts_CLUSTER=twitter_posts.tsv
# AUTHOR_DATA=../../data/mined_tweets/loanword_verb_posts_CLUSTER=twitter_posts_STARTDATE=2017_7_9_ENDDATE=2019_4_6.tsv
# AUTHOR_DATA=../../data/mined_tweets/loanword_verb_posts_CLUSTER=twitter_posts_STARTDATE=2017_7_9_ENDDATE=2019_4_6_SPLIT=1.tsv
# OUT_DIR=../../data/mined_tweets/loanword_author_tweets/
# non-loanword authors
AUTHOR_DATA=../../data/mined_tweets/non_loanword_author_tweets/non_loanword_verb_authors_CLUSTER=twitter_posts_STARTDATE=2017_7_9_ENDDATE=2019_4_6.gz
OUT_DIR=../../data/mined_tweets/non_loanword_author_tweets/
DATE_RANGE=('2014-01-01' '2019-07-01')
# TODO: more tweets per person ;_;
# AUTHORS_PER_LOANWORD=1000
AUTHORS_PER_LOANWORD=0
MAX_TWEETS=1000
OVERWRITE_FILES=False
# FILTER_SAMPLE="loanword_type=light_verb_loanword"
# FILTER_SAMPLE="loanword_type=integrated_loanword"
# FILTER_SAMPLE="" # all authors
TWITTER_AUTH_DATA=../../data/mined_tweets/twitter_auth.csv
AUTHOR_EXIST_DATA=../../data/mined_tweets/author_exist_data.tsv

# python3 collect_tweets_from_loanword_authors.py $AUTHOR_DATA --out_dir $OUT_DIR --date_range "${DATE_RANGE[@]}" --authors_per_loanword $AUTHORS_PER_LOANWORD --max_tweets $MAX_TWEETS --overwrite_files $OVERWRITE_FILES --filter_sample $FILTER_SAMPLE --twitter_auth_data $TWITTER_AUTH_DATA --author_exist_data $AUTHOR_EXIST_DATA
# all authors
python3 collect_tweets_from_loanword_authors.py $AUTHOR_DATA --out_dir $OUT_DIR --date_range "${DATE_RANGE[@]}" --authors_per_loanword $AUTHORS_PER_LOANWORD --max_tweets $MAX_TWEETS --overwrite_files $OVERWRITE_FILES --twitter_auth_data $TWITTER_AUTH_DATA --author_exist_data $AUTHOR_EXIST_DATA
