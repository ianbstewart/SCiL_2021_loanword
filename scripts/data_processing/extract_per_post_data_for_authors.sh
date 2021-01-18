# extract per-post data for authors, e.g. hashtags per post
LOANWORD_DATA=../../data/mined_tweets/loanword_verb_posts_CLUSTER=twitter_posts_STARTDATE=2017_7_9_ENDDATE=2019_4_6.tsv
EXTRA_LOANWORD_DATA=../../data/mined_tweets/loanword_author_tweets_all_archives_extra_loanword_tweets.gz
NATIVE_VERB_DATA=../../data/mined_tweets/loanword_author_tweets_all_archives/native_integrated_light_verbs_per_post.tsv
OUT_DIR=../../data/mined_tweets/
HASHTAG_COUNT_DATA=../../data/mined_tweets/loanword_author_tweets_all_archives_hashtag_freq.tsv
MENTION_COUNT_DATA=../../data/mined_tweets/loanword_author_tweets_all_archives_mention_freq.tsv
python extract_per_post_data_for_authors.py $LOANWORD_DATA $OUT_DIR --extra_loanword_data $EXTRA_LOANWORD_DATA --native_verb_data $NATIVE_VERB_DATA --hashtag_count_data $HASHTAG_COUNT_DATA --mention_count_data $MENTION_COUNT_DATA