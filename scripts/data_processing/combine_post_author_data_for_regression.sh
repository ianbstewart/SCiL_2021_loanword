# combine post, author social data for regression
# loanword data
# original loanword data
# POST_DATA=../../data/mined_tweets/loanword_verb_posts_CLUSTER=twitter_posts_STARTDATE=2017_7_9_ENDDATE=2019_4_6.tsv
# EXTRA_POST_DATA=../../data/mined_tweets/loanword_author_tweets_all_archives_extra_loanword_tweets.gz
# PER_POST_DATA=../../data/mined_tweets/loanword_author_per_post_extra_data.tsv
# DATA_NAME=loanword_verbs
# native verb data
POST_DATA=../../data/mined_tweets/loanword_author_tweets_all_archives/native_integrated_light_verbs_per_post.tsv
PER_POST_DATA=../../data/mined_tweets/native_verb_author_per_post_extra_data.tsv
DATA_NAME=native_verbs
# author data
AUTHOR_DATA=../../data/mined_tweets/loanword_authors_combined_full_social_data.tsv

OUT_DIR=../../data/mined_tweets/

# loanwords
# python3 combine_post_author_data_for_regression.py --post_data $POST_DATA --author_data $AUTHOR_DATA --per_post_data $PER_POST_DATA --out_dir $OUT_DIR --data_name $DATA_NAME --extra_post_data $EXTRA_POST_DATA
# native verbs
python3 combine_post_author_data_for_regression.py --post_data $POST_DATA --author_data $AUTHOR_DATA --per_post_data $PER_POST_DATA --out_dir $OUT_DIR --data_name $DATA_NAME