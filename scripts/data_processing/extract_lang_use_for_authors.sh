# extract lang use for loanword authors
# DATA_DIRS=(../../data/mined_tweets/loanword_integrated_verb_author_counts_CLUSTER\=twitter_posts_tweets/ ../../data/mined_tweets/loanword_light_verb_author_counts_CLUSTER\=twitter_posts_tweets/)
# DATA_DIRS=(../../data/mined_tweets/loanword_author_tweets/)
DATA_DIRS=(../../data/mined_tweets/loanword_author_tweets_all_archives/)
LANG_ID_DATA=lang_id.gz
OUT_DIR=../../data/mined_tweets/loanword_author_tweets_all_archives/

python3 extract_lang_use_for_authors.py "${DATA_DIRS[@]}" --lang_id_data $LANG_ID_DATA --out_dir $OUT_DIR