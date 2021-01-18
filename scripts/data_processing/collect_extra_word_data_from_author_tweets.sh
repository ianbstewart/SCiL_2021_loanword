# collect extra matching word data from author tweets
DATA_DIR=../../data/mined_tweets/loanword_author_tweets_all_archives/
OLD_DATA=../../data/mined_tweets/loanword_verb_posts_CLUSTER=twitter_posts_STARTDATE=2017_7_9_ENDDATE=2019_4_6.tsv
WORD_DATA=(../../data/loanword_resources/wiktionary_twitter_reddit_loanword_verbs_integrated_verbs_query_phrases.tsv ../../data/loanword_resources/wiktionary_twitter_reddit_loanword_verbs_light_verbs_query_phrases.tsv)
WORD_DATA_TYPES=(integrated_verb light_verb)
OUT_DIR=../../data/mined_tweets/
DATA_NAME=extra_loanword_tweets

python3 collect_extra_word_data_from_author_tweets.py $DATA_DIR --old_data $OLD_DATA --word_data "${WORD_DATA[@]}" --word_data_types "${WORD_DATA_TYPES[@]}" --out_dir $OUT_DIR --data_name $DATA_NAME