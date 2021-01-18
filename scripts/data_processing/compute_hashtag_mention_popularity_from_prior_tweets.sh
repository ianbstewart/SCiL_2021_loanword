# compute hashtag mention popularity from prior tweets
DATA_DIR=../../data/mined_tweets/loanword_author_tweets_all_archives/
OUT_DIR=../../data/mined_tweets/
python compute_hashtag_mention_popularity_from_prior_tweets.py $DATA_DIR --out_dir $OUT_DIR