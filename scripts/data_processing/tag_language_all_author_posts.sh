# tag language in all author posts
DATA_DIR=../../data/mined_tweets/loanword_author_tweets/
# DATA_DIR=../../data/mined_tweets/loanword_light_verb_author_counts_CLUSTER\=twitter_posts_tweets/
DATA_TYPE=twitter

# limit thread use RIP
MAX_CPU_USE=20
export OMP_NUM_THREADS=$MAX_CPU_USE
python3 tag_language_all_author_posts.py $DATA_DIR --data_type $DATA_TYPE