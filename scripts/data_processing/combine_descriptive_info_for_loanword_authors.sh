# combine descriptive info for authors for later extraction of demographics
# location, age...etc
# AUTHOR_DATA_FILES=(../../data/mined_tweets/loanword_light_verb_author_counts_CLUSTER=twitter_posts.tsv ../../data/mined_tweets/loanword_integrated_verb_author_counts_CLUSTER=twitter_posts.tsv)
# loanword author data
# AUTHOR_DATA_FILES=(../../data/mined_tweets/loanword_verb_posts_CLUSTER=twitter_posts_STARTDATE=2017_7_9_ENDDATE=2019_4_6.tsv)
# OUT_DIR=../../data/mined_tweets/
# OUT_FILE_NAME=loanword_author_descriptive_data
# non-loanword author data
AUTHOR_DATA_FILES=(../../data/mined_tweets/non_loanword_author_tweets/non_loanword_verb_authors_CLUSTER=twitter_posts_STARTDATE=2017_7_9_ENDDATE=2019_4_6.gz)
OUT_DIR=../../data/mined_tweets/non_loanword_author_tweets/
OUT_FILE_NAME=non_loanword_author_descriptive_data

python3 combine_descriptive_info_for_loanword_authors.py "${AUTHOR_DATA_FILES[@]}" --out_dir $OUT_DIR --out_file_name $OUT_FILE_NAME