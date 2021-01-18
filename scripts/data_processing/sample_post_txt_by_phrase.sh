# sample post text by phrase
# e.g. all posts with "tweetear"
PHRASE_FILE=../../data/loanword_resources/wiktionary_twitter_reddit_loanword_verbs_integrated_verbs_query_phrases.tsv
OUT_DIR=../../data/mined_reddit_comments

ES_YEAR=2018
ES_START_MONTH=7
ES_END_MONTH=9
# ES_START_MONTH=10
# ES_END_MONTH=12
# ES_CLUSTER_NAME=reddit_comments
# ES_DIR=/hg190/elastic_search/es_instances_for_reddit_comments/
ES_CLUSTER_NAME=twitter_posts
ES_DIR=/hg190/elastic_search/es_instances_for_twitter_posts/
LANG=es

# ES version
python3 sample_post_txt_by_phrase.py $PHRASE_FILE --es_year $ES_YEAR --es_start_month $ES_START_MONTH --es_end_month $ES_END_MONTH --es_cluster_name $ES_CLUSTER_NAME --es_dir $ES_DIR --lang $LANG --out_dir $OUT_DIR

# dumb post-file version
# POST_DIR=/hg190/corpora/reddit_full_comment_data/2019
# POST_FILES=("$POST_DIR"/RC_2019-01.zst)
# POST_FILES=$(ls $POST_DIR/*.zst)
# echo "$POST_FILES"
# POST_FILE=/hg190/corpora/reddit_full_comment_data/2019/RC_2019-01.zst

# JOBS=1
# parallel --jobs $JOBS --verbose --bar python3 sample_post_txt_by_phrase.py $PHRASE_FILE {} ::: "$POST_FILES"