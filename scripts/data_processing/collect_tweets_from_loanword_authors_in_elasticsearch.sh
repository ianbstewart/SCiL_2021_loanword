AUTHOR_DATA=../../data/mined_tweets/loanword_verb_posts_CLUSTER=twitter_posts_STARTDATE=2017_7_9_ENDDATE=2019_4_6.tsv
ES_CLUSTER_NAME=twitter_posts
OUT_DIR=../../data/mined_tweets/loanword_author_tweets_elasticsearch/

python collect_tweets_from_loanword_authors_in_elasticsearch.py $AUTHOR_DATA --es_cluster_name $ES_CLUSTER_NAME --out_dir $OUT_DIR