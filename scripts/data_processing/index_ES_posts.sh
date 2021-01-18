# run lang ID and tokenizer on archive posts in ElasticSearch
## assume that we have separate sub-directory for each new index
## e.g. /hg190/elastic_search/es_instances_for_reddit_comments/2018/jan-june-2018
## WARNING this will delete all files that already exist for index => use wisely!!

## set up index
# choose post type
# POST_TYPE='reddit'
# POST_TYPE='reddit_submissions'
POST_TYPE='twitter'
if [ $POST_TYPE == 'twitter' ]; then
    ES_HOME_DIR=/hg190/elastic_search/es_instances_for_twitter_posts/
    POST_DIR=/hg190/corpora/twitter-crawl/new-archive/
    ES_CLUSTER_NAME="twitter_posts"
    ES_MAPPING_FILE="None"
elif [ $POST_TYPE == 'reddit' ]; then
    ES_HOME_DIR=/hg190/elastic_search/es_instances_for_reddit_comments
    POST_DIR=/hg190/corpora/reddit_full_comment_data/
    ES_CLUSTER_NAME="reddit_comments"
    ES_MAPPING_FILE=/hg190/elastic_search/submission_mapping_lang.json
elif [ $POST_TYPE == 'reddit_submissions' ]; then
    ES_HOME_DIR=/hg190/elastic_search/es_instances_for_reddit_submissions
    POST_DIR=/hg190/corpora/reddit_submissions/
    ES_CLUSTER_NAME="reddit_submissions"
    ES_MAPPING_FILE=/hg190/elastic_search/comment_mapping_reduced_lang_full_body.json
fi
if [ ! -d $ES_HOME_DIR ]; then
    mkdir -p $ES_HOME_DIR
fi
# choose time span
# NOTE: larger datasets (reddit) require shorter time spans to be loaded without error
# YEAR=2017
# YEAR=2018
YEAR=2019
MONTH_NAMES=(jan feb mar apr may june july aug sep oct nov dec)
# months: 01-03
# START_MONTH=1
# END_MONTH=3
# months: 04-06
# START_MONTH=4
# END_MONTH=6
# months: 07-09
# START_MONTH=7
# END_MONTH=9
# months: 10-12
START_MONTH=10
END_MONTH=12
# months: 01-06
# START_MONTH=1
# END_MONTH=6
# months: 07-12
# START_MONTH=7
# END_MONTH=12
MONTH="$START_MONTH"_"$END_MONTH"
MONTH_YEAR_STR="${MONTH_NAMES[$START_MONTH-1]}"-"${MONTH_NAMES[$END_MONTH-1]}"-"$YEAR"
# MONTH="$START_MONTH"_"$END_MONTH"
ES_INDEX="$ES_CLUSTER_NAME"_"$YEAR"_m_"$MONTH"
SCRIPT_DIR=$(pwd .)
# if ES instance is not set up, set up ES instance in new directory
ES_YEAR_MONTH_DIR="$ES_HOME_DIR"/"$YEAR"/"$MONTH_YEAR_STR"
echo $ES_YEAR_MONTH_DIR
# WARNING!! DELETE ALL EXISTING FILES (should probably add some user input here as safeguard)
if [ -d $ES_YEAR_MONTH_DIR ]; then
    rm -r $ES_YEAR_MONTH_DIR
fi
# remove index just to be safe
# curl -XDELETE 'localhost:9200/'$"$ES_INDEX"
if [ ! -d $ES_YEAR_MONTH_DIR ]; then
    mkdir -p $ES_YEAR_MONTH_DIR
    cd $ES_YEAR_MONTH_DIR
    ES_ORIGINAL_FILE=/hg190/elastic_search/elasticsearch-2.1.1.zip
    cp $ES_ORIGINAL_FILE .
    ES_ORIGINAL_FILE=${ES_ORIGINAL_FILE##*/}
    echo $ES_ORIGINAL_FILE
    unzip $ES_ORIGINAL_FILE
fi
ES_DIR="$ES_YEAR_MONTH_DIR"/elasticsearch-2.1.1/bin
cd $ES_DIR
export ES_HEAP_SIZE=50g
echo "./elasticsearch --cluster.name "$ES_CLUSTER_NAME" --node.name $ES_INDEX --Xmx $ES_HEAP_SIZE --Xms $ES_HEAP_SIZE"
(./elasticsearch --cluster.name $ES_CLUSTER_NAME --Xmx $ES_HEAP_SIZE --Xms $ES_HEAP_SIZE)&
PID=$!
echo $PID
# wait 5 mins for server to set up
ES_SETUP_TIME=300
sleep "$ES_SETUP_TIME"s

## optional: filter criteria
# valid languages
VALID_LANGS=(en es)
# VALID_LANGS=''

## verify that index exists!
curl 'localhost:9200/_cat/indices?v'

## add mapping
## TODO: add mapping with lang ID fields
# POST_MAP_FILE=/hg190/elastic_search/comment_mapping_reduced_lang.json
# cp $POST_MAP_FILE .
# POST_MAP_FILE=${POST_MAP_FILE##*/}
# echo 'localhost:9200/'$"$ES_INDEX"/"$ES_INDEX_TYPE"$'/_mapping' 
# echo @"$POST_MAP_FILE"
# curl -XPUT 'localhost:9200/'$"$ES_INDEX"/"$ES_INDEX_TYPE"$'/_mapping' -d @"$POST_MAP_FILE"

## index posts
cd $SCRIPT_DIR
python3 index_ES_posts.py $ES_INDEX --post_dir $POST_DIR --post_year $YEAR --post_start_month $START_MONTH --post_end_month $END_MONTH --post_type $POST_TYPE --valid_langs "${VALID_LANGS[@]}" --ES_mapping_file $ES_MAPPING_FILE

## shut down index
kill $PID