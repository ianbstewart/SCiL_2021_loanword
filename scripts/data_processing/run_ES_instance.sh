# start ES server
# ES_CLUSTER_NAME=reddit_comments
ES_CLUSTER_NAME=twitter_posts
# ES_CLUSTER_NAME=reddit_submissions
ES_HOME_DIR=/hg190/elastic_search/es_instances_for_"$ES_CLUSTER_NAME"
YEAR=2018
# YEAR=2019
# 1-3
# START_MONTH=1
# END_MONTH=3
# 4-6
# START_MONTH=4
# END_MONTH=6
# 1-6
# START_MONTH=1
# END_MONTH=6
# 7-9
START_MONTH=7
END_MONTH=9
# 10-12
# START_MONTH=10
# END_MONTH=12
MONTH_NAMES=(jan feb mar apr may june july aug sep oct nov dec)
MONTH="$START_MONTH"_"$END_MONTH"
MONTH_YEAR_STR="${MONTH_NAMES[$START_MONTH-1]}"-"${MONTH_NAMES[$END_MONTH-1]}"-"$YEAR"
# MONTH="$START_MONTH"_"$END_MONTH"
ES_INDEX="$ES_CLUSTER_NAME"_"$YEAR"_m_"$MONTH"
ES_DIR="$ES_HOME_DIR"/"$YEAR"/"$MONTH_YEAR_STR"
ES_INDEX="$ES_CLUSTER_NAME"_"$YEAR"_m_"$MONTH"

# run instance
PORT=9400
# PORT=9500
MAX_MEM=25g
cd "$ES_DIR"/elasticsearch-2.1.1/bin/
# ./elasticsearch --cluster.name $ES_CLUSTER_NAME --node.name $ES_INDEX --Xms $MAX_MEM --Xmx $MAX_MEM
./elasticsearch -Des.http.port=$PORT --cluster.name $ES_CLUSTER_NAME --node.name $ES_INDEX --Xms $MAX_MEM --Xmx $MAX_MEM