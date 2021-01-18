# get loanword integration counts over several years
## querying functions
function QUERY_WRITE_TO_FILE {
    LOANWORD=$1
    INTEGRATED_VERB_QUERY=$2
    LIGHT_VERB_QUERY=$3
    OUT_DIR=$4
    DATA_FILE=$5
    POST_LANG=$6
#     echo "data file $DATA_FILE"
#     echo "integrated verb query $INTEGRATED_VERB_QUERY"
    LIGHT_VERB_QUERY=$(echo "$LIGHT_VERB_QUERY" | sed 's/\$/ /g')
    # add spacing to queries
    # TODO: test jq for speed
    LIGHT_VERB_QUERY="(?<=[\.:;\!\? \"])$LIGHT_VERB_QUERY(?=[\.:;\!\? \"])"
    INTEGRATED_VERB_QUERY="(?<=[\.:;\!\? \"])$INTEGRATED_VERB_QUERY(?=[\.:;\!\? \"])"
#     echo "light verb query '$LIGHT_VERB_QUERY'"
    DATA_FILE_BASE=$(basename $DATA_FILE)
    DATA_FILE_BASE="${DATA_FILE_BASE%.*}"
#     echo "data file base $DATA_FILE_BASE"
    OUT_FILE="$OUT_DIR"/LOANWORD="$LOANWORD"_DATA="$DATA_FILE_BASE"_twitter_counts.txt
    if [ -f $OUT_FILE ]; then
        rm $OUT_FILE
    fi
#     echo "zgrep -i -P '$LIGHT_VERB_QUERY' $DATA_FILE"
    LIGHT_VERB_LOANWORD_COUNT=$(zgrep -i -P "$LIGHT_VERB_QUERY" $DATA_FILE | grep '"lang":'$"\"$POST_LANG\"" | wc -l)
    INTEGRATED_LOANWORD_COUNT=$(zgrep -i -P "$INTEGRATED_VERB_QUERY" $DATA_FILE | grep '"lang":'$"\"$POST_LANG\"" | wc -l)
#     # NOTE output file doesn't appear until after all data files are processed, quirk of `parallel`
    printf "$LOANWORD,$INTEGRATED_LOANWORD_COUNT,$LIGHT_VERB_LOANWORD_COUNT,$DATA_FILE_BASE\n" >> $OUT_FILE
}

# query file for different strings, write counts to file
function QUERY_FILES {
    LOANWORD=$1
    INTEGRATED_VERB_QUERY=$2
    LIGHT_VERB_QUERY=$3
    OUT_DIR=$4
    DATA_FILES_FILE=$5
    POST_LANG=$6
#     LIGHT_VERB_QUERY=$(echo $LIGHT_VERB_QUERY | sed 's/\$/ /g')
#     echo "querying loanword $LOANWORD"
#     echo "light verb query $LIGHT_VERB_QUERY"
    DATA_FILES=($(cat $DATA_FILES_FILE))
#     echo "DATA FILES"
#     echo "${DATA_FILES[@]}"
    
    # TODO: not good for parallel to write to same file?
#     OUT_FILE="$OUT_DIR"/LOANWORD="$LOANWORD"_twitter_counts.txt
#     if [ -f $OUT_FILE ]; then
#         rm $OUT_FILE
#     fi
    
    ## process all files in parallel
    JOBS=35
#     parallel --jobs $JOBS --bar QUERY_WRITE_TO_FILE $LOANWORD "$INTEGRATED_VERB_QUERY" "$LIGHT_VERB_QUERY" $OUT_DIR {} $POST_LANG ::: "${DATA_FILES[@]}"
    # need to add quotes to queries to protect parentheses
    parallel --jobs $JOBS --bar QUERY_WRITE_TO_FILE $LOANWORD '"'$INTEGRATED_VERB_QUERY$'"' $LIGHT_VERB_QUERY $OUT_DIR {} $POST_LANG ::: "${DATA_FILES[@]}"
#     # combine counts from files
    COMBINED_OUT_FILE="$OUT_DIR"/LOANWORD="$LOANWORD"_twitter_counts.txt
    MINED_OUT_FILES=($(ls "$OUT_DIR"/LOANWORD="$LOANWORD"_DATA=*twitter_counts.txt))
    echo "mined files ${MINED_OUT_FILES[@]}"
    cat "${MINED_OUT_FILES[@]}" > $COMBINED_OUT_FILE
    rm "${MINED_OUT_FILES[@]}"

#     for DATA_FILE in "${DATA_FILES[@]}";
#     do
#         # TODO: unique author counts? to avoid spammers
#         echo $DATA_FILE
#         DATA_FILE_BASE=$(basename $DATA_FILE)
#         echo "testing data $DATA_FILE_BASE"
#         INTEGRATED_LOANWORD_COUNT=$(zgrep -i -P "$INTEGRATED_VERB_QUERY" $DATA_FILE | grep '"lang":'$"\"$POST_LANG\"" | wc -l)
#         LIGHT_VERB_LOANWORD_COUNT=$(zgrep -i -P "$LIGHT_VERB_QUERY" $DATA_FILE | grep '"lang":'$"\"$POST_LANG\"" | wc -l)
#         # NOTE output file doesn't appear until after all data files are processed, quirk of `parallel`
#         printf "$LOANWORD,$INTEGRATED_LOANWORD_COUNT,$LIGHT_VERB_LOANWORD_COUNT,$DATA_FILE_BASE\n" >> $OUT_FILE 
#     done
}
function QUERY_TOTAL_COUNT() {
    OUT_DIR=$1
    DATA_FILE=$2
    POST_LANG=$3
    DATA_FILE_BASE=$(basename $DATA_FILE)
    DATA_FILE_BASE="${DATA_FILE_BASE%.*}"
    OUT_FILE="$OUT_DIR"/DATA="$DATA_FILE_BASE"_LANG="$POST_LANG"_twitter_total_counts.txt
#     echo "zgrep -i '\"lang\":'$\"\"$POST_LANG\"' $DATA_FILE"
    LINE_COUNT=$(zgrep -i "\"lang\":"$"\"$POST_LANG\"" $DATA_FILE | wc -l)
    printf "$LINE_COUNT,$DATA_FILE_BASE\n" > $OUT_FILE
}
function QUERY_TOTAL_COUNT_ALL_FILES() {
    OUT_DIR=$1
    DATA_FILES_FILE=$2
    POST_LANG=$3
    DATA_FILES=($(cat $DATA_FILES_FILE))
    ## process all files in parallel
    JOBS=30
    parallel --jobs $JOBS --bar QUERY_TOTAL_COUNT $OUT_DIR {} $POST_LANG ::: "${DATA_FILES[@]}"
    COMBINED_OUT_FILE="$OUT_DIR"/LANG="$POST_LANG"_twitter_total_counts.txt
    MINED_OUT_FILES=($(ls "$OUT_DIR"/DATA=*LANG="$POST_LANG"_twitter_total_counts.txt))
    echo "mined files ${MINED_OUT_FILES[@]}"
    cat "${MINED_OUT_FILES[@]}" > $COMBINED_OUT_FILE
    rm "${MINED_OUT_FILES[@]}"
}
export -f QUERY_FILES
export -f QUERY_WRITE_TO_FILE
export -f QUERY_TOTAL_COUNT
export -f QUERY_TOTAL_COUNT_ALL_FILES

MONTHS=("Jan" "Feb" "Mar" "Apr" "May" "Jun" "Jul" "Aug" "Sep" "Oct" "Nov" "Dec")
YEARS=$(seq 14 19)
DAY=15
DATA_FILES=()
for YEAR in $YEARS;
do
#     echo $YEAR
    if [ $YEAR == 14 ] || [ $YEAR == 15 ];
    then
        DATA_DIR=/hg190/corpora/twitter-crawl/daily-tweet-archives
    else
        DATA_DIR=/hg190/corpora/twitter-crawl/new-archive
    fi
    for MONTH in "${MONTHS[@]}";
    do
        DATA_FILE_MATCHES=$(ls "$DATA_DIR"/tweets-"$MONTH"-"$DAY"-"$YEAR"*.gz)
        DATA_FILES=("${DATA_FILES[@]}" $DATA_FILE_MATCHES)
    done
done
# hard coded lists
# DATA_FILES=("/hg190/corpora/twitter-crawl/new-archive/tweets-Jul-15-18-03-23.gz" "/hg190/corpora/twitter-crawl/new-archive/tweets-Jul-15-19-03-27.gz")
# DATA_FILES=("/hg190/corpora/twitter-crawl/daily-tweet-archives/tweets-Jul-01-14-00-00.gz" "/hg190/corpora/twitter-crawl/daily-tweet-archives/tweets-Jul-01-15-03-56.gz" "/hg190/corpora/twitter-crawl/new-archive/tweets-Jul-01-16-03-43.gz" "/hg190/corpora/twitter-crawl/new-archive/tweets-Jul-01-17-04-37.gz" "/hg190/corpora/twitter-crawl/new-archive/tweets-Jul-01-18-04-00.gz" "/hg190/corpora/twitter-crawl/new-archive/tweets-Jul-01-19-04-20.gz")
# LOANWORDS=("google" "tweet" "delete" "flirt" "ghost" "ban")
# INTEGRATED_LOANWORDS=("googlear" "tuitear" "deletear" "flirtear" "gostear" "banear")
# LOANWORD_PHRASES=("buscar en google" "poner un tweet" "dar delete" "hacer flirting" "hacer ghosting" "hacer ban")
INTEGRATED_LOANWORD_DATA=../../data/loanword_resources/wiktionary_twitter_reddit_loanword_verbs_integrated_verbs_query_phrases.tsv
INTEGRATED_VERB_QUERIES=($(tail -n +2 $INTEGRATED_LOANWORD_DATA | cut -f2))
# sanity check: "google" only
# INTEGRATED_VERB_QUERIES=($(tail -n +2 $INTEGRATED_LOANWORD_DATA | grep -i "google" | cut -f2))
LIGHT_VERB_LOANWORD_DATA=../../data/loanword_resources/wiktionary_twitter_reddit_loanword_verbs_light_verbs_query_phrases.tsv
LIGHT_VERB_QUERIES=($(tail -n +2 $LIGHT_VERB_LOANWORD_DATA | cut -f2 | sed 's/ /$/g')) # convert space to special char because of delimiter issues
# sanity check: "google" only
# LIGHT_VERB_QUERIES=($(tail -n +2 $LIGHT_VERB_LOANWORD_DATA | grep -i "google" | cut -f2 | sed 's/ /$/g'))
LOANWORDS=($(tail -n +2 $INTEGRATED_LOANWORD_DATA | cut -f1))
# sanity check: "google" only
# LOANWORDS=($(tail -n +2 $INTEGRATED_LOANWORD_DATA | grep -i "google" | cut -f1))
POST_LANG="es"
N=${#LOANWORDS[@]}
echo "$N loanwords"
OUT_DIR=../../output/twitter_word_count_data
if [ ! -d $OUT_DIR ]; then
    mkdir $OUT_DIR
fi

# write files to separate file for easier handling in parallel
DATA_FILES_FILE=twitter_data_files.txt
function join_by { local d=$1; shift; echo -n "$1"; shift; printf "%s" "${@/#/$d}"; }
join_by $'\n' "${DATA_FILES[@]}" > $DATA_FILES_FILE

# for i in $(seq 0 $((N-1))); 
# do
#     LOANWORD="${LOANWORDS[i]}"
# #     INTEGRATED_LOANWORD="${INTEGRATED_LOANWORDS[i]}"
# #     LOANWORD_PHRASE="${LOANWORD_PHRASES[i]}"
#     INTEGRATED_VERB_QUERY="${INTEGRATED_VERB_QUERIES[i]}"
# #     LIGHT_VERB_QUERY=$(echo "${LIGHT_VERB_QUERIES[i]}" | sed 's/\$/ /g')
#     LIGHT_VERB_QUERY="'"$"${LIGHT_VERB_QUERIES[i]}"$"'"
#     QUERY_FILES $LOANWORD $INTEGRATED_VERB_QUERY $LIGHT_VERB_QUERY $OUT_DIR $DATA_FILES_FILE $POST_LANG
# done

## get total counts for each file to normalize total counts
QUERY_TOTAL_COUNT_ALL_FILES $OUT_DIR $DATA_FILES_FILE $POST_LANG

## old ugly code
# JOBS=20
# parallel query
# parallel --link --jobs $JOBS --bar QUERY_FILE ::: "${LOANWORDS[@]}" ::: "${INTEGRATED_VERB_QUERIES[@]}" ::: "${LIGHT_VERB_QUERIES[@]}" ::: $OUT_DIR ::: $DATA_FILES_FILE ::: $POST_LANG

# serial query
# for i in $(seq 0 $((N-1))); 
# do
#     LOANWORD="${LOANWORDS[i]}"
# #     INTEGRATED_LOANWORD="${INTEGRATED_LOANWORDS[i]}"
# #     LOANWORD_PHRASE="${LOANWORD_PHRASES[i]}"
#     INTEGRATED_VERB_QUERY="${INTEGRATED_VERB_QUERIES[i]}"
#     LIGHT_VERB_QUERY=$(echo "${LIGHT_VERB_QUERIES[i]}" | sed 's/\$/ /g')
#     echo "testing loanword $LOANWORD"
# #     echo "light verb query <$LIGHT_VERB_QUERY>"
# #     echo "query $INTEGRATED_VERB_QUERY"
    
#     OUT_FILE="$OUT_DIR"/LOANWORD="$LOANWORD"_twitter_counts.txt
#     if [ -f $OUT_FILE ]; then
#         rm $OUT_FILE
#     fi
    
#     for DATA_FILE in "${DATA_FILES[@]}";
#     do
#         # TODO: unique author counts? to avoid spammers
#         echo $DATA_FILE
#         DATA_FILE_BASE=$(basename $DATA_FILE)
#         echo "testing data $DATA_FILE_BASE"
#         INTEGRATED_LOANWORD_COUNT=$(zgrep -i -P "$INTEGRATED_VERB_QUERY" $DATA_FILE | grep '"lang":'$"\"$LANG\"" | wc -l)
#         LIGHT_VERB_LOANWORD_COUNT=$(zgrep -i -P "$LIGHT_VERB_QUERY" $DATA_FILE | grep '"lang":'$"\"$LANG\"" | wc -l)
#         printf "$LOANWORD,$INTEGRATED_LOANWORD_COUNT,$LIGHT_VERB_LOANWORD_COUNT,$DATA_FILE_BASE\n" >> $OUT_FILE
#     done
# done