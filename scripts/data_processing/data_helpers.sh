## collect data
COLLECT_DAILY_ARCHIVE_FILES () {
    ## mine all months, all days in specified years => twitter
    ## TODO: specify start/end months and generate range in-between end-start
    # arg 1: data directory
    # arg 2: start year
    # arg 3: end year
    ARCHIVE_FILES=()
    DATA_DIR=$1
    YEARS=($2 $3)
    MONTHS=(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec)
    START_DAY=1
    END_DAY=31

    MONTH_COUNT=$(expr "${#MONTHS[@]}" - 1)
    for YEAR in $(seq "${YEARS[0]}" "${YEARS[1]}");
    do
        for i in $(seq 0 "$MONTH_COUNT");
        do
            MONTH="${MONTHS[$i]}"
#             START_DAY="${START_DAYS[$i]}"
#             END_DAY="${END_DAYS[$i]}"
            for DAY in $(seq -f "%02g" "$START_DAY" "$END_DAY");
            do
                RELEVANT_FILE=$DATA_DIR/tweets-"$MONTH"-"$DAY"-"$YEAR"-[0-9][0-9]-[0-9][0-9].gz
                if [ -f $RELEVANT_FILE ];
                then
                    ARCHIVE_FILES+=($RELEVANT_FILE)
                fi
            done
        done
    done
    echo "${ARCHIVE_FILES[@]}"
}
COLLECT_MONTHLY_ARCHIVE_FILES () {
    ## mine all months, all days in specified years => reddit
    # arg 1: data directory
    # arg 2: start year
    # arg 3: end year
    ARCHIVE_FILES=()
    DATA_DIR=$1
    YEARS=($2 $3)
    MONTHS=$(seq -f "%02g" 1 12)
    FILE_EXT_TYPES=(bz2 xz zst)

    for YEAR in $(seq "${YEARS[0]}" "${YEARS[1]}");
    do
        for MONTH in $MONTHS;
        do
            # check for multiple file types
            for FILE_EXT_TYPE in "${FILE_EXT_TYPES[@]}";
            do            
                RELEVANT_FILE="$DATA_DIR"/"$YEAR"/RC_"$YEAR"-"$MONTH"."$FILE_EXT_TYPE"
                if [ -f $RELEVANT_FILE ];
                then
                    ARCHIVE_FILES+=($RELEVANT_FILE)
                fi
            done
#             RELEVANT_FILE_XZ="$DATA_DIR"/"$YEAR"/RC_"$YEAR"-"$MONTH".xz
#             if [ -f $RELEVANT_FILE_XZ ];
#             then
#                 ARCHIVE_FILES+=($RELEVANT_FILE_XZ)
#             fi
#             RELEVANT_FILE_="$DATA_DIR"/"$YEAR"/RC_"$YEAR"-"$MONTH".xz
#             if [ -f $RELEVANT_FILE_XZ ];
#             then
#                 ARCHIVE_FILES+=($RELEVANT_FILE_XZ)
#             fi
        done
    done
    echo "${ARCHIVE_FILES[@]}"
}
## select all posts from particular subreddit
PROCESS_ARCHIVE_FILE() {
# select posts from a specific subreddit
    ARCHIVE_FILE=$1
    SUBREDDIT_STR=$2
    OUT_DIR=$3
    FILE_BASE=${ARCHIVE_FILE##*/}
    FILE_BASE=${FILE_BASE/.bz2/}
    OUT_FILE="$OUT_DIR"/"$FILE_BASE"_subreddit="$SUBREDDIT_STR".txt
#     echo $ARCHIVE_FILE
#     echo $OUT_FILE
    bzcat $ARCHIVE_FILE | jq -c ". | select(.subreddit | try match(\"$SUBREDDIT_STR\"))" > $OUT_FILE
    # compress
    COMPRESS_OUT_FILE=${OUT_FILE/.txt/.gz}
    gzip -c $OUT_FILE > $COMPRESS_OUT_FILE
    rm $OUT_FILE
}