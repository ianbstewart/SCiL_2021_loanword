# get lang ID counts

# twitter
# ARCHIVE_DIR=/hg190/corpora/twitter-crawl/new-archive # 2016-
# ARCHIVE_DIR=/hg190/corpora/twitter-crawl/daily-tweet-archives # 2014-15
# LANG_ID_ARCHIVE_FILES=$(ls $ARCHIVE_DIR/lang_id/*.gz)
# OUT_DIR=../../data/mined_tweets/lang_id_counts

# reddit
ARCHIVE_DIR=/hg190/corpora/reddit_full_comment_data
LANG_ID_ARCHIVE_FILES=$(ls $ARCHIVE_DIR/*/lang_id/*.gz)
OUT_DIR=../../data/mined_reddit_comments/lang_id_counts

if [ ! -d $OUT_DIR ];
then
    mkdir -p $OUT_DIR
fi

## TODO: why do we get "permissions denied" when we call this function?
COUNT_LANG_IDS () {
    LANG_ID_ARCHIVE_FILE=$1
    OUT_DIR=$2
    LANG_ID_COUNT_FILE=${LANG_ID_ARCHIVE_FILE##*/}
    LANG_ID_COUNT_FILE=${LANG_ID_COUNT_FILE/".tsv.gz"/"_count.tsv"}
    LANG_ID_COUNT_FILE=$OUT_DIR/$LANG_ID_COUNT_FILE
    echo $LANG_ID_COUNT_FILE
    # get unique lang counts
#     zcat $LANG_ID_ARCHIVE_FILE | cut -f2 | sort | uniq -c > $LANG_ID_COUNT_FILE
    # fix file format from "COUNT LANG" to "LANG \t COUNT"
    python fix_uniq_count_file_format.py $LANG_ID_COUNT_FILE
}

## get lang ID counts
## serial
for LANG_ID_ARCHIVE_FILE in $LANG_ID_ARCHIVE_FILES;
do
#     COUNT_LANG_IDS $LANG_ID_ARCHIVE_FILE $OUT_DIR
    LANG_ID_COUNT_FILE=${LANG_ID_ARCHIVE_FILE##*/}
    LANG_ID_COUNT_FILE=${LANG_ID_COUNT_FILE/".tsv.gz"/"_count.tsv"}
    LANG_ID_COUNT_FILE=$OUT_DIR/$LANG_ID_COUNT_FILE
    echo $LANG_ID_COUNT_FILE
#     # get unique lang counts
#     zcat $LANG_ID_ARCHIVE_FILE | cut -f2 | sort | uniq -c > $LANG_ID_COUNT_FILE
    # fix file format from "COUNT LANG" to "LANG \t COUNT"
    python fix_uniq_count_file_format.py $LANG_ID_COUNT_FILE
done
## parallel
# JOBS=20
# parallel --jobs $JOBS --bar --verbose $COUNT_LANG_IDS {} $OUT_DIR ::: $(ls $LANG_ID_ARCHIVE_FILES)