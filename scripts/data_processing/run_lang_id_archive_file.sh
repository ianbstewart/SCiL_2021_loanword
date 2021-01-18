## run langid on archive (JSON) tweets
source data_helpers.sh

## collect data
## twitter data
# START_YEAR=16
# END_YEAR=19
# START_YEAR=14
# END_YEAR=15
# # ARCHIVE_DIR=/hg190/corpora/twitter-crawl/new-archive
# ARCHIVE_DIR=/hg190/corpora/twitter-crawl/daily-tweet-archives
# OUT_DIR="$ARCHIVE_DIR"/lang_id
# if [ ! -d $OUT_DIR ];
# then
#     mkdir -p $OUT_DIR
# fi
# ARCHIVE_FILES=$(COLLECT_DAILY_ARCHIVE_FILES $ARCHIVE_DIR $START_YEAR $END_YEAR)
# TXT_VAR='text'
# ID_VAR='id'
## reddit data
START_YEAR=2017
END_YEAR=2019
ARCHIVE_DIR=/hg190/corpora/reddit_full_comment_data
ARCHIVE_FILES=$(COLLECT_MONTHLY_ARCHIVE_FILES $ARCHIVE_DIR $START_YEAR $END_YEAR)
echo $ARCHIVE_FILES
# custom list
# ARCHIVE_FILES=($ARCHIVE_DIR/2013/RC_2013-12.bz2 $ARCHIVE_DIR/2014/RC_2014-12.bz2)
TXT_VAR='body'
ID_VAR='id'

## parallel
JOBS=20
# generated list
parallel --jobs $JOBS --bar --verbose python run_lang_id_archive_file.py {} --text_var $TXT_VAR --id_var $ID_VAR ::: $(ls $ARCHIVE_FILES)
# custom list
# parallel --jobs $JOBS --bar --verbose python run_lang_id_archive_file.py {} --text_var $TXT_VAR --id_var $ID_VAR ::: "${ARCHIVE_FILES[@]}"

## combine all files
## actually don't do this; we should keep them separate for modularity
# LANG_ID_FILES=$(ls $ARCHIVE_DIR/*lang_id.tsv.gz)
# # generate start/end date for file name
# START_DATE="Jan-01-16"
# END_DATE="Sep-23-19"
# LANG_ID_COMBINED_FILE="$ARCHIVE_DIR"/"$START_DATE"_"$END_DATE"_lang_id.tsv.gz
# python combine_data_frames.py $LANG_ID_FILES --out_file $LANG_ID_COMBINED_FILE