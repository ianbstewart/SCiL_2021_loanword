source data_helpers.sh

## twitter data
# START_YEAR=16
# END_YEAR=19
START_YEAR=16
END_YEAR=19
ARCHIVE_DIR=/hg190/corpora/twitter-crawl/new-archive
# # ARCHIVE_DIR=/hg190/corpora/twitter-crawl/daily-tweet-archives/
# # COLLECT_ARCHIVE_FILES $ARCHIVE_DIR $START_YEAR $END_YEAR
ARCHIVE_FILES=$(COLLECT_DAILY_ARCHIVE_FILES $ARCHIVE_DIR $START_YEAR $END_YEAR)
TXT_VAR='text'
ID_VAR='id'
OUT_DIR=../../data/mined_tweets/freq_data

## reddit data
# START_YEAR=2008
# END_YEAR=2019
# ARCHIVE_DIR=/hg190/corpora/reddit_full_comment_data
# ARCHIVE_FILES=$(COLLECT_MONTHLY_ARCHIVE_FILES $ARCHIVE_DIR $START_YEAR $END_YEAR)
# custom list
# ARCHIVE_FILES=($ARCHIVE_DIR/2013/RC_2013-12.bz2 $ARCHIVE_DIR/2014/RC_2014-12.bz2)
# TXT_VAR='body'
# ID_VAR='id'
# OUT_DIR=../../data/mined_reddit_comments/freq_data

# POST_LANG='fr'
# POST_LANG='es'
POST_LANG='en'
MIN_DF=100
JOBS=20
parallel --jobs $JOBS --bar --verbose python generate_word_counts_from_data.py {} --lang $POST_LANG --out_dir $OUT_DIR --min_df $MIN_DF --txt_var $TXT_VAR --id_var $ID_VAR ::: $(ls $ARCHIVE_FILES)
# custom list
# parallel --jobs $JOBS --bar --verbose python generate_word_counts_from_data.py {} --lang $POST_LANG --out_dir $OUT_DIR --min_df $MIN_DF --txt_var $TXT_VAR --id_var $ID_VAR ::: "${ARCHIVE_FILES[@]}"