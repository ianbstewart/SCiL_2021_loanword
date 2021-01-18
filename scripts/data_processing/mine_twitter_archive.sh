# mine Twitter archive for specified hashtags or locations
source data_helpers.sh

## collect files to mine
START_YEAR=18
END_YEAR=19
# START_YEAR=14
# END_YEAR=15
ARCHIVE_DIR=/hg190/corpora/twitter-crawl/new-archive
# ARCHIVE_DIR=/hg190/corpora/twitter-crawl/daily-tweet-archives
# COLLECT_ARCHIVE_FILES $ARCHIVE_DIR $START_YEAR $END_YEAR
ARCHIVE_FILES=$(COLLECT_DAILY_ARCHIVE_FILES $ARCHIVE_DIR $START_YEAR $END_YEAR)

## set mine parameters
# phrases in file
# EN phrases
# PHRASE_FILE=../../data/loanword_resources/EN_only_phrases.txt
# PHRASE_FILE=../../data/loanword_resources/EN_only_tech_phrases.txt
# ES loanword verb phrases
# PHRASE_FILE=../../data/loanword_resources/es_light_verbs_tweets_loanwords_clean_phrases.txt
# ES integrated loanwords
PHRASE_FILE=../../data/loanword_resources/tweets_loanwords_clean_integrated_verbs.txt
# comma-separated phrases
# PHRASES='""'
# location: lat1 lat2 lon1 lon2
# LOCATION_BOX=(24 26 70 71)
# USER_LOC_PHRASE='""'
# lang
# TWEET_LANG_DETECT='fr'
TWEET_LANG_DETECT='es'
# pre-detected language data
TWEET_LANG_DETECT_DIR=$ARCHIVE_DIR/lang_id
# output directory
# OUT_DIR=../../data/mined_tweets/loanword_tweets/FR_archive/
OUT_DIR=../../data/mined_tweets/loanword_tweets/ES_archive/

## mine
# mine safely in parallel
JOBS=1
parallel --jobs $JOBS --bar --verbose python mine_twitter_archive.py {} --phrase_file $PHRASE_FILE --lang_detect $TWEET_LANG_DETECT --lang_detect_dir $TWEET_LANG_DETECT_DIR --out_dir $OUT_DIR ::: $(ls $ARCHIVE_FILES)
## debugging: see what run actually does
# ls "${ARCHIVE_FILES[@]}" | parallel --dryrun --jobs $JOBS --bar python "mine_twitter_archive.py --archive_files {} --phrases ${PHRASES[@]} --out_dir $OUT_DIR_TWEETS" ::: "${ARCHIVE_FILES[@]}"