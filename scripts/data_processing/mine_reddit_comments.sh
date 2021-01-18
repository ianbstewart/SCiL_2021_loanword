# mine Reddit comments for subreddit, text, date, etc.
source data_helpers.sh

## collect files
ARCHIVE_DIR=/hg190/corpora/reddit_full_comment_data
START_YEAR=2009
END_YEAR=2017
MONTHS=$(seq $START_YEAR $END_YEAR)
ARCHIVE_FILES=$(COLLECT_MONTHLY_ARCHIVE_FILES $ARCHIVE_DIR $START_YEAR $END_YEAR)
# light verb loanwords
# PHRASE_FILE=../../data/loanword_resources/es_light_verbs_reddit_clean_loanwords_phrases.txt
# integrated loanwords
# PHRASE_FILE=../../data/loanword_resources/reddit_clean_loanwords_integrated_verbs.txt
# valid subreddits
# SUBREDDIT_FILE=../../data/mined_reddit_comments/subreddit_counts/es_subreddits.txt
# valid users (loanword/phrase users)
USER_FILE=../../data/mined_reddit_comments/loanword_users/PHRASES=integrated_verbs_users_combined.txt
# USER_FILE=../../data/mined_reddit_comments/loanword_users/PHRASES=light_verb_phrases_users_combined.txt

## mine subreddits with jq
## parallel processing
## output counts to file
# SUBREDDIT_STR="spain"
# SUBREDDIT_STR="mexico"
# SUBREDDIT_STR="france"
# OUT_DIR=../../output/subreddit="$SUBREDDIT_STR"
# if [ ! -d $OUT_DIR ];
# then
#     mkdir -p $OUT_DIR
# fi
# JOBS=10
# parallel --jobs $JOBS --bar --verbose PROCESS_ARCHIVE_FILE {} $SUBREDDIT_STR $OUT_DIR ::: $(ls $ARCHIVE_FILES)

## mine with python
## parallel processing
# all comments
# OUT_DIR=../../data/mined_reddit_comments/
# loanword user comments
OUT_DIR=../../data/mined_reddit_comments/loanword_users
POST_LANG=es
LANG_ID_DIR=lang_id/
JOBS=20
# all comments
# parallel --jobs $JOBS --bar --verbose python mine_reddit_comments.py {} --out_dir $OUT_DIR --lang $POST_LANG --lang_id_dir $LANG_ID_DIR --phrase_file $PHRASE_FILE --subreddit_file $SUBREDDIT_FILE ::: $(ls $ARCHIVE_FILES)
# loanword user comments
parallel --jobs $JOBS --bar --verbose python mine_reddit_comments.py {} --out_dir $OUT_DIR --user_file $USER_FILE ::: $(ls $ARCHIVE_FILES)

## serial processing => FOR LOSERS
# for ARCHIVE_FILE in $ARCHIVE_FILES;
# do
#     PROCESS_ARCHIVE_FILE $ARCHIVE_FILE $SUBREDDIT_STR $OUT_DIR
# done