# extract native verb use for loanword authors
# DATA_DIR=../../data/mined_tweets/loanword_author_tweets/
DATA_DIR=../../data/mined_tweets/loanword_author_tweets_all_archives/
LANG_ID_FILE=lang_id.gz
NATIVE_VERB_DATA=../../data/loanword_resources/native_verb_light_verb_pairs.csv

python3 extract_native_verb_use_for_authors.py $DATA_DIR --lang_id_data $LANG_ID_FILE --native_verb_data $NATIVE_VERB_DATA