# get sample of posts that contain loanword verbs for annotation
DATA_DIR="../../data/mined_tweets/loanword_tweets/ES_archive"
DATA_FILES="$DATA_DIR"/archive_PHRASES=tweets_loanwords_clean_integrated_verbs_DATERANGE=*_LANGDETECT=es.gz
LOANWORD_INTEGRATED_VERB_FILE="../../data/loanword_resources/tweets_loanwords_clean_integrated_verbs.tsv"

python get_loanword_verb_sample_for_annotation.py $DATA_FILES --loanword_integrated_verb_file $LOANWORD_INTEGRATED_VERB_FILE