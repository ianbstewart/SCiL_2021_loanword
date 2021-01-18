LOANWORD_DATA=../../data/loanword_resources/wiktionary_twitter_reddit_loanword_integrated_verbs_light_verbs.tsv
NATIVE_VERB_DATA=../../data/loanword_resources/native_verb_light_verb_pairs.csv
OUT_DIR=../../data/google_ngram_data/

python extract_counts_from_google_ngrams.py $LOANWORD_DATA $NATIVE_VERB_DATA --out_dir $OUT_DIR