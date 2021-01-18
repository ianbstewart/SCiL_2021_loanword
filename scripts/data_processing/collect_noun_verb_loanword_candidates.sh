# collect NOUN/VERB loanword candidates from POS tag percents
# twitter
# TAG_PCT_FILE=../../data/mined_tweets/POS_tag_stats/tweets_POS_tags_LANG\=en.npz
# WORD_COUNT_FILE=../../data/mined_tweets/freq_data/tweets_combined_word_count_LANG=es.npz
# reddit
TAG_PCT_FILE=../../data/mined_reddit_comments/POS_tag_stats/reddit_2013_2016_tag_pcts.tsv
WORD_COUNT_FILE=../../data/mined_reddit_comments/freq_data/reddit_combined_LANG=es_word_counts.npz

# L1 standard word list for filtering false positives ("fin")
STANDARD_LANG_WORD_FILE=../../data/loanword_resources/ES_words.txt

# noun/verb cutoff percents
NOUN_PCT_CUTOFF=0.25
VERB_PCT_CUTOFF=0.1

## collect candidates
python collect_noun_verb_loanword_candidates.py $TAG_PCT_FILE --noun_pct_cutoff $NOUN_PCT_CUTOFF --verb_pct_cutoff $VERB_PCT_CUTOFF --word_count_file $WORD_COUNT_FILE --standard_lang_word_file $STANDARD_LANG_WORD_FILE