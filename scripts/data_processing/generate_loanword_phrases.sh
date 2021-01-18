# generate loanword light verb phrases (all conjugations!!)
# ex. "hacer un tweet"
# twitter
# LOANWORD_FILE=../../data/mined_tweets/POS_tag_stats/tweets_loanwords_clean.txt
# reddit
# LOANWORD_FILE=../../data/mined_reddit_comments/POS_tag_stats/reddit_clean_loanwords.txt
# wiktionary
# LOANWORD_FILE=../../data/loanword_resources/es_wiktionary_loanword_nouns_clean.txt
# light verbs
# LIGHT_VERB_FILE=../../data/loanword_resources/es_light_verbs.txt
# combined light verb
LIGHT_VERB_LOANWORD_FILE=../../data/loanword_resources/wiktionary_twitter_reddit_loanword_verbs_light_verbs.tsv
# combined integrated verbs
INTEGRATED_LOANWORD_FILE=../../data/loanword_resources/wiktionary_twitter_reddit_loanword_verbs_integrated_verbs.tsv

# generate light verb phrases
python3 generate_loanword_phrases.py $LIGHT_VERB_LOANWORD_FILE

# also generate the integrated loanwords
python3 generate_loanword_phrases.py $INTEGRATED_LOANWORD_FILE
# python3 generate_loanword_phrases.py $LOANWORD_FILE