# collect authors of loanword posts
ES_CLUSTER_NAME="twitter_posts"
LOANWORD_INTEGRATED_DATA='../../data/loanword_resources/wiktionary_twitter_reddit_loanword_verbs_integrated_verbs_query_phrases.tsv'
LOANWORD_PHRASE_DATA='../../data/loanword_resources/wiktionary_twitter_reddit_loanword_verbs_light_verbs_query_phrases.tsv'
python3 collect_loanword_authors.py --es_cluster_name $ES_CLUSTER_NAME --loanword_integrated_data $LOANWORD_INTEGRATED_DATA --loanword_phrase_data $LOANWORD_PHRASE_DATA