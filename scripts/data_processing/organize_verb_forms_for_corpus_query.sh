# organize loanword/native verb data for corpus query
LOANWORD_POST_DATA=../../data/mined_tweets/loanword_verb_posts_CLUSTER=twitter_posts_STARTDATE=2017_7_9_ENDDATE=2019_4_6.tsv
NATIVE_VERB_POST_DATA=../../data/mined_tweets/native_integrated_light_verbs_per_post.tsv
LOANWORD_DATA=../../data/loanword_resources/wiktionary_twitter_reddit_loanword_integrated_verbs_light_verbs.tsv
NATIVE_VERB_DATA=../../data/loanword_resources/native_verb_light_verb_pairs.csv
TOP_K=50
OUT_DIR=../../data/loanword_resources

python organize_verb_forms_for_corpus_query.py $LOANWORD_POST_DATA $NATIVE_VERB_POST_DATA --loanword_data $LOANWORD_DATA --native_verb_data $NATIVE_VERB_DATA --top_k $TOP_K --out_dir $OUT_DIR