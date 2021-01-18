# combine author social data 
AUTHOR_LOCATION_DATA=../../data/mined_tweets/loanword_author_descriptive_data_location_data.tsv
AUTHOR_LANG_DATA=../../data/mined_tweets/loanword_author_tweets_all_archives/loanword_author_tweets_all_archives_LANG=es_pct.tsv
AUTHOR_MEDIA_DATA=../../data/mined_tweets/loanword_author_tweets_author_media_sharing.tsv
BALANCED_AUTHOR_MEDIA_DATA=../../data/mined_tweets/loanword_author_tweets_all_archives_author_media_sharing_balanced.tsv
AUTHOR_NATIVE_VERB_DATA=../../data/mined_tweets/loanword_author_tweets_all_archives/native_integrated_light_verbs_per_author.tsv
AUTHOR_ACTIVITY_DATA=../../data/mined_tweets/loanword_author_activity_data.tsv
OUT_DIR=../../data/mined_tweets/
FILE_NAME=loanword_authors_combined

python3 combine_author_social_data.py $AUTHOR_LOCATION_DATA $AUTHOR_LANG_DATA $AUTHOR_MEDIA_DATA $AUTHOR_NATIVE_VERB_DATA $AUTHOR_ACTIVITY_DATA --balanced_media_data  $BALANCED_AUTHOR_MEDIA_DATA --out_dir $OUT_DIR --file_name $FILE_NAME