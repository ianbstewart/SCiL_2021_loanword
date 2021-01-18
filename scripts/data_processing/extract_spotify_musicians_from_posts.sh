# extract Spotify musicians from URLs shared in posts
# POST_DATA_DIRS=(../../data/mined_tweets/loanword_integrated_verb_author_counts_CLUSTER=twitter_posts_tweets/ ../../data/mined_tweets/loanword_light_verb_author_counts_CLUSTER=twitter_posts_tweets/)
POST_DATA_DIRS=(../../data/mined_tweets/loanword_author_tweets_all_archives/)
MUSIC_API_AUTH_FILE=../../data/culture_metadata/spotify_auth.csv
OUT_DIR=../../data/culture_metadata/

python3 extract_spotify_musicians_from_posts.py "${POST_DATA_DIRS[@]}" --music_API_auth_data $MUSIC_API_AUTH_FILE --out_dir $OUT_DIR