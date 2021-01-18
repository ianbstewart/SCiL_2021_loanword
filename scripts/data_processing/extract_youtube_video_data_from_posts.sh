# extract YouTube metadata from URLs
# DATA_DIR=../../data/mined_tweets/loanword_author_tweets/
DATA_DIR=../../data/mined_tweets/loanword_author_tweets_all_archives/
AUTH_DATA=../../data/culture_metadata/google_api_auth.csv
YOUTUBE_VIDEO_DATA=../../data/culture_metadata/youtube_video_data.tsv

python3 extract_youtube_video_data_from_posts.py $DATA_DIR --auth_data $AUTH_DATA --youtube_video_data $YOUTUBE_VIDEO_DATA