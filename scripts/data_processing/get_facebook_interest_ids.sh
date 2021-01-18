# get interest IDs from Facebook
# INTEREST_DATA=../../data/culture_metadata/spotify_musician_data.tsv
INTEREST_DATA=../../data/culture_metadata/youtube_video_music_genre_data.tsv
AUTH_DATA=../../data/culture_metadata/facebook_auth_multi.csv
OUT_DIR=../../data/culture_metadata/

python3 get_facebook_interest_ids.py $INTEREST_DATA --auth_data $AUTH_DATA --out_dir $OUT_DIR