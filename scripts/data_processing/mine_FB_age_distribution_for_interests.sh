# mine FB age distribution for (music) interests
# INTEREST_DATA=../../data/culture_metadata/spotify_musician_data.tsv
INTEREST_DATA=../../data/culture_metadata/facebook_interest_data.tsv
# TODO: same thing but for YouTube artists?? ugh!!
# INTEREST_DATA=../../data/culture_metadata/youtube_video_music_genre_data.tsv
AUTH_DATA=../../data/culture_metadata/facebook_auth_multi.csv

python3 mine_FB_age_distribution_for_interests.py $INTEREST_DATA --auth_data $AUTH_DATA