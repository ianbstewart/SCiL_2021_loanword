# extract YouTube video genre from posts based on assumed artist name
# NOTE: assume that we have already collected all relevant YouTube video data in extract_youtube_video_data_from_posts.sh
# DATA_DIR=../../data/mined_tweets/loanword_author_tweets/
DATA_DIR=../../data/mined_tweets/loanword_author_tweets_all_archives/
YOUTUBE_DATA=../../data/culture_metadata/youtube_video_data.tsv
LATIN_AMERICAN_ARTIST_DATA=(../../data/culture_metadata/latin_american_musician_subcategory_dbpedia_data.tsv ../../data/culture_metadata/latin_american_pop_musicians_en_wiki_data.tsv ../../data/culture_metadata/latin_american_pop_musicians_es_wiki_data.tsv)
US_AMERICAN_ARTIST_DATA=(../../data/culture_metadata/us_american_musician_subcategory_dbpedia_data.tsv ../../data/culture_metadata/us_american_pop_musicians_en_wiki_data.tsv)
OUT_DIR=../../data/mined_tweets/

python3 extract_youtube_video_genre_from_posts.py $DATA_DIR --youtube_data $YOUTUBE_DATA --latin_american_artist_data "${LATIN_AMERICAN_ARTIST_DATA[@]}" --us_american_artist_data "${US_AMERICAN_ARTIST_DATA[@]}" --out_dir $OUT_DIR