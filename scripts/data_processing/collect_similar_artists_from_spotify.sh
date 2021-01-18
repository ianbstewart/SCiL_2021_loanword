# collect artists similar to Wiki artists from Spotify suggestions
AUTH_FILE=../../data/culture_metadata/spotify_auth.csv
# DATA_FILE=../../data/culture_metadata/latin_american_pop_musicians_en_wiki_data.tsv
# DATA_FILE=../../data/culture_metadata/latin_american_pop_musicians_es_wiki_data.tsv
# DATA_FILE=../../data/culture_metadata/us_american_pop_musicians_en_wiki_data.tsv
# DATA_FILE=../../data/culture_metadata/latin_american_musician_subcategory_dbpedia_data.tsv
DATA_FILE=../../data/culture_metadata/us_american_musician_subcategory_dbpedia_data.tsv
OUT_DIR=../../data/culture_metadata/

# collect data
python3 collect_similar_artists_from_spotify.py $DATA_FILE --auth_data $AUTH_FILE --out_dir $OUT_DIR