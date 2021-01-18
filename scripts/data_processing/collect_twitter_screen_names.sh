# collect screen names from Twitter
# wiki artists
# NAME_FILE=../../data/culture_metadata/us_american_pop_musicians_en_wiki_data.tsv
# NAME_FILE=../../data/culture_metadata/latin_american_pop_musicians_en_wiki_data.tsv
# NAME_FILE=../../data/culture_metadata/latin_american_pop_musicians_es_wiki_data.tsv
# DBPedia artists
# NAME_FILE=../../data/culture_metadata/us_american_musician_subcategory_dbpedia_data.tsv
NAME_FILE=../../data/culture_metadata/latin_american_musician_subcategory_dbpedia_data.tsv
# similar artists
# NAME_FILE=../../data/culture_metadata/latin_american_pop_musicians_en_wiki_data_similar_artists.tsv
# NAME_FILE=../../data/culture_metadata/latin_american_pop_musicians_es_wiki_data_similar_artists.tsv
# NAME_FILE=../../data/culture_metadata/us_american_pop_musicians_en_wiki_data_similar_artists.tsv

AUTH_FILE=../../data/mined_tweets/twitter_auth.csv
OUT_DIR=../../data/culture_metadata/

python3 collect_twitter_screen_names.py $NAME_FILE --auth_file $AUTH_FILE --out_dir $OUT_DIR