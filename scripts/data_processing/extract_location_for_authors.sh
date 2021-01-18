# extract location from author data
# loanword data
AUTHOR_DATA=../../data/mined_tweets/loanword_author_descriptive_data.tsv
OUT_DIR=../../data/mined_tweets/
# non loanword data
# AUTHOR_DATA=../../data/mined_tweets/non_loanword_author_tweets/non_loanword_author_descriptive_data.tsv
# OUT_DIR=../../data/mined_tweets/non_loanword_author_tweets/
UNAMBIG_CITY_DATA=../../data/control_var_data/unambig_city_data.tsv
GEO_DATA=/hg190/corpora/GeoNames/allCountriesSimplified.tsv
python3 extract_location_for_authors.py $AUTHOR_DATA  --unambig_city_data $UNAMBIG_CITY_DATA --geo_data $GEO_DATA --out_dir $OUT_DIR