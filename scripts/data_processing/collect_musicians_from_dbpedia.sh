# collect US and Latin American musicians from DBPedia
US_AMERICAN_MUSICIAN_SEED_CATEGORY='American_singers_by_genre'
LATIN_AMERICAN_MUSICIAN_SEED_CATEGORY='Singers_by_nationality'
OUT_DIR='../../data/culture_metadata/'
python3 collect_musicians_from_dbpedia.py --us_american_musician_seed_category $US_AMERICAN_MUSICIAN_SEED_CATEGORY --latin_american_musician_seed_category $LATIN_AMERICAN_MUSICIAN_SEED_CATEGORY --out_dir $OUT_DIR