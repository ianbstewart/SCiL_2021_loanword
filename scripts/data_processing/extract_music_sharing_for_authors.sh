# extract music sharing (e.g. Spotify) for authors
# loanword authors
# DATA_DIRS=(../../data/mined_tweets/loanword_author_tweets_all_archives/)
# OUT_DIR=../../data/mined_tweets/
# non-loanword authors
DATA_DIRS=(../../data/mined_tweets/non_loanword_author_tweets/)
OUT_DIR=../../data/mined_tweets/

python3 extract_music_sharing_for_authors.py "${DATA_DIRS[@]}" --out_dir $OUT_DIR