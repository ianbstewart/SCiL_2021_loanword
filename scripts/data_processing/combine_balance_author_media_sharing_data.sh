# combine media sharing data and balance on the likely audience distribution
OUT_DIR=../../data/mined_tweets/
DATA_NAME=loanword_author_tweets_all_archives

python3 combine_balance_author_media_sharing_data.py --out_dir $OUT_DIR --data_name $DATA_NAME