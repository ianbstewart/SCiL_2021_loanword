# copy data from remote server
# adaptation, arizona
USER_NAME=istewart6
# ephemeral
# USER_NAME=ian
# adaptation
SERVER_NAME=adaptation.cc.gatech.edu
SOURCE_PATH=/hg191/istewart6/loanword_adoption/data/mined_tweets/loanword_author_tweets/*tweets.gz
# arizona
# SERVER_NAME=arizona.cc.gatech.edu
# SOURCE_PATH=/nethome/istewart6/loanword_adoption/data/mined_tweets/loanword_author_tweets/*tweets.gz
# ephemeral
# SERVER_NAME=130.207.124.201
# SOURCE_PATH=/home/ian/loanword_adoption/data/mined_tweets/loanword_author_tweets/*tweets.gz

# target
TARGET_PATH=../../data/mined_tweets/loanword_author_tweets/

# remove set intersect (SOURCE - TARGET), only copy non-intersect files
TARGET_FILES=($(ls -a $TARGET_PATH | xargs -n 1 basename))
SOURCE_FILES=($(ssh $USER_NAME@$SERVER_NAME ls $SOURCE_PATH | xargs -n 1 basename))
# set complement in python...because the bash options use "sort" which doesn't work on file lists?
COMP_FILES=$(python -c 'from argparse import ArgumentParser; parser=ArgumentParser(); parser.add_argument("--data_files1", nargs="+"); parser.add_argument("--data_files2", nargs="+"); args=parser.parse_args(); files1=list(map(lambda x: x.lower(), args.data_files1)); files2=list(map(lambda x: x.lower(), args.data_files2)); files3=set(files1)-set(files2); print("\n".join(files3))' --data_files1 "${SOURCE_FILES[@]}" --data_files2 "${TARGET_FILES[@]}")
SOURCE_DIR=$(dirname $SOURCE_PATH)
function join_by { local IFS="$1"; shift; echo "$*"; }
FULL_SOURCE_STR=$(join_by , $COMP_FILES)
FULL_SOURCE_STR=$SOURCE_DIR/{$FULL_SOURCE_STR}

# copy
scp $USER_NAME@$SERVER_NAME:"$FULL_SOURCE_STR" $TARGET_PATH
# scp $USER_NAME@$SERVER_NAME:$SOURCE_PATH $TARGET_PATH
