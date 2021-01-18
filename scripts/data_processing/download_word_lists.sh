# download word lists for languages
# easy first step in loanword detection

COLLECT_DICT_DATA () {
    DATA_DIR=$1
    DICT_URL=$2
    LANG_SHORT=$3
    FILE_BASE=${DICT_URL##*/}
    ZIP_OUT_FILE=$DATA_DIR/$FILE_BASE
    TXT_OUT_FILE=${FILE_BASE/.zip/.txt}
    wget $DICT_URL -O $ZIP_OUT_FILE
    unzip $ZIP_OUT_FILE
    mv $TXT_OUT_FILE "$DATA_DIR"/"$LANG_SHORT"_words.txt
    rm $ZIP_OUT_FILE
}

DATA_DIR=../../data/loanword_resources
# French
LANG_URL=http://gwicks.net/textlists/francais.zip
LANG_SHORT=FR
COLLECT_DICT_DATA $DATA_DIR $LANG_URL $LANG_SHORT
# add stopwords
FR_STOPS=https://raw.githubusercontent.com/stopwords-iso/stopwords-fr/master/stopwords-fr.txt
wget $FR_STOPS -O $DATA_DIR/FR_stops.txt
cat $DATA_DIR/FR_stops.txt >> "$DATA_DIR"/"$LANG_SHORT"_words.txt

# Spanish
LANG_URL=http://gwicks.net/textlists/espanol.zip
LANG_SHORT=ES
COLLECT_DICT_DATA $DATA_DIR $LANG_URL $LANG_SHORT
# add stopwords
ES_STOPS=https://raw.githubusercontent.com/stopwords-iso/stopwords-es/master/stopwords-es.txt
wget $ES_STOPS -O $DATA_DIR/ES_stops.txt
cat $DATA_DIR/ES_stops.txt >> "$DATA_DIR"/"$LANG_SHORT"_words.txt

# English
LANG_URL=http://gwicks.net/textlists/english3.zip
LANG_SHORT=EN
COLLECT_DICT_DATA $DATA_DIR $LANG_URL $LANG_SHORT