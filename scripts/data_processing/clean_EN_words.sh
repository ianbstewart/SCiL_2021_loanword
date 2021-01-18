# remove all non-EN words from EN vocabulary
DATA_DIR=../../data/loanword_resources
EN_WORD_FILE=$DATA_DIR/EN_words.txt

# combine non-EN words
NON_EN_WORD_FILES=($DATA_DIR/FR_words.txt $DATA_DIR/ES_words.txt)
NON_EN_WORD_OUT_FILE=$DATA_DIR/NON_EN_words.txt
cat "${NON_EN_WORD_FILES[@]}" | sort | uniq > $NON_EN_WORD_OUT_FILE

# remove non-EN words (and de-accented words!) from EN words; write to file
EN_ONLY_WORD_FILE=$DATA_DIR/EN_only_phrases.txt
export EN_WORD_FILE
export NON_EN_WORD_OUT_FILE
export EN_ONLY_WORD_FILE
python -c 'import os; from unidecode import unidecode; en_words = set([l.strip() for l in open(os.environ["EN_WORD_FILE"], "r")]); non_en_words = set([l.strip() for l in open(os.environ["NON_EN_WORD_OUT_FILE"], encoding="ISO-8859-1")]); non_en_words = non_en_words | set([unidecode(word) for word in list(non_en_words)]); en_only_words = en_words - non_en_words; print("%d/%d"%(len(en_only_words), len(en_words))); open(os.environ["EN_ONLY_WORD_FILE"], "w").write("\n".join(sorted(en_only_words)));'