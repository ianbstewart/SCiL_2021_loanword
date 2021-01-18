# get raw counts for phrases in Twitter archive
PHRASE="faire un tweet"
# PHRASE="tweeter"
# PHRASE="[Gg]oogler"
# PHRASE="chercher sur [Gg]oogle"
# PHRASE="tuitear"
# PHRASE="hacer un tweet"
# PHRASE="googlear"
# PHRASE="buscar en [Gg]oogle"

LANG="fr"
# LANG="es"

OUT_DIR=../../output
ARCHIVE_DIR=/hg190/corpora/twitter-crawl/new-archive
ARCHIVE_FILES=$ARCHIVE_DIR/tweets-*[0-9].gz

## mine files
# skip first line of file because corruption
PHRASE_OUT_DIR="$OUT_DIR"/phrase=\""$PHRASE"\"_lang="$LANG"_output
if [ ! -d "$PHRASE_OUT_DIR" ];
then
    mkdir "$PHRASE_OUT_DIR"
fi

# requires jq 1.5
for ARCHIVE_FILE in $ARCHIVE_FILES;
do
    OUT_FILE_BASE="${ARCHIVE_FILE/.gz/}"
    OUT_FILE_BASE=$(basename $OUT_FILE_BASE)
    PHRASE_OUT_FILE="$PHRASE_OUT_DIR"/"$OUT_FILE_BASE".json
    echo "$PHRASE_OUT_FILE"
    zcat $ARCHIVE_FILE | tail -n +2 | jq -c '. | select(.lang | try contains("$LANG")) | select(.text | try test("$PHRASE"))' > "$PHRASE_OUT_FILE"
done