# download pushshift data
# collect file names
XZ_YEAR_RANGE=(2018)
XZ_MONTH_RANGE=$(seq -f "%02g" 1 10)
XZ_FILES=()
for XZ_YEAR in "$XZ_YEAR_RANGE";
do
    for XZ_MONTH in $XZ_MONTH_RANGE;
    do
	XZ_FILES+=("RS_"$"$XZ_YEAR"-$"$XZ_MONTH".xz)
    done
done
#echo "${XZ_FILES[@]}"
ZST_YEAR_RANGE=(2018 2019)
ZST_YEAR_START_MONTH=([2018]=11 [2019]=1)
ZST_YEAR_END_MONTH=([2018]=12 [2019]=6)
ZST_FILES=()
for ZST_YEAR in "${ZST_YEAR_RANGE[@]}";
do
    ZST_START_MONTH="${ZST_YEAR_START_MONTH[$ZST_YEAR]}"
    ZST_END_MONTH="${ZST_YEAR_END_MONTH[$ZST_YEAR]}"
    ZST_MONTH_RANGE=$(seq -f "%02g" $ZST_START_MONTH $ZST_END_MONTH)
    for ZST_MONTH in $ZST_MONTH_RANGE;
    do
	ZST_FILES+=("RS_"$"$ZST_YEAR"-$"$ZST_MONTH".zst)
    done
done
#echo "${ZST_FILES[@]}"
ALL_FILES=()
ALL_FILES+=("${XZ_FILES[@]}")
ALL_FILES+=("${ZST_FILES[@]}")
echo "${ALL_FILES[@]}"
# download data
SITE_URL=https://files.pushshift.io/reddit/submissions/
for FILE_NAME in "${ALL_FILES[@]}";
do
    echo $FILE_NAME
    wget -O $FILE_NAME "$SITE_URL"$"$FILE_NAME"
done