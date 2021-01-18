# collect pages from Wikipedia based on category
# Latin American (en) pop musician page categories
# PAGE_CATEGORY_GROUP="latin_american_pop_musicians_en"
# WIKI_LANG="EN"
# PAGE_CATEGORIES=("Argentine_pop_singers" "Brazilian_pop_singers" "Chilean_pop_singers" "Colombian_pop_singers" "Ecuadorian_pop_singers" "Mexican_pop_singers" "Uruguayan_pop_singers" "Latin_pop_singers")
# Latin American (es) pop musician page categories
# PAGE_CATEGORY_GROUP="latin_american_pop_musicians_es"
# WIKI_LANG="ES"
# PAGE_CATEGORIES=("Cantantes_de_pop_de_Argentina" "Cantantes_de_pop_de_Brasil" "Cantantes_de_pop_de_Chile" "Cantantes_de_pop_de_Colombia" "Cantantes_de_pop_de_Cuba" "Cantantes_de_pop_de_Guatemala" "Cantantes_de_pop_de_México" "Cantantes_de_pop_de_Perú" "Cantantes_de_pop_de_la_República_Dominicana" "Cantantes_de_pop_de_Venezuela")
# US American (en) pop musician page categories
PAGE_CATEGORY_GROUP="us_american_pop_musicians_en"
WIKI_LANG="EN"
PAGE_CATEGORIES=("American_male_pop_singers" "American_female_pop_singers")

OUT_DIR="../../data/culture_metadata/"

## collect pages
python3 collect_pages_from_wikipedia_by_category.py "${PAGE_CATEGORIES[@]}" --wiki_lang $WIKI_LANG --out_dir $OUT_DIR --page_category_group $PAGE_CATEGORY_GROUP