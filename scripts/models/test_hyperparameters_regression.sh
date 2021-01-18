# test hyperparameters for regression
# loanwords
# DATA=../../data/mined_tweets/loanword_verbs_post_social_data.tsv
# DATA_TYPE=loanword
# native verbs
DATA=../../data/mined_tweets/native_verbs_post_social_data.tsv
DATA_TYPE=native_verb
# hyperparameters
L2_WEIGHTS=("0.0" "0.00001" "0.0001" "0.001" "0.01" "0.1" "0.25" "0.5" "0.75" "0.9" "0.95" "0.99" "1.0")
OUT_DIR=../../output/regression_results/
# variables
# SCALAR_VARS=("log_es" "log_integrated_verb_pct" "log_latin_american_media_pct" "log_balanced_latin_american_media_pct" "log_media_URL_pct" "log_post_pct" "log_URL_share_pct" "log_RT_pct") # all log vars
SCALAR_VARS=("log_post_pct" "log_URL_share_pct" "log_RT_pct")
# categorical variables
# CAT_VARS=("description_location_region" "es_bin" "latin_american_media_pct_bin" "balanced_latin_american_media_pct_bin") # all vars
CAT_VARS=("description_location_region" "es_bin" "balanced_latin_american_media_pct_bin")
# CAT_VAR_TREATMENT_VALS=("other" "es_low" "media_low") # all vars
CAT_VAR_TREATMENT_VALS=("UNK" "es_low" "media_low")
# FIXED_EFFECT_VARS=("loanword" "screen_name" "native_word_type") # all fixed effects
FIXED_EFFECT_VARS=("native_word_type" "screen_name")
# INTERACTION_VARS=("es_bin*latin_american_media_pct_bin*categorical" "es_bin*description_location_region*categorical") # all vars

python test_hyperparameters_regression.py $DATA --data_type $DATA_TYPE --L2_weights "${L2_WEIGHTS[@]}" --out_dir $OUT_DIR --scalar_vars "${SCALAR_VARS[@]}" --cat_vars "${CAT_VARS[@]}" --cat_var_treatment_vals "${CAT_VAR_TREATMENT_VALS[@]}" --fixed_effect_vars "${FIXED_EFFECT_VARS[@]}"