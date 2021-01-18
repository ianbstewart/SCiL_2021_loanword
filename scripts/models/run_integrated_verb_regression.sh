# run regression to predict light verbs
# loanwords
# DATA=../../data/mined_tweets/loanword_verbs_post_social_data.tsv
# DATA_TYPE=loanword
# native verbs
DATA=../../data/mined_tweets/native_verbs_post_social_data.tsv
DATA_TYPE=native_verb
# scalar variables
# SCALAR_VARS=("es" "integrated_verb_pct" "latin_american_media_pct" "post_pct" "URL_share_pct" "RT_pct" "rand") # all vars
# SCALAR_VARS=("log_es" "log_integrated_verb_pct" "log_latin_american_media_pct" "log_balanced_latin_american_media_pct" "log_media_URL_pct" "log_post_pct" "log_URL_share_pct" "log_RT_pct") # all log vars
SCALAR_VARS=("log_post_pct" "log_URL_share_pct" "log_RT_pct")
# categorical variables
# CAT_VARS=("description_location_region" "es_bin" "latin_american_media_pct_bin" "balanced_latin_american_media_pct_bin" "latin_american_media_count_bin" "us_american_media_count_bin" "other_media_count_bin" "media_count_bin" "has_hashtag" "has_mention" "max_hashtag_freq" "max_mention_freq") # all vars
CAT_VARS=("has_hashtag" "has_mention")
# CAT_VAR_TREATMENT_VALS=("UNK" "es_low" "media_low" "no_media" "0") # all vars
CAT_VAR_TREATMENT_VALS=("-1" "-1")
# FIXED_EFFECT_VARS=("loanword" "native_word_type" "screen_name") # all fixed effects
FIXED_EFFECT_VARS=("native_word_type" "screen_name")
# INTERACTION_VARS=("es_bin*latin_american_media_pct_bin*categorical" "es_bin*description_location_region*categorical" "us_american_media_count_bin*latin_american_media_count_bin*categorical") # all vars
INTERACTION_VARS=("")
# DATA_FILTER_VARS=("es_bin")
# DATA_FILTER_VAR_RANGES=("es_high")
DATA_FILTER_VARS=("")
DATA_FILTER_VAR_RANGES=("") # format = VAL_1:VAL_2
# filtered loanwords: access,aim,alert,audit,ban,bang,bash,block,boycott,box,bully,bust,cast,change,chat,check,shoot,combat,connect,crack,customize,default,delete,dope,downvote,draft,drain,smash,sniff,standard,exit,export,externalize,fangirl,film,flash,flex,flirt,focus,format,form,freak,freeze,fund,gentrify,ghost,google,hack,hail,hang,harm,hypnosis,host,hype,intercept,hang,lag,like,limit,lynch,link,love,look,make,melt,mope,nag,knock,pack,pan,panic,park,perform,pitch,pin,punch,post,posterize,print,protest,push,pump,quote,rank,rant,rape,record,render,rent,report,reset,ring,rock,roll,sample,selfie,sext,ship,shitpost,shock,sign-in,stalk,strike,surf,tackle,text,tick,torment,touch,transport,travel,troll,tweet,twerk,upvote,vape,zap
# MODEL_TYPE='logistic_regression'
MODEL_TYPE='random_forest'
# L2_WEIGHT=0.1 # default weight
# L2_WEIGHT=0.00001 # optimal L2 weight: loanwords; all vars ../../output/regression_results/DATA=loanword_PRED=has_light_verb~log_integrated_verb_pct+log_post_pct+log_URL_share_pct+log_RT_pct+C(description_location_region)+C(es_bin)_hyperparameter_likelihoods.tsv
# L2_WEIGHT=0.99 # optimal L2 weight: loanwords; all vars + media var ../../output/regression_results
# L2_WEIGHT=0.001 # optimal L2 weight (local minimum): loanwords; all vars + media var ../../output/regression_results/DATA=loanword_PRED=has_light_verb~log_integrated_verb_pct+log_post_pct+log_URL_share_pct+log_RT_pct+C(description_location_region)+C(es_bin)+C(balanced_latin_american_media_pct_bin)_hyperparameter_likelihoods.tsv
# L2_WEIGHT=0.00001 # optimal L2 weight: native words; all vars ../../output/regression_results/DATA=native_verb_PRED=has_light_verb~log_post_pct+log_URL_share_pct+log_RT_pct+C(description_location_region)+C(es_bin)_hyperparameter_likelihoods.tsv
L2_WEIGHT=0.0001 # optimal L2 weight: native words; all vars + media var ../../output/regression_results/DATA=native_verb_PRED=has_light_verb~log_post_pct+log_URL_share_pct+log_RT_pct+C(description_location_region)+C(es_bin)+C(balanced_latin_american_media_pct_bin)_hyperparameter_likelihoods.tsv
OUT_DIR=../../output/regression_results_new/
if [ ! -d $OUT_DIR ]; then
    mkdir $OUT_DIR
fi
# if regression has too many variables, need a shorter name
REGRESSION_NAME="DEFAULT_REGRESSION"
# REGRESSION_NAME="LOANWORD_COUNT_MEDIA_VARS"
# REGRESSION_NAME="LOANWORD_COUNT_MEDIA_VARS_MEDIA_INTERACTION"
# REGRESSION_NAME="NATIVE_VERB_COUNT_MEDIA_VARS"
# REGRESSION_NAME="NATIVE_VERB_COUNT_MEDIA_VARS_MEDIA_INTERACTION"
# REGRESSION_NAME="CLEAN_LOANWORDS"

python run_integrated_verb_regression.py $DATA --scalar_vars "${SCALAR_VARS[@]}" --cat_vars "${CAT_VARS[@]}" --cat_var_treatment_vals "${CAT_VAR_TREATMENT_VALS[@]}" --fixed_effect_vars "${FIXED_EFFECT_VARS[@]}" --interaction_vars "${INTERACTION_VARS[@]}" --data_filter_vars "${DATA_FILTER_VARS[@]}" --data_filter_var_ranges "${DATA_FILTER_VAR_RANGES[@]}" --data_type $DATA_TYPE --out_dir $OUT_DIR --model_type $MODEL_TYPE --l2_weight $L2_WEIGHT --regression_name $REGRESSION_NAME