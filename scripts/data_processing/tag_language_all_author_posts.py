"""
Tag language ID and score for all prior posts from authors.
"""
from argparse import ArgumentParser
import logging
import os
from langid.langid import LanguageIdentifier, model
from data_helpers import load_data_from_dirs, clean_tweet_txt, clean_txt_simple
from pandarallel import pandarallel
import pandas as pd
import re
# import resource

# restrict core use

def tag_lang(data, txt_var='text_clean'):
    """
    Tag language in all text in data, 
    return language, score and post IDs.
    
    :param data: data frame
    :param txt_var: text var
    :returns lang_id_data:: lang, score and post ID
    """
    lang_id_model = LanguageIdentifier.from_modelstring(model, norm_probs=True)
    # parallel
    MAX_JOBS=5
    pandarallel.initialize(nb_workers=MAX_JOBS)
    lang_score_vals = data.loc[:, txt_var].parallel_apply(lang_id_model.classify)
    # serial
    # TODO: why does langid wreck CPU use?
#     lang_score_vals = data.loc[:, txt_var].apply(lang_id_model.classify)
    # separate lang/score
    lang_val, lang_score = zip(*lang_score_vals)
    lang_var = 'lang'
    lang_score_var = 'lang_score'
    post_id_var = 'id'
    data = data.assign(**{
        lang_var : lang_val,
        lang_score_var : lang_score,
    })
    lang_id_data = data.loc[:, [lang_var, lang_score_var, post_id_var]]
    return lang_id_data

def main():
    parser = ArgumentParser()
    parser.add_argument('data_dir') # ../../data/mined_tweets/loanword_integrated_verb_author_counts_CLUSTER\=twitter_posts_tweets/
    parser.add_argument('--data_type', default='twitter')
    args = vars(parser.parse_args())
    logging_file = '../../output/tag_language_all_author_posts.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    ## load data
    data_dir = args['data_dir']
    file_matcher = re.compile('.*tweets.gz')
    combined_data = load_data_from_dirs([data_dir], file_matcher=file_matcher)
    combined_data.fillna('', inplace=True)
    # tmp debugging
#     combined_data = combined_data.iloc[:2000, :]
    # clean text
    # e.g. remove @-mention, #hashtags
    data_type = args['data_type']
    if(data_type == 'twitter'):
        txt_clean_method = clean_tweet_txt
    else:
        txt_clean_method = clean_txt_simple
    txt_var = 'text'
    clean_txt_var = f'{txt_var}_clean'
    combined_data = combined_data[combined_data.loc[:, txt_var] != '']
    combined_data = combined_data.assign(**{
        clean_txt_var : combined_data.loc[:, txt_var].apply(txt_clean_method)
    })
    
    ## label lang ID
    # remove existing data
    # post ID | lang ID | lang score
    lang_id_data_file = os.path.join(data_dir, 'lang_id.gz')
    post_id_var = 'id'
    if(os.path.exists(lang_id_data_file)):
        old_lang_id_data = pd.read_csv(lang_id_data_file, sep='\t', index_col=False, compression='gzip')
        old_lang_id_post_ids = set(old_lang_id_data.loc[:, post_id_var].values)
        combined_data = combined_data[~combined_data.loc[:, 'id'].isin(old_lang_id_post_ids)]
    else:
        old_lang_id_data = []
    lang_id_data = tag_lang(combined_data, txt_var=clean_txt_var)
    
    ## write to file
    if(len(old_lang_id_data) > 0):
        lang_id_data = pd.concat([old_lang_id_data, lang_id_data], axis=0)
    lang_id_data.to_csv(lang_id_data_file, sep='\t', index=False, compression='gzip')
    
if __name__ == '__main__':
    main()