"""
Extract authors' use of native integrated/light verbs.
"""
from argparse import ArgumentParser
import logging
import os
from data_helpers import conjugate_verb, BasicTokenizer, load_data_from_dirs
import numpy as np
import pandas as pd
import re
from pandarallel import pandarallel

def conjugate_filter_verb(verb):
    # remove false positives, e.g. "preguntas" "paseo"
    banned_verb_forms = [verb.replace('ar', 'a'), verb.replace('ar', 'as'), verb.replace('ar', 'o')]
    verb_conjugations = conjugate_verb(verb)
    verb_conjugations = list(filter(lambda x: x not in banned_verb_forms, verb_conjugations))
    return verb_conjugations
def generate_start_mid_end(txt):
    return f'^{txt} | {txt} | {txt}$'
def extract_matches(txt, match_phrases, matchers):
    matches = []
    for match_phrase, matcher in zip(match_phrases, matchers):
        if(matcher.search(txt) is not None):
            matches.append(match_phrase)
    return matches

def main():
    parser = ArgumentParser()
    parser.add_argument('data_dir')
    parser.add_argument('--lang_id_data', default='lang_id_data.gz')
    parser.add_argument('--native_verb_data', default='../../data/loanword_resources/native_verb_light_verb_pairs.csv')
    args = vars(parser.parse_args())
    logging_file = '../../output/extract_native_verb_use_for_authors.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    ## load prior data
    ## i.e. all data from loanword authors
    data_dir = args['data_dir']
    file_matcher = re.compile('.+_tweets.gz')
    loanword_data = load_data_from_dirs([data_dir], file_matcher=file_matcher)
    logging.info('extracted %d data'%(loanword_data.shape[0]))
    # dump existing lang data => noisy?
    if('lang' in loanword_data.columns):
        loanword_data.drop(['lang', 'lang_score'], axis=1, inplace=True)
    # fix post ID type => speeds up merge with lang IDs?
    id_var = 'id'
    loanword_data = loanword_data.assign(**{
        id_var : loanword_data.loc[:, id_var].astype(np.int64)
    })
    # add lang ID
    lang_id_file = os.path.join(data_dir, args['lang_id_data'])
    lang_id_data = pd.read_csv(lang_id_file, sep='\t', compression='gzip')
#     id_var = 'id'
#     loanword_data.index = loanword_data.loc[:, id_var]
#     lang_id_data.index = lang_id_data.loc[:, id_var]
    # slow merge!! how to speed up?
    loanword_data = pd.merge(loanword_data, lang_id_data, on='id', how='left')
    loanword_data.fillna('', inplace=True)
    # cleanup
    loanword_data.drop_duplicates('id', inplace=True)
    logging.info('merged %d post/lang data'%(loanword_data.shape[0]))
    loanword_data = loanword_data[loanword_data.loc[:, 'lang_score'] != '']
    # only keep original content
    non_original_tweet_matcher = re.compile('^\s?RT @\w+')
    txt_var = 'text'
    loanword_data = loanword_data[loanword_data.loc[:, txt_var].apply(lambda x: non_original_tweet_matcher.search(x) is None)]
    # get ES posts
    valid_lang = 'es'
    score_cutoff = 0.9
    valid_lang_loanword_data = loanword_data[(loanword_data.loc[:, 'lang'] == valid_lang) &
                                             (loanword_data.loc[:, 'lang_score'] >= score_cutoff)]
    logging.info('%d valid data'%(valid_lang_loanword_data.shape[0]))
    # clean text
    lang_long = 'spanish'
    tokenizer = BasicTokenizer(lang=lang_long)
    valid_lang_loanword_data = valid_lang_loanword_data.assign(**{
        'text_clean' : valid_lang_loanword_data.loc[:, 'text'].apply(lambda x: ' '.join(tokenizer.tokenize(x.lower())))
    })
    
    ## load native verb matcher
    native_verb_data_file = args['native_verb_data']
    native_verb_data = pd.read_csv(native_verb_data_file, sep=',')
    ## conjugate 
    # extract light verbs from phrase
    native_verb_data = native_verb_data.assign(**{
        'light_verb_VB' : native_verb_data.loc[:, 'light_verb'].apply(lambda x: x.split(' ')[0]),
        'light_verb_NP' : native_verb_data.loc[:, 'light_verb'].apply(lambda x: ' '.join(x.split(' ')[1:])),
    })
    # remove clitics for easier conjugation
    clitic_matcher = re.compile('se$')
    native_verb_data = native_verb_data.assign(**{
        'integrated_verb' : native_verb_data.loc[:, 'integrated_verb'].apply(lambda x: clitic_matcher.sub('', x)),
        'light_verb_VB' : native_verb_data.loc[:, 'light_verb_VB'].apply(lambda x: clitic_matcher.sub('', x)),
    })
    native_verb_data = native_verb_data.assign(**{
        'integrated_verb_matcher' : native_verb_data.loc[:, 'integrated_verb'].apply(lambda x: re.compile(generate_start_mid_end('(%s)'%('|'.join(conjugate_filter_verb(x)))))),
        'light_verb_matcher' : native_verb_data.apply(lambda x: re.compile(generate_start_mid_end('(%s) %s'%('|'.join(conjugate_verb(x.loc['light_verb_VB'])), x.loc['light_verb_NP']))), axis=1),
    })
    
    ## get native verbs
    # match in parallel for *SPEED*
    MAX_JOBS=10
    pandarallel.initialize(nb_workers=MAX_JOBS)
    integrated_verb_phrases = native_verb_data.loc[:, 'integrated_verb'].values
    integrated_verb_matchers = native_verb_data.loc[:, 'integrated_verb_matcher'].values
    clean_txt_var = 'text_clean'
    # integrated verbs
    logging.info('about to extract integrated verbs')
    valid_lang_loanword_data = valid_lang_loanword_data.assign(**{
        'native_integrated_verb' : valid_lang_loanword_data.loc[:, clean_txt_var].parallel_apply(lambda x: extract_matches(x, integrated_verb_phrases, integrated_verb_matchers))
    })
    logging.info('extracted all integrated verbs')
    # light verbs
    light_verb_phrases = native_verb_data.loc[:, 'light_verb'].values
    light_verb_matchers = native_verb_data.loc[:, 'light_verb_matcher'].values
    clean_txt_var = 'text_clean'
    logging.info('about to extract light verbs')
    valid_lang_loanword_data = valid_lang_loanword_data.assign(**{
        'native_light_verb' : valid_lang_loanword_data.loc[:, clean_txt_var].parallel_apply(lambda x: extract_matches(x, light_verb_phrases, light_verb_matchers))
    })
    logging.info('extracted all light verbs')
    # clean data
    # light verb supercedes integrated verb
    # e.g. "pedir disculpas" matches "diculpar" => only keep "pedir disculpas"
    native_light_integrated_verb_lookup = dict(native_verb_data.loc[:, ['light_verb', 'integrated_verb']].values)
    clean_valid_lang_loanword_data = []
    for i, data_i in valid_lang_loanword_data.iterrows():
        integrated_verbs_i = data_i.loc['native_integrated_verb']
        light_verbs_i = data_i.loc['native_light_verb']
        for light_verb_j in light_verbs_i:
            integrated_verb_j = native_light_integrated_verb_lookup[light_verb_j]
            if(integrated_verb_j in integrated_verbs_i):
                integrated_verbs_i.remove(integrated_verb_j)
        data_i.loc['native_integrated_verb'] = list(integrated_verbs_i)
        clean_valid_lang_loanword_data.append(data_i)
    clean_valid_lang_loanword_data = pd.concat(clean_valid_lang_loanword_data, axis=1).transpose()
    # only keep data with at least one native verb
    clean_valid_lang_loanword_data = clean_valid_lang_loanword_data[(clean_valid_lang_loanword_data.loc[:, 'native_light_verb'].apply(lambda x: len(x) > 0)) | (clean_valid_lang_loanword_data.loc[:, 'native_integrated_verb'].apply(lambda x: len(x) > 0))]
    
    # tmp debugging
#     clean_valid_lang_loanword_data.to_csv('native_verb_use_tmp.tsv', sep='\t')
    
    ## organize data
    # per-post
    # author | text | post ID | date | native verb type | native verb category (integrated vs. light)
    post_data_vars = ['screen_name', 'id', 'date', 'created_at', 'text']
    native_verb_vars = ['native_integrated_verb', 'native_light_verb']
    # TODO: parallel 
#     def add_word_info(data, word_vars)
    # serial
    flat_native_post_data = []
    for idx, data_i in clean_valid_lang_loanword_data.iterrows():
        clean_data_i = data_i.loc[post_data_vars]
        for var_j in native_verb_vars:
            for verb_k in data_i.loc[var_j]:
                data_k = clean_data_i.copy()
                data_k.loc['native_word_category'] = var_j
                word_type = verb_k
                # map light verb -> native verb to make type the same (fixed effect!!)
                # e.g. "pedir disculpas" -> "disculpar"
                if(var_j == 'native_light_verb'):
                    word_type = native_light_integrated_verb_lookup[word_type]
                data_k.loc['native_word_type'] = word_type
                data_k.loc['native_verb'] = verb_k
                flat_native_post_data.append(data_k)
    flat_native_post_data = pd.concat(flat_native_post_data, axis=1).transpose()
    logging.info('%d flat native verb data'%(flat_native_post_data.shape[0]))
    # per-author
    # author | native verb integration % | date
    author_var = 'screen_name'
    word_var = 'native_word_type'
    word_category_var = 'native_word_category'
    per_author_native_pct_data = flat_native_post_data.groupby([author_var, word_var]).apply(lambda x: (x.loc[:, word_category_var]=='native_integrated_verb').sum() / x.shape[0])
    logging.info('%d per author per word data'%(per_author_native_pct_data.shape[0]))
    per_author_native_pct_data = per_author_native_pct_data.reset_index().rename(columns={0 : 'integrated_verb_pct'})
    # write data
    flat_native_post_data_file = os.path.join(data_dir, 'native_integrated_light_verbs_per_post.tsv')
    per_author_native_pct_data_file = os.path.join(data_dir, 'native_integrated_light_verbs_per_author.tsv')
    flat_native_post_data.to_csv(flat_native_post_data_file, sep='\t', index=False)
    per_author_native_pct_data.to_csv(per_author_native_pct_data_file, sep='\t', index=False)
    
if __name__ == '__main__':
    main()