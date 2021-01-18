"""
Organize loanword and native verb forms 
to query in external corpus.
One verb per line, in infinitive form.
Limit by frequency for easier queries: top-50 by loanword/native
"""
from argparse import ArgumentParser
import logging
import os
import pandas as pd
from data_helpers import conjugate_verb
from functools import reduce
import re

def extract_all_light_verb_phrases(txt):
    verb_str = txt.split(' ')[0]
    non_verb_str = ' '.join(txt.split(' ')[1:])
    verbs = verb_str.split('|')
    verb_phrases = []
    for verb in verbs:
        verb_phrases.append(' '.join([verb, non_verb_str]))
    return verb_phrases

def conjugate_light_verb(txt):
    verb_str = txt.split(' ')[0]
    non_verb_str = ' '.join(txt.split(' ')[1:])
    verb_forms = conjugate_verb(verb_str)
    light_verb_forms = list(map(lambda x: ' '.join([x, non_verb_str]), verb_forms))
    return light_verb_forms

def main():
    parser = ArgumentParser()
    parser.add_argument('loanword_post_data') # ../../data/mined_tweets/loanword_verb_posts_CLUSTER=twitter_posts_STARTDATE=2017_7_9_ENDDATE=2019_4_6.tsv
    parser.add_argument('native_verb_post_data') # ../../data/mined_tweets/native_integrated_light_verbs_per_post.tsv
    parser.add_argument('--loanword_data', default='../../data/loanword_resources/wiktionary_twitter_reddit_loanword_integrated_verbs_light_verbs.tsv')
    parser.add_argument('--native_verb_data', default='../../data/loanword_resources/native_verb_light_verb_pairs.csv')
    parser.add_argument('--top_k', type=int, default=50)
    parser.add_argument('--out_dir', default='../../data/loanword_resources')
    args = vars(parser.parse_args())
    logging_file = '../../output/organize_verb_forms_for_corpus_query.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    ## load data
    loanword_data = pd.read_csv(args['loanword_data'], sep='\t')
    native_verb_data = pd.read_csv(args['native_verb_data'], sep=',')
    loanword_post_data = pd.read_csv(args['loanword_post_data'], sep='\t', usecols=['loanword'])
    native_verb_post_data = pd.read_csv(args['native_verb_post_data'], sep='\t', usecols=['native_word_type'])
    
    ## filter data by count
    loanword_var = 'loanword'
    native_verb_var = 'native_word_type'
    top_k = args['top_k']
    loanword_counts = loanword_post_data.loc[:, loanword_var].value_counts()
    native_verb_counts = native_verb_post_data.loc[:, native_verb_var].value_counts()
    valid_loanwords = loanword_counts.index[:top_k]
    valid_native_verbs = native_verb_counts.index[:top_k]
    valid_loanword_data = loanword_data[loanword_data.loc[:, loanword_var].isin(valid_loanwords)]
    valid_native_verb_data = native_verb_data[native_verb_data.loc[:, 'integrated_verb'].isin(valid_native_verbs)]
    logging.info('%d/%d valid loanwords'%(valid_loanword_data.shape[0], loanword_data.shape[0]))
    logging.info('%d/%d valid native verbs'%(valid_native_verb_data.shape[0], native_verb_data.shape[0]))
    
    ## clean data
    # for loanword light verbs: get all possible verbs
    valid_loanword_data = valid_loanword_data.assign(**{
        'light_verb_phrases' : valid_loanword_data.loc[:, 'light verb'].apply(extract_all_light_verb_phrases)
    })
    # flatten
    flat_loanword_data = []
    for idx, data_i in valid_loanword_data.iterrows():
        for verb_phrase in data_i.loc['light_verb_phrases']:
            flat_loanword_data.append([data_i.loc['loanword'], data_i.loc['integrated verb'], verb_phrase])
    valid_loanword_data = pd.DataFrame(flat_loanword_data, columns=['loanword', 'integrated_verb', 'light_verb'])
    # combine data for bookkeeping
    valid_loanword_data = valid_loanword_data.assign(**{
        'word_category' : 'loanword'
    })
    valid_native_verb_data = valid_native_verb_data.assign(**{
        'word_category' : 'native_verb'
    })
    combined_word_data = pd.concat([valid_loanword_data, valid_native_verb_data], axis=0)
    # clean parentheses
    paren_matcher = re.compile('[\(\)]')
    light_verb_var = 'light_verb'
    combined_word_data = combined_word_data.assign(**{
        light_verb_var : combined_word_data.loc[:, light_verb_var].apply(lambda x: paren_matcher.sub('', x))
    })
    # conjugate verbs
    integrated_verb_var = 'integrated_verb'
    combined_word_data = combined_word_data.assign(**{
        f'{integrated_verb_var}_all_forms' : combined_word_data.loc[:, integrated_verb_var].apply(lambda x: conjugate_verb(x))
    })
    combined_word_data = combined_word_data.assign(**{
        f'{light_verb_var}_all_forms' : combined_word_data.loc[:, light_verb_var].apply(lambda x: conjugate_light_verb(x))
    })
    # remove ambiguous forms from integrated verb: e.g. ending in -o and -a
    ambiguous_integrated_verbs = ['accesar', 'auditar', 'boxear', 'chequear', 'formear', 'frizar']
    for ambiguous_integrated_verb in ambiguous_integrated_verbs:
        false_positive_verb_matcher = re.compile('^(%s)$'%('|'.join([ambiguous_integrated_verb.replace('ar', 'o'), ambiguous_integrated_verb.replace('ar', 'a')])))
        combined_word_data = combined_word_data.assign(**{
            f'{integrated_verb_var}_all_forms' : combined_word_data.loc[:, f'{integrated_verb_var}_all_forms'].apply(lambda x: list(filter(lambda y: false_positive_verb_matcher.search(y) is None, x)))
        })
    # convert to query
    # uppercase everything => match lemma
#     word_vars = [integrated_verb_var, light_verb_var]
#     for word_var in word_vars:
#         valid_loanword_data = valid_loanword_data.assign(**{
#             word_var : valid_loanword_data.loc[:, word_var].apply(lambda x: x.upper())
#         })
#         valid_native_verb_data = valid_native_verb_data.assign(**{
#             word_var : valid_native_verb_data.loc[:, word_var].apply(lambda x: x.upper())
#         })
    
    ## write to file
    # one query per line
    combined_query_words = []
    word_vars = [f'{integrated_verb_var}_all_forms', f'{light_verb_var}_all_forms']
    for word_var in word_vars:
        query_words = list(reduce(lambda x,y: x+y, combined_word_data.loc[:, word_var].values))
        combined_query_words += query_words
    out_dir = args['out_dir']
    combined_word_data_out_file = os.path.join(out_dir, 'loanword_native_verb_query_data.tsv')
    combined_word_out_file = os.path.join(out_dir, 'loanword_native_verb_queries.txt')
    combined_word_data.to_csv(combined_word_data_out_file, sep='\t', index=False)
    with open(combined_word_out_file, 'w') as output_file:
        output_file.write('\n'.join(combined_query_words))
    
if __name__ == '__main__':
    main()