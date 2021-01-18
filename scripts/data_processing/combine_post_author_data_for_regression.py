"""
Combine post-level data and author social
data for regression.
Based on code from organize_data_for_regression.ipynb.
"""
from argparse import ArgumentParser
import logging
import os
import numpy as np
import pandas as pd
import re
from data_helpers import bin_data_var

def main():
    parser = ArgumentParser()
    parser.add_argument('--post_data', nargs='+') 
    # loanword post data: ../../data/mined_tweets/loanword_verb_posts_CLUSTER=twitter_posts_STARTDATE=2017_7_9_ENDDATE=2019_4_6.tsv
    # native verb post data: ../../data/mined_tweets/loanword_author_tweets_all_archives/native_integrated_light_verbs_per_post.tsv
    parser.add_argument('--author_data')
    # all authors social data: ../../data/mined_tweets/loanword_authors_combined_full_social_data.tsv
    parser.add_argument('--per_post_data', default=None)
    # extra post data (extra loanwords from prior data)
    parser.add_argument('--extra_post_data', default=None)
#     parser.add_argument()
    # loanword per-post data: ../../data/mined_tweets/loanword_author_per_post_extra_data.tsv
    # native verb per-post data: ../../data/mined_tweets/native_verb_author_per_post_extra_data.tsv
    parser.add_argument('--out_dir', default='../../data/mined_tweets/')
    parser.add_argument('--data_name', default='loanword_verbs')
    args = vars(parser.parse_args())
    logging_file = '../../output/combine_post_author_data_for_regression.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    ## clean data
    post_data = []
    for post_data_file in args['post_data']:
        post_data_i = pd.read_csv(post_data_file, sep='\t')
        post_data.append(post_data_i)
    post_data = pd.concat(post_data, axis=0)
    post_data.fillna('', inplace=True)
    author_var = 'screen_name'
    if('user_screen_name' in post_data.columns):
        post_data.rename(columns={'user_screen_name' : author_var}, inplace=True)
    # clean post authors
    post_data = post_data[post_data.loc[:, author_var] != '']
    logging.info('%d post data'%(post_data.shape[0]))
    post_data = post_data.assign(**{
        author_var : post_data.loc[:, author_var].apply(lambda x: x.lower())
    })
    # clean RTs 
    txt_var = 'text'
    RT_matcher = re.compile('^RT @[a-zA-Z0-9_]+')
    post_data = post_data[post_data.loc[:, txt_var].apply(lambda x: RT_matcher.search(x) is None)]
    # get author data
    author_data = pd.read_csv(args['author_data'], sep='\t')
    author_data.fillna('', inplace=True)
    
    ## add dependent variable
    data_name = args['data_name']
#     dep_var = 'has_light_verb' # friendship ended with light verbs, now I only trust integrated verbs
    dep_var = 'has_integrated_verb'
    if('loanword_verbs' in data_name):
        loanword_dep_var = 'loanword_type'
        post_data = post_data.assign(**{
            dep_var : (post_data.loc[:, loanword_dep_var]=='integrated_loanword').astype(int)
        })
    elif('native_verbs' in data_name):
        native_verb_dep_var = 'native_word_category'
        post_data = post_data.assign(**{
            dep_var : (post_data.loc[:, native_verb_dep_var]=='native_integrated_verb').astype(int)
        })
    
    ## cleanup, optional extra data
    # optional: add extra post data
    if(args.get('extra_post_data') is not None):
        extra_loanword_data = pd.read_csv(args['extra_post_data'], sep='\t', compression='gzip', index_col=False)
        print('%d extra data'%(extra_loanword_data.shape[0]))
        extra_loanword_data.drop(['permalink', 'username', 'date', 'formatted_date', 'mentions', 'hashtags', 'geo', 'urls', 'clean_txt',], axis=1, inplace=True)
        loanword_type_lookup = {
            'integrated_verb' : 'integrated_loanword',
            'light_verb' : 'light_verb_loanword',
        }
        extra_loanword_data = extra_loanword_data.assign(**{
            'loanword_type' : extra_loanword_data.loc[:, 'loanword_type'].apply(loanword_type_lookup.get)
        })
        extra_loanword_data = extra_loanword_data.assign(**{
            dep_var : (extra_loanword_data.loc[:, 'loanword_type']=='integrated_loanword').astype(int)
        })
        post_data = pd.concat([post_data, extra_loanword_data], axis=0)
    post_author_data = pd.merge(post_data, author_data, on=author_var)
    # optional: add per-post data e.g. presence of hashtag
    if(args.get('per_post_data') is not None):
        per_post_vars = ['has_hashtag', 'has_mention', 'max_hashtag_freq', 'max_mention_freq']
        id_var = 'id'
        per_post_data_cols = per_post_vars + [id_var]
        per_post_data = pd.read_csv(args['per_post_data'], sep='\t', index_col=False, usecols=per_post_data_cols)
        post_author_data = pd.merge(post_author_data, per_post_data, on='id', how='left')
    # remove duplicates
    id_var = 'id'
    txt_var = 'text'
    post_author_data.drop_duplicates(id_var, inplace=True)
    post_author_data.drop_duplicates(txt_var, inplace=True)
    logging.info('after cleaning: dep var %s has values %s'%(dep_var, post_author_data.loc[:, dep_var].value_counts()))

    ## save to file
    out_dir = args['out_dir']
    out_file = os.path.join(out_dir, f'{data_name}_post_social_data.tsv')
    post_author_data.to_csv(out_file, sep='\t', index=False)
    
if __name__ == '__main__':
    main()