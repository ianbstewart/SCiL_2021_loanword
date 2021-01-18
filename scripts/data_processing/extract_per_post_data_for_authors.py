"""
Extract post-level data for authors, including:

- hashtags
- @-mentions
- post length
- prior native verb use
- prior loanword use

Compute data for loanword and native verb posts.
"""
from argparse import ArgumentParser
import logging
import os
from datetime import datetime
import re
import numpy as np
import pandas as pd
from pandas import Timestamp
from data_helpers import clean_txt_simple

def convert_to_day(txt_date, date_matchers, date_formats):
    """
    Convert text to date, according to matching dates. 
    """
    date_day = None
    for date_matcher, date_format in zip(date_matchers, date_formats):
        try:
            txt_day = date_matcher.search(txt_date.strip()).group(0)
            date_day = datetime.strptime(txt_day, date_format)
        except Exception as e:
            pass
    if(date_day is None):
        print(txt_date)
        raise Exception('bad')
    # round down to nearest day
    if(date_day is not None):
        date_day = datetime(year=date_day.year, month=date_day.month, day=date_day.day)
    return date_day

def compute_prior_integrated_verb_rate(data, date_var):
    """
    Compute prior integrated verb rate for all authors
    in data.
    """
    unique_dates = list(map(lambda x: Timestamp(x), data.loc[:, date_var].unique()))
    per_date_integrated_counts = data[data.loc[:, 'verb_is_integrated']==1].loc[:, date_var].value_counts().sort_index(ascending=True)
    # add zeros
    zero_dates = list(set(unique_dates) - set(per_date_integrated_counts.index))
    if(len(zero_dates) > 0):
        zero_dates = pd.Series([0,]*len(zero_dates), index=zero_dates)
        per_date_integrated_counts = per_date_integrated_counts.append(zero_dates).sort_index(ascending=True)
    per_date_cumulative_integrated_counts = per_date_integrated_counts.cumsum()
    return per_date_cumulative_integrated_counts

def shift_author_dates_forward(data, date_var, author_var, shift=1):
    """
    Shift dates for authors forward by X times.
    """
    shifted_data = []
    for author_i, data_i in data.groupby(author_var):
        data_i.sort_values(date_var, ascending=False, inplace=True)
        data_i = data_i.iloc[shift:, :]
        shifted_data.append(data_i)
    shifted_data = pd.concat(shifted_data, axis=0)
    return shifted_data

def align_counts(data_1, data_2, author_var, date_var, count_var):
    """
    Align new data_2 with dates from data_1.
    """
    # need to connect count data from data_2 to data_1
    # get count 1 data at time t, align with count 2 data at time {max(t') \in t'<=t}
    aligned_data = []
    for author_i, data_1_i in data_1.groupby(author_var):
        data_2_i = data_2[data_2.loc[:, author_var] == author_i]
        if(data_2_i.shape[0] > 0):
            aligned_data_i = []
            for idx_j, data_1_j in data_1_i.iterrows():
                date_1_j = data_1_j.loc[date_var]
                data_2_j = data_2_i.assign(**{
                    'date_diff' : (date_1_j - data_2_i.loc[:, date_var]).apply(lambda x: x.days)
                })
                data_2_j = data_2_j[data_2_j.loc[:, 'date_diff'] >= 0.]
                if(data_2_j.shape[0] > 0):
                    data_2_j.sort_values('date_diff', inplace=True, ascending=True)
                    data_2_j = data_2_j.iloc[0, :].loc[[count_var]]
                    data_1_j = data_1_j.append(data_2_j)
                    aligned_data_i.append(data_1_j)
            if(len(aligned_data_i) > 0):
                aligned_data_i = pd.concat(aligned_data_i, axis=1).transpose()
                aligned_data.append(aligned_data_i)
    aligned_data = pd.concat(aligned_data, axis=0)
    return aligned_data

def compute_content_features(data, word_var='loanword_verb'):
    """
    Compute per-post content features:
    - hashtags
    - @-mentions
    - text length
    """
    text_var = 'text'
    clean_text_var = 'clean_text'
    data = data.assign(**{
        'clean_text' : data.loc[:, text_var].apply(lambda x: x.lower())
    })
    hashtag_matcher = re.compile('#[a-zA-Z0-9_]{3,}')
    mention_matcher = re.compile('@[a-zA-Z0-9_]{3,}')
    data = data.assign(**{
        'hashtags' : data.loc[:, 'text'].apply(lambda x: hashtag_matcher.findall(x)),
        'mentions' : data.loc[:, 'text'].apply(lambda x: mention_matcher.findall(x)),
    })
    data = data.assign(**{
        'has_hashtag' : data.loc[:, 'hashtags'].apply(lambda x: len(x) > 0).astype(int),
        'has_mention' : data.loc[:, 'mentions'].apply(lambda x: len(x) > 0).astype(int),
    })
    data = data.assign(**{
        'clean_text' : data.loc[:, 'text'].apply(lambda x: clean_txt_simple(x))
    })
    # replace all loanword phrases
    data = data.assign(**{
        'clean_text_no_word' : data.apply(lambda x: x.loc['clean_text'].replace(x.loc[word_var], ''), axis=1)
    })
    data = data.assign(**{
        'clean_text_no_word_len' : data.loc[:, 'clean_text_no_word'].apply(len)
    })
    return data

def compute_item_max_freq(items, item_count_lookup):
    max_freq = -1
    if(len(items) > 0):
        max_freq = 0 # any items are better than none
        item_freq = [item_count_lookup[item] for item in items if item in item_count_lookup]
        if(len(item_freq) > 0):
            max_freq = max(item_freq)
    return max_freq

def main():
    parser = ArgumentParser()
    parser.add_argument('loanword_data') # ../../data/mined_tweets/loanword_verb_posts_CLUSTER=twitter_posts_STARTDATE=2017_7_9_ENDDATE=2019_4_6.tsv
    parser.add_argument('out_dir')
    parser.add_argument('--extra_loanword_data', default=None) # ../../data/mined_tweets/loanword_author_tweets_all_archives_extra_loanword_tweets.gz
    parser.add_argument('--native_verb_data', default=None) # ../../data/mined_tweets/loanword_author_tweets_all_archives/native_integrated_light_verbs_per_post.tsv
    parser.add_argument('--hashtag_count_data', default=None) # ../../data/mined_tweets/loanword_author_tweets_all_archives_hashtag_freq.tsv
    parser.add_argument('--mention_count_data', default=None) # ../../data/mined_tweets/loanword_author_tweets_all_archives_mention_freq.tsv
    args = vars(parser.parse_args())
    logging_file = '../../output/extract_post_data_for_authors.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    ## load data
    # loanword data
    mined_loanword_data = pd.read_csv(args['loanword_data'], sep='\t')
    mined_loanword_data.rename(columns={'user_screen_name' : 'screen_name', 'user_id' : 'author_id'}, inplace=True)
    loanword_type_lookup = {
        'integrated_verb' : 'integrated_loanword',
        'light_verb' : 'light_verb_loanword',
    }
    # add extra loanword data from prior tweets (optional)
    if(args.get('extra_loanword_data') is not None):
        extra_loanword_data = pd.read_csv(args['extra_loanword_data'], sep='\t', compression='gzip')
        extra_loanword_data.drop(['permalink', 'username', 'date', 'formatted_date', 'mentions', 'hashtags', 'geo', 'urls', 'clean_txt'], axis=1, inplace=True)
        extra_loanword_data = extra_loanword_data.assign(**{
        'loanword_type' : extra_loanword_data.loc[:, 'loanword_type'].apply(loanword_type_lookup.get)
    })
        combined_loanword_data = pd.concat([mined_loanword_data, extra_loanword_data], axis=0)
        combined_loanword_data.drop_duplicates('id', inplace=True)
    else:
        combined_loanword_data = mined_loanword_data.copy()
    # cleanup
    drop_cols = ['Unnamed: 0']
    combined_loanword_data.drop(drop_cols, inplace=True, axis=1)
    # remove shares!
    share_matcher = re.compile('^RT @[a-zA-Z0-9_]+')
    combined_loanword_data = combined_loanword_data[combined_loanword_data.loc[:, 'text'].apply(lambda x: share_matcher.search(x) is None)]
    # get author names
    author_var = 'screen_name'
    combined_loanword_data = combined_loanword_data.assign(**{
        f'clean_{author_var}' : combined_loanword_data.loc[:, author_var].apply(lambda x: x.lower())
    })
    # assign integrated verb variable (need this for computing prior rate of verb integration)
    combined_loanword_data = combined_loanword_data.assign(**{
        'verb_is_integrated' : (combined_loanword_data.loc[:, 'loanword_type'] == 'integrated_loanword').astype(int)
    })
    # native verb data
    native_verb_data = pd.read_csv(args['native_verb_data'], sep='\t')
    # dump nan authors?
    author_var = 'screen_name'
    native_verb_data = native_verb_data[~native_verb_data.loc[:, author_var].apply(lambda x: type(x) is float and np.isnan(x))]
    # combine date columns
    native_verb_data = native_verb_data.assign(**{
        'date' : native_verb_data.loc[:, 'date'].fillna('') + native_verb_data.loc[:, 'created_at'].fillna('')
    })
    # convert dates again...yay
    date_day_var = 'date_day'
    date_var = 'date'
    date_matchers = [
        re.compile('^[0-9]{4}\-[0-9]{2}\-[0-9]{2}'),
        re.compile('^[A-Z][a-z]{2} [A-Z][a-z]{2} [0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} \+0000 20[0-9]{2}$'),
    ]
    date_formats = [
        '%Y-%m-%d',
        '%a %b %d %H:%M:%S +0000 %Y',
    ]
    native_verb_data = native_verb_data.assign(**{
        date_day_var : native_verb_data.loc[:, date_var].apply(lambda x: convert_to_day(x, date_matchers, date_formats))
    })
    # compute prior native integrated verb count
    author_var = 'screen_name'
    clean_author_var = 'clean_screen_name'
    native_verb_data = native_verb_data.assign(**{
        'verb_is_integrated' : (native_verb_data.loc[:, 'native_word_category']=='native_integrated_verb').astype(int),
        clean_author_var : native_verb_data.loc[:, author_var].apply(lambda x: x.lower()),
    })
    per_author_native_verb_counts = native_verb_data.groupby([clean_author_var]).apply(lambda x: x.loc[:, date_day_var].value_counts().sort_index(ascending=True).cumsum()).reset_index().rename(columns={'level_1' : date_day_var, date_day_var : 'native_verb_count'})
    per_author_native_verb_integrated_counts = native_verb_data.groupby(clean_author_var).apply(lambda x: compute_prior_integrated_verb_rate(x, date_day_var)).reset_index().rename(columns={'level_1' : date_day_var, 0 : 'native_verb_integrated_count'})
    # combine
    author_native_verb_counts = pd.merge(per_author_native_verb_counts, per_author_native_verb_integrated_counts, on=[clean_author_var, date_day_var], how='left')
    # compute native integrated verb percent
    author_native_verb_counts = author_native_verb_counts.assign(**{
        'native_verb_integrated_pct' : author_native_verb_counts.loc[:, 'native_verb_integrated_count'] / author_native_verb_counts.loc[:, 'native_verb_count']
    })
    
    ## content features: hashtags, @-mentions, post length
    combined_loanword_data = compute_content_features(combined_loanword_data, word_var='loanword_verb')
    native_verb_data = compute_content_features(native_verb_data, word_var='native_verb')
    
    # prior data variables
    date_var = 'created_at'
    date_matchers = [
        re.compile('^[0-9]{4}\-[0-9]{2}\-[0-9]{2}'),
        re.compile('^[A-Z][a-z]{2} [A-Z][a-z]{2} [0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} \+0000 20[0-9]{2}$'),
    ]
    date_formats = [
        '%Y-%m-%d',
        '%a %b %d %H:%M:%S +0000 %Y',
    ]
    date_day_var = 'date_day'
    combined_loanword_data = combined_loanword_data.assign(**{
        date_day_var : combined_loanword_data.loc[:, date_var].apply(lambda x: convert_to_day(x, date_matchers, date_formats))
    })
    clean_author_var = 'clean_screen_name'
    author_integrated_loanword_per_date_counts = combined_loanword_data.groupby(clean_author_var).apply(lambda x: compute_prior_integrated_verb_rate(x, date_day_var)).reset_index().rename(columns={'level_1' : date_day_var, date_day_var : 'prior_integrated_loanword_count'})
    clean_author_var = 'clean_screen_name'
    loanword_var = 'loanword'
    date_day_var = 'date_day'
    author_loanword_per_date_counts = combined_loanword_data.groupby([clean_author_var]).apply(lambda x: x.loc[:, date_day_var].value_counts().sort_index(ascending=True).cumsum()).reset_index().rename(columns={'level_1' : date_day_var, date_day_var : 'prior_loanword_count'})
    author_integrated_loanword_per_date_counts = combined_loanword_data.groupby(clean_author_var).apply(lambda x: compute_prior_integrated_verb_rate(x, date_day_var)).reset_index().rename(columns={'level_1' : date_day_var, 0 : 'prior_integrated_loanword_count'})
    ## combine 
    author_loanword_prior_counts = pd.merge(author_loanword_per_date_counts, author_integrated_loanword_per_date_counts, on=[clean_author_var, date_day_var])
    ## compute prior rate of integrated loanword/native verb use
    author_loanword_prior_counts = author_loanword_prior_counts.assign(**{
        'integrated_loanword_pct' : author_loanword_prior_counts.loc[:, 'prior_integrated_loanword_count'] / author_loanword_prior_counts.loc[:, 'prior_loanword_count']
    })
    date_shift = 1
    author_loanword_prior_counts = shift_author_dates_forward(author_loanword_prior_counts, date_day_var, clean_author_var, shift=date_shift)
    clean_author_var = 'clean_screen_name'
    date_var = 'date_day'
    count_var = 'native_verb_integrated_pct'
    author_prior_word_use_data = align_counts(author_loanword_prior_counts, author_native_verb_counts, clean_author_var, date_var, count_var)
    ## join with prior data
    prior_count_vars = ['prior_loanword_count', 'prior_integrated_loanword_count']
    for prior_count_var in prior_count_vars:
        if(prior_count_var in combined_loanword_data.columns):
            combined_loanword_data.drop(prior_count_var, axis=1, inplace=True)
    combined_loanword_data = pd.merge(combined_loanword_data, author_loanword_prior_counts, on=[clean_author_var, date_day_var], how='left')
    combined_loanword_data = pd.merge(combined_loanword_data, author_prior_word_use_data.loc[:, ['clean_screen_name', 'date_day', 'native_verb_integrated_pct']], on=['clean_screen_name', 'date_day'], how='left')
    native_verb_data = pd.merge(native_verb_data, author_loanword_prior_counts, on=[clean_author_var, date_day_var], how='left')
    native_verb_data = pd.merge(native_verb_data, author_prior_word_use_data.loc[:, ['clean_screen_name', 'date_day', 'native_verb_integrated_pct']], on=['clean_screen_name', 'date_day'], how='left')
    # fix missing vals
    combined_loanword_data.fillna(value={prior_count_var : 0. for prior_count_var in prior_count_vars}, inplace=True)
    native_verb_data.fillna(value={prior_count_var : 0. for prior_count_var in prior_count_vars}, inplace=True)
#     print(combined_loanword_data.columns)
#     print(combined_loanword_data.shape[0])
    
    ## optional: add frequency for hashtags, mentions
#     print('example hashtags %s'%(combined_loanword_data[combined_loanword_data.loc[:, 'hashtags'].apply(lambda x: len(x) > 0)].loc[:, 'hashtags'].head(5)))
    if(args.get('hashtag_count_data') is not None and args.get('mention_count_data') is not None):
        hashtag_count_data = pd.read_csv(args['hashtag_count_data'], sep='\t', index_col=False)
        mention_count_data = pd.read_csv(args['mention_count_data'], sep='\t', index_col=False)
        hashtag_count_lookup = dict(zip(hashtag_count_data.loc[:, 'hashtag'].values, hashtag_count_data.loc[:, 'freq_category'].values))
        mention_count_lookup = dict(zip(mention_count_data.loc[:, 'mention'].values, mention_count_data.loc[:, 'freq_category'].values))
#         print('hashtag counts %s'%(list(hashtag_count_lookup.items())[:10]))
        # tmp debugging
#         print('testing hashtag type')
#         for hashtags in combined_loanword_data.loc[:, 'hashtags'].values:
#             if(len(hashtags) > 0):
#                 a = 
#             if(type(hashtags) is not list):
#                 print(hashtags)
#                 print(type(hashtags))
#                 break
        # post hashtag/mention frequency = maximum relative frequency of all hashtags/mentions
        combined_loanword_data = combined_loanword_data.assign(**{
            'max_hashtag_freq' : combined_loanword_data.loc[:, 'hashtags'].apply(lambda x: compute_item_max_freq(x, hashtag_count_lookup))
        })
        combined_loanword_data = combined_loanword_data.assign(**{
            'max_mention_freq' : combined_loanword_data.loc[:, 'mentions'].apply(lambda x: compute_item_max_freq(x, mention_count_lookup))
        })
        native_verb_data = native_verb_data.assign(**{
            'max_hashtag_freq' : native_verb_data.loc[:, 'hashtags'].apply(lambda x: compute_item_max_freq(x, hashtag_count_lookup))
        })
        native_verb_data = native_verb_data.assign(**{
            'max_mention_freq' : native_verb_data.loc[:, 'mentions'].apply(lambda x: compute_item_max_freq(x, mention_count_lookup))
        })
        
    
    ## save per-post data to file
    combined_loanword_data.rename(columns={'clean_screen_name' : 'screen_name'}, inplace=True)
    per_post_data_cols = ['id', 'screen_name', 'has_hashtag', 'has_mention', 'clean_text_no_word_len', 'prior_loanword_count', 'prior_integrated_loanword_count', 'integrated_loanword_pct', 'native_verb_integrated_pct', 'max_hashtag_freq', 'max_mention_freq']
    combined_loanword_data = combined_loanword_data.loc[:, per_post_data_cols]
    native_verb_data = native_verb_data.loc[:, per_post_data_cols]
    loanword_data_out_file_name = os.path.join(args['out_dir'], 'loanword_author_per_post_extra_data.tsv')
    native_verb_out_file_name = os.path.join(args['out_dir'], 'native_verb_author_per_post_extra_data.tsv')
    combined_loanword_data.to_csv(loanword_data_out_file_name, sep='\t', index=False)
    native_verb_data.to_csv(native_verb_out_file_name, sep='\t', index=False)

if __name__ == '__main__':
    main()