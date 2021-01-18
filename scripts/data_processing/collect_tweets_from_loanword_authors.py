"""
Collect prior tweets from loanword authors
in historical data using Get Old Tweets (TM).
TODO: change to SQL database for easier indexing
"""
from argparse import ArgumentParser
import logging
import os
# add GOT to path
import sys
sys.path.append('GetOldTweets-python')
import got3
import numpy as np
import pandas as pd
import urllib
from time import sleep
from urllib.error import HTTPError
import twitter

np.random.seed(2020)

def filter_non_exist_authors(authors, author_exist_data_file, twitter_auth_data_file):
    """
    Query Twitter API for authors who exist and remove non-existing authors 
    from list.
    """
    if(os.path.exists(author_exist_data_file)):
        old_author_exist_data = pd.read_csv(author_exist_data_file, sep='\t', index_col=False)
        already_queried_authors = old_author_exist_data.loc[:, 'author'].values
        exist_query_authors = list(set(authors) - set(already_queried_authors))
    else:
        old_author_exist_data = []
        exist_query_authors = list(authors)
    twitter_auth_data = pd.read_csv(twitter_auth_data_file, header=None, index_col=0).iloc[:, 0]
    twitter_api = twitter.Api(consumer_key=twitter_auth_data.loc['consumer_key'], 
                              consumer_secret=twitter_auth_data.loc['consumer_secret'],
                              access_token_key=twitter_auth_data.loc['access_token'],
                              access_token_secret=twitter_auth_data.loc['access_secret'])
    author_exist_chunk_size = 100
    author_chunks = list(map(lambda x: exist_query_authors[(x*author_exist_chunk_size):((x+1)*author_exist_chunk_size)], range(int(len(exist_query_authors) / author_exist_chunk_size)+1)))
    author_exist_data = []
    twitter_query_sleep_time = 300
    max_try_ctr = 5
    for author_chunk_i in author_chunks:
        success = False
        try_ctr = 0
        while(not success and try_ctr <= max_try_ctr):
            try:
                author_data_i = twitter_api.UsersLookup(screen_name=author_chunk_i)
                success = True
            except Exception as e:
                logging.info(f'rate limit querying user existence; sleep {twitter_query_sleep_time} seconds')
                sleep(twitter_query_sleep_time)
                try_ctr += 1
                author_data_i = []
        if(len(author_data_i) > 0):
            exist_authors_i = list(map(lambda x: x.screen_name.lower(), author_data_i))
            non_exist_authors_i = list(set(author_chunk_i) - set(exist_authors_i))
            author_exist_data += list(map(lambda x: [x, True], exist_authors_i))
            author_exist_data += list(map(lambda x: [x, False], non_exist_authors_i))
    author_exist_data = pd.DataFrame(author_exist_data, columns=['author', 'exists'])
    logging.info('collected author exist data:\n%s'%(author_exist_data.head()))
    if(len(author_exist_data) > 0):
        if(len(old_author_exist_data) > 0):
            author_exist_data = pd.concat([author_exist_data, old_author_exist_data], axis=0)
        author_exist_data.to_csv(author_exist_data_file, sep='\t', index=False)
    else:
        author_exist_data = old_author_exist_data.copy()
    non_exist_authors = set(author_exist_data[~author_exist_data.loc[:, 'exists']].loc[:, 'author'].values)
    authors = list(set(authors) - non_exist_authors)
    return authors

def collect_authors_per_word(data, authors_per_word=100, author_var='user_screen_name', word_var='loanword'):
    """
    Collect set of authors who used word 
    at least once.
    """
    author_set = set()
    for loanword_i, data_i in data.groupby(word_var):
        np.random.shuffle(data_i.values)
        authors_i = data_i.loc[:, author_var].unique()[:authors_per_word]
        author_set.update(authors_i)
    return author_set

def main():
    parser = ArgumentParser()
    parser.add_argument('loanword_author_data') # '../../data/mined_tweets/loanword_integrated_verb_author_counts_CLUSTER=twitter_posts.tsv'
#     parser.add_argument('--out_dir', default='../../data/mined_tweets/')
    parser.add_argument('--out_dir', default='../../data/mined_tweets/loanword_author_tweets/')
    parser.add_argument('--authors_per_loanword', type=int, default=0)
    parser.add_argument('--date_range', nargs='+', default=['2014-01-01', '2019-07-01'])
    parser.add_argument('--max_tweets', type=int, default=1000)
    parser.add_argument('--overwrite_files', type=bool, default=False)
    parser.add_argument('--filter_sample', default=None)
    # data for filtering non-existing authors
    parser.add_argument('--twitter_auth_data', default='../../data/mined_tweets/twitter_auth.csv')
    parser.add_argument('--author_exist_data', default='../../data/mined_tweets/loanword_author_tweets/author_exist_data.tsv')
    parser.add_argument('--author_mined_data', default='../../data/mined_tweets/loanword_author_tweets/')
    args = vars(parser.parse_args())
    filter_sample = args['filter_sample']
    if(filter_sample is not None):
        logging_file = f'../../output/collect_tweets_from_loanword_authors_FILTER={filter_sample}.txt'
    else:
        logging_file = '../../output/collect_tweets_from_loanword_authors.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    ## load author names
    loanword_author_data_file = args['loanword_author_data']
    if(loanword_author_data_file.split('.')[-1]=='gz'):
        loanword_author_data = pd.read_csv(loanword_author_data_file, sep='\t', compression='gzip', index_col=False)
    else:
        loanword_author_data = pd.read_csv(loanword_author_data_file, sep='\t', index_col=False)
    author_var = 'user_screen_name'
    if('screen_name' in loanword_author_data.columns):
        loanword_author_data.rename(columns={'screen_name':author_var}, inplace=True)
    all_loanword_authors = list(loanword_author_data.loc[:, author_var].unique())
    
    ## filter authors that do not exist
    author_exist_data_file = args['author_exist_data']
    twitter_auth_data_file = args['twitter_auth_data']
    existing_loanword_authors = filter_non_exist_authors(all_loanword_authors, author_exist_data_file, twitter_auth_data_file)
    loanword_author_data = loanword_author_data[loanword_author_data.loc[:, author_var].isin(existing_loanword_authors)]
    logging.info('%d/%d existing loanword authors'%(len(existing_loanword_authors), len(all_loanword_authors)))
    
    # sample authors, N per loanword type
#     loanword_authors = set()
    word_var = 'loanword'
    filter_sample = args['filter_sample']
    authors_per_loanword = args['authors_per_loanword']
    if(authors_per_loanword > 0):
        if(filter_sample is None):
            loanword_authors = collect_authors_per_word(loanword_author_data, authors_per_word=authors_per_loanword, author_var=author_var, word_var=word_var)
        else:
            filter_sample_var, filter_sample_val = filter_sample.split('=')
            filter_loanword_author_data = loanword_author_data[loanword_author_data.loc[:, filter_sample_var] == filter_sample_val]
            loanword_authors = collect_authors_per_word(filter_loanword_author_data, authors_per_word=authors_per_loanword, author_var=author_var, word_var=word_var)
    #         for balance_val_i, data_i in loanword_author_data.groupby(sample_balance_var):
    #             loanword_authors_i = collect_authors_per_word(data_i, authors_per_word=authors_per_loanword, author_var=author_var, word_var=word_var)
    #             logging.info('%d authors for balance var %s'%(len(loanword_authors_i), sample_balance_var))
    #             loanword_authors.update(loanword_authors_i)
    else:
        loanword_authors = loanword_author_data.loc[:, author_var].unique()
    loanword_authors = list(loanword_authors)
    logging.info('%d authors total'%(len(loanword_authors)))
    
    ## mine authors
    out_dir = args['out_dir']
    date_range = args['date_range']
    max_tweets = args['max_tweets']
#     loanword_author_data_name = os.path.basename(loanword_author_data_file).split('.')[0]
#     out_dir = os.path.join(out_dir, f'{loanword_author_data_name}_tweets')
    overwrite_files = args['overwrite_files']
    max_try_count = 5
    # may have to sleep between requests
    # because of rate limiting
    request_sleep_time = 30
    if(not os.path.exists(out_dir)):
        os.mkdir(out_dir)
    for author_i in loanword_authors:
        logging.info(f'processing author={author_i}')
        out_file_name_i = os.path.join(out_dir, f'{author_i}_tweets.gz')
        overwrite_file_i = overwrite_files
        if(os.path.exists(out_file_name_i)):
            old_tweets_data_i = pd.read_csv(out_file_name_i, sep='\t', index_col=False, compression='gzip')
            overwrite_file_i = overwrite_files or old_tweets_data_i.shape[0] <= max_tweets
        else:
            old_tweets_data_i = None
        if(not os.path.exists(out_file_name_i) or overwrite_file_i):
            criteria = got3.manager.TweetCriteria()
            criteria = criteria.setUsername(author_i)
            criteria = criteria.setSince(date_range[0]).setUntil(date_range[1])
            criteria = criteria.setMaxTweets(max_tweets)
            request_success = False
            request_try_ctr = 0
            tweets_i = []
            while(not request_success and request_try_ctr < max_try_count):
                ## TODO: check for author existence!
                # i.e. if error = 'author does not exist' then skip
                try:
                    logging.info(f'getting old tweets for author={author_i}')
                    tweets_i = got3.manager.TweetManager.getTweets(criteria)
                    request_success = True
#                 except urllib.error.HTTPError as e:
                except Exception as e:
                    logging.info(f'bad tweet mining because error={e}')
                    logging.info(f'sleeping for {request_sleep_time} sec')
                    sleep(request_sleep_time)
                    request_try_ctr += 1
            ## TODO: also get user data? or use what's available in original tweet??
            logging.info('extracted %d tweets for author=%s'%(len(tweets_i), author_i))
            if(len(tweets_i) > 0):
                # convert to dataframe
                tweets_data_i = pd.concat(list(map(lambda x: pd.Series(x.__dict__), tweets_i)), axis=1).transpose()
                # add author name
                tweets_data_i = tweets_data_i.assign(**{
                    'screen_name' : author_i
                })
                if(old_tweets_data_i is not None):
                    tweets_data_i = pd.concat([tweets_data_i, old_tweets_data_i], axis=0)
                    tweets_data_i.drop_duplicates('id', inplace=True)
                tweets_data_i.to_csv(out_file_name_i, sep='\t', index=False, compression='gzip')
    
if __name__ == '__main__':
    main()