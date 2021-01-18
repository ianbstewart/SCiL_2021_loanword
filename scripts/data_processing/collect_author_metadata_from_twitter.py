"""
Collect author metadata from Twitter: 
- age
"""
from argparse import ArgumentParser
import logging
import pandas as pd
from math import ceil
import os
from time import sleep
from data_helpers import load_twitter_API

def collect_author_data(author_names, twitter_auth_data_file, out_file, author_keys=['id', 'created_at']):
    """
    Query Twitter API for author data.
    
    :param author_names: author screen names
    :param twitter_auth_data_file: API auth file
    :param out_file: output file
    :param author_keys: relevant author data to collect
    """
    MAX_SLEEP_TIME=300
    MAX_TRY_COUNT=10
    twitter_api = load_twitter_API(twitter_auth_data_file)
    name_chunk_size = 100
    chunk_count = int(ceil(len(author_names) / name_chunk_size))
    author_name_chunks = list(map(lambda x: author_names[(x*name_chunk_size):((x+1)*name_chunk_size)], range(chunk_count)))
    combined_author_data = []
    for author_name_chunk in author_name_chunks:
        print(f'author chunk = {author_name_chunk}')
        success = False
        try_ctr = 0
        while(not success and try_ctr < MAX_TRY_COUNT):
            try:
                author_data = twitter_api.UsersLookup(screen_name=author_name_chunk)
            except Exception as e:
                logging.info(f'rate limit with error {e}; sleeping for {MAX_SLEEP_TIME}')
                sleep(MAX_SLEEP_TIME)
                try_ctr += 1
        author_data = list(map(lambda x: {x.__getattribute__(k) for k in author_keys}, author_data))
        combined_author_data += author_data
        # write to file => fail fast die young
        combined_author_data_df = pd.DataFrame(combined_author_data)
        combined_author_data_df.to_csv(out_file, sep='\t', index=False)
    combined_author_data_df = pd.DataFrame(combined_author_data)
    return combined_author_data_df

def main():
    parser = ArgumentParser()
    parser.add_argument('loanword_data') # ../../data/mined_tweets/loanword_verb_posts_CLUSTER=twitter_posts_STARTDATE=2017_7_9_ENDDATE=2019_4_6.tsv
    parser.add_argument('--twitter_auth_data', default='../../data/mined_tweets/twitter_auth.csv')
    parser.add_argument('--out_dir', default='../../data/mined_tweets/')
    args = vars(parser.parse_args())
    logging_file = '../../output/collect_author_metadata_from_twitter.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    ## load data
    author_var = 'user_screen_name'
    author_data = pd.read_csv(args['loanword_data'], sep='\t', usecols=[author_var])
    author_names = list(author_data.loc[:, author_var].unique())
    # tmp debugging
    author_names = author_names[:50]

    ## mine data
    twitter_auth_data_file = args['twitter_auth_data']
    # TODO: multiple keys for rate limiting??
    author_keys = ['created_at', 'followers_count', 'friends_count', 'id', 'verified', 'statuses_count', 'screen_name']
    out_dir = args['out_dir']
    out_file = os.path.join(out_dir, 'loanword_author_account_metadata.tsv')
    author_data = collect_author_data(author_names, twitter_auth_data_file, out_file, author_keys=author_keys)
    logging.info(author_data)
    
    ## write to file
    out_dir = args['out_dir']
    out_file = os.path.join(out_dir, 'loanword_author_account_metadata.tsv')
    author_data.to_csv(out_file, sep='\t', index=False)
    
if __name__ == '__main__':
    main()