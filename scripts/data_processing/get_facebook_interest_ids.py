"""
Mine FB for interest IDs that match given string queries.

E.g. what is the ID for Shakira?
"""
from argparse import ArgumentParser
import logging
import os
from data_helpers import AccessTokenHandler, load_data_manual
from functools import reduce
from ast import literal_eval
import pandas as pd
import requests
from time import sleep
import json
import re
import sys

def query_spec_data(spec_name, access_token_handler):
    """
    Query FB for data corresponding to spec_name.
    
    :param spec_name: name of specification (e.g. "Brazil" => "Brazil (Ex-pats)")
    :param access_token_handler: handling access token data
    
    spec_data :: spec data (ID, full name, audience, etc.)
    """
    response_data = None
    try_ctr = 0
    MAX_TRY_COUNT = 10
    query_sleep_time = 300
    while(response_data is None and try_ctr < MAX_TRY_COUNT):
        if(access_token_handler.get_curr_token_rate_limited()):
            access_token_handler.next_token()
            logging.info('switched to token %s'%(access_token_handler.get_access_token_curr()))
        access_token = access_token_handler.get_access_token_curr()
        user_ID = access_token_handler.get_user_ID_curr()
        query_URL = f'https://graph.facebook.com/v6.0/act_{user_ID}/targetingsearch?access_token={access_token}&q={spec_name}'
        # tmp debugging
#         logging.info(query_URL)
        try:
            response = requests.get(query_URL)
            response_data = json.loads(response.text)
            logging.info(f'response data:{response_data}')
            access_token_handler.set_curr_token_rate_limited(False)
            # handle error codes
            if(response_data.get('error') is not None):
                error_code = response_data['error']['code']
                if(error_code == 80004):
                    logging.info('rate limit reached; sleeping now')
                    try_ctr += 1
                    access_token_handler.set_curr_token_rate_limited(True)
                    sleep(query_sleep_time)
                    response_data = None
        except Exception as e:
            logging.info(f'query error {e}')
            try_ctr += 1
            access_token_handler.set_curr_token_rate_limited(True)
            sleep(query_sleep_time)
            response_data = None
    # extract names and IDs from response
    spec_data = pd.DataFrame()
    if(response_data is not None and response_data.get('data') is not None):
        spec_data_response = response_data['data']
        spec_data = pd.DataFrame(spec_data_response)
        # responses can include bad matches
        # e.g. querying "M.I.A." yields "Animal" (???)
        # clean data to include actual query name
        if(spec_data.shape[0] > 0):
            spec_data = spec_data[spec_data.loc[:, 'name'].apply(lambda x: spec_name in x)]
    if(spec_data.shape[0] == 0):
        # assign null vals for all keys except name
        data_keys = ['id', 'name', 'path', 'audience_size', 'description']
        null_type = 'interests'
        spec_data = pd.DataFrame([spec_name], columns=['query'])
        spec_data = spec_data.assign(**{
            k : ''
            for k in data_keys
        })
        spec_data = spec_data.assign(**{'type' : null_type})
        spec_data = spec_data.assign(**{'exists' : False})
    else:
        spec_data = spec_data.assign(**{'exists' : True})
    # add query name for bookkeeping
    spec_data.loc[:, 'query'] = spec_name
    return spec_data

def collect_all_interest_data(interest_names, interest_data_out_file, access_token_handler):
    """
    Query FB for interest names, write repeatedly to file.
    
    :param interest_names: interest names to query
    :param interest_data_out_file: interest data output
    :param access_token_handler: access token handler
    :param audience_param: audience param ('estimate_mau' = monthly audience estimate, 'estimate_dau' = daily audience estimate)
    """
    if(os.path.exists(interest_data_out_file)):
        old_interest_data = pd.read_csv(interest_data_out_file, sep='\t', index_col=False)
        old_interest_data.fillna('', inplace=True)
#         print(old_interest_data.head())
        old_interest_names = old_interest_data[old_interest_data.loc[:, 'id'] != -1].loc[:, 'query'].apply(lambda x: x.lower()).unique()
#         print(f'old interest names {old_interest_names}')
#         old_interest_names = old_interest_data[~old_interest_data.loc[:, 'exists']].loc[:, 'query'].unique()
        interest_names = list(set(interest_names) - set(old_interest_names))
    else:
        old_interest_data = []
    query_type = 'interests'
    data_write_ctr = 50
    if(len(interest_names) > 0):
        interest_data = []
        for i, interest_name_i in enumerate(interest_names):
            ## TODO: multiple access tokens, user IDs
            logging.info(f'mining interest {interest_name_i}')
            interest_data_i = query_spec_data(interest_name_i, access_token_handler)
            # limit to interest data
            interest_data_i = interest_data_i[interest_data_i.loc[:, 'type']==query_type]
            logging.info('interest %s has %d data'%(interest_name_i, interest_data_i.shape[0]))
            interest_data.append(interest_data_i)
            # repeatedly write to file => avoid early crashing
            if(i % data_write_ctr == 0 and len(interest_data) > 0):
                combined_interest_data = pd.concat(interest_data, axis=0)
                if(len(old_interest_data) > 0):
                    combined_interest_data = pd.concat([old_interest_data, combined_interest_data], axis=0)
                combined_interest_data.to_csv(interest_data_out_file, sep='\t', index=False)
        interest_data = pd.concat(interest_data, axis=0)
    else:
        interest_data = pd.DataFrame()
    # combine with old data
    if(len(old_interest_data) > 0):
        interest_data = pd.concat([old_interest_data, interest_data], axis=0)
        interest_data.drop_duplicates(['name', 'query'], inplace=True)
    # cleanup
    num_matcher = re.compile('\d+')
    interest_data = interest_data.assign(**{
        'id' : interest_data.loc[:, 'id'].apply(lambda x: int(x) if num_matcher.search(str(x)) is not None else -1)
    })
    return interest_data

def extract_literal_eval(x):
    x_lit = []
    try:
        x_lit = literal_eval(x)
    except Exception as e:
        pass
    return x_lit

def main():
    parser = ArgumentParser()
    parser.add_argument('interest_data') # music interest data: ../../data/culture_metadata/spotify_musician_data.tsv
    parser.add_argument('--auth_data', default='../../data/culture_metadata/facebook_auth_multi.csv')
    parser.add_argument('--out_dir', default='../../data/culture_metadata/')
    args = vars(parser.parse_args())
    logging_file = '../../output/get_facebook_interest_ids.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    ## convert interests to IDs
    ## load interests
    interest_data_file = args['interest_data']
    interest_data_file_name = os.path.basename(interest_data_file).split('.')[0]
    interest_data = pd.read_csv(interest_data_file, sep='\t', index_col=False)
    interest_data.fillna('', inplace=True)
    # Spotify: get interest names from musician data
    if(interest_data_file_name == 'spotify_musician_data'):
        name_var = 'name'
        interest_names = interest_data.loc[:, 'name'].values
    # YouTube: get interest names from video title and user-generated tags 
    elif(interest_data_file_name == 'youtube_video_music_genre_data'):
        title_name_var = 'title_artist'
        artist_vars = ['latin_american_artist_tags', 'us_american_artist_tags']
        interest_names = interest_data[(interest_data.loc[:, title_name_var]!='') & ((interest_data.loc[:, 'has_latin_american_title_artist']) | (interest_data.loc[:, 'has_us_american_title_artist']))].loc[:, title_name_var].unique().tolist()
        for artist_var in artist_vars:
            interest_data = interest_data.assign(**{
                artist_var : interest_data.loc[:, artist_var].apply(lambda x: list(extract_literal_eval(x)))
            })
            interest_names += list(reduce(lambda x,y: x+y, interest_data[interest_data.loc[:, artist_var]!=''].loc[:, artist_var].values))
    interest_names = list(set(map(lambda x: x.lower(), interest_names)))
    # limit to valid potential interest names
    max_interest_word_len = 4
    min_interest_char_len = 3
    interest_names = list(filter(lambda x: len(x) > min_interest_char_len and len(x.split(' ')) <= max_interest_word_len, interest_names))
    logging.info('%d interest names to mine: examples %s'%(len(interest_names), str(interest_names[:10])))
    # tmp debugging
    
#     tmp debugging
#     interest_names = interest_names[:10]    
    auth_data_file = args['auth_data']
    auth_data = pd.read_csv(auth_data_file, sep=',', index_col=False)
    # get all token/ID pairs
    access_tokens = auth_data.loc[:, 'access_token'].values
    user_IDs = auth_data.loc[:, 'user_id'].values
    access_token_handler = AccessTokenHandler(access_tokens, user_IDs)
    out_dir = args['out_dir']
    interest_id_data_out_file = os.path.join(out_dir, 'facebook_interest_data.tsv')
#     interest_data = collect_all_interest_data(interest_names, interest_data_out_file, access_token, user_ID)
    interest_id_data = collect_all_interest_data(interest_names, interest_id_data_out_file, access_token_handler)
    interest_id_data.to_csv(interest_id_data_out_file, sep='\t', index=False)
    
if __name__ == '__main__':
    main()