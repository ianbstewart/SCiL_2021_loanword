"""
Collect similar artists to the ones provided
using Spotify suggestions. 
API: https://spotipy.readthedocs.io/en/2.7.0/
"""
from argparse import ArgumentParser
import logging
import os
import numpy as np
import pandas as pd
from unidecode import unidecode
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy import Spotify

def clean_txt(txt):
    return unidecode(txt.lower())
def collect_artist_data(artist_name, api, 
#                         search_url='https://api.spotify.com/v1/search', 
                        request_sleep_time=60,
                        artist_keys=['name', 'id', 'genres'],
                        SEARCH_LIMIT=20):
    """
    Collect all artist data from Spotify:
    follower count, genres, ID
    """
    clean_artist_name = clean_txt(artist_name)
    successful_search = False
    while(not successful_search):
        search_result = api.search(q=clean_artist_name, type='artist', limit=SEARCH_LIMIT)
        # TODO: status code
#         search_result = requests.get(search_url, params={'q' : clean_artist_name, 'type' : search_type}, headers={'authorization' : f'Bearer {auth_token}'})
#         sleep(10)
#         if(search_result.status_code == 200):
        successful_search = True
        # filter for name match
        search_result_data = search_result['artists']['items']
        valid_search_result_data = list(filter(lambda x: clean_txt(x['name'])==clean_artist_name, search_result_data))
        if(len(valid_search_result_data) > 0):
            # sort by followers
            valid_search_result_data = list(sorted(valid_search_result_data, key=lambda x: x['followers']['total'], reverse=True))
            # take top result
            matching_search_result = valid_search_result_data[0]
            artist_data = {k : matching_search_result[k] for k in artist_keys}
            artist_data['followers'] = matching_search_result['followers']['total']
        else:
            artist_data = {'name' : clean_artist_name}
#         elif(search_result.status_code == 429):
#             logging.info(f'error, too many requests, sleeping for {request_sleep_time} sec')
#             sleep(request_sleep_time)
    return artist_data

def collect_all_artist_data(artist_names, api):
    artist_data_combined = []
    for artist_name in artist_names:
        logging.info('processing artist %s'%(artist_name))
        artist_data = collect_artist_data(artist_name, api)
        artist_data = pd.Series(artist_data)
        artist_data_combined.append(artist_data)
    artist_data_combined = pd.concat(artist_data_combined, axis=1).transpose()
    return artist_data_combined

def get_similar_artists(artist_id, api, artist_keys=['name', 'id', 'genres'],):
    similar_artist_data_response = api.artist_related_artists(artist_id)
    similar_artists = similar_artist_data_response['artists']
    similar_artist_data_combined = []
    for similar_artist in similar_artists:
        artist_data = {k : similar_artist[k] for k in artist_keys}
        artist_data['followers'] = similar_artist['followers']['total']
        similar_artist_data_combined.append(pd.Series(artist_data))
    if(len(similar_artist_data_combined) > 0):
        similar_artist_data_combined = pd.concat(similar_artist_data_combined, axis=1).transpose()
    return similar_artist_data_combined

def get_similar_artists_all_ids(artist_ids, api):
    similar_artist_data_combined = []
    for artist_id in artist_ids:
        logging.info('processing ID %s'%(artist_id))
        similar_artist_data = get_similar_artists(artist_id, api)
        if(len(similar_artist_data) > 0):
            similar_artist_data = similar_artist_data.assign(**{
                'similar_artist_query_id' : artist_id,
            })
            similar_artist_data_combined.append(similar_artist_data)
    similar_artist_data_combined = pd.concat(similar_artist_data_combined, axis=0)
    return similar_artist_data_combined

def main():
    parser = ArgumentParser()
    parser.add_argument('artist_data') # ../../data/culture_metadata/
    parser.add_argument('--out_dir', default='../../data/culture_metadata/')
    parser.add_argument('--auth_data', default='../../data/culture_metadata/spotify_auth.csv')
    args = vars(parser.parse_args())
    artist_data_file_base = os.path.basename(args['artist_data']).split('.')[0]
    logging_file = f'../../output/collect_similar_artists_from_spotify_{artist_data_file_base}.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    ## load data
    artist_data = pd.read_csv(args['artist_data'], sep='\t', index_col=False)
    artist_data = artist_data.assign(**{
        'clean_name' : artist_data.loc[:, 'name'].apply(clean_txt)
    })
    
    ## if we don't have IDs, search for IDs
    auth_data = pd.read_csv(args['auth_data'], header=None, index_col=0).loc[:, 1]
    api_creds = SpotifyClientCredentials(client_id=auth_data.loc['client_id'], client_secret=auth_data.loc['client_secret'])
    api = Spotify(client_credentials_manager=api_creds)
    out_dir = args['out_dir']
    artist_id_data_file = os.path.join(out_dir, f'{artist_data_file_base}_spotify_data.tsv')
    if(not os.path.exists(artist_id_data_file)):
        artist_names = artist_data.loc[:, 'clean_name'].unique()
        artist_id_data = collect_all_artist_data(artist_names, api)
        artist_id_data = artist_id_data.assign(**{
            'clean_name' : artist_id_data.loc[:, 'name'].apply(clean_txt)
        })
        # drop nan values
        artist_id_data = artist_id_data[~artist_id_data.loc[:, 'id'].apply(lambda x: type(x) is float and np.isnan(x))]
        artist_id_data.to_csv(artist_id_data_file, sep='\t', index=False)
    else:
        artist_id_data = pd.read_csv(artist_id_data_file, sep='\t', index_col=False)

    ## search for related artists
    valid_artist_ids = artist_id_data.loc[:, 'id'].unique()
    similar_artist_data = get_similar_artists_all_ids(valid_artist_ids, api)
    # drop duplicate artists
    similar_artist_data.drop_duplicates('id', inplace=True)
    similar_artist_data = similar_artist_data[~similar_artist_data.loc[:, 'id'].isin(valid_artist_ids)]
    # save to file
    similar_artist_data_file = os.path.join(out_dir, f'{artist_data_file_base}_similar_artists.tsv')
    similar_artist_data.to_csv(similar_artist_data_file, sep='\t', index=False)

if __name__ == '__main__':
    main()