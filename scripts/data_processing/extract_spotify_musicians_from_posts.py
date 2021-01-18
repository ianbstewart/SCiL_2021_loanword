"""
Extract Spotify musicians from author data
and query Spotify for musician data, e.g. genres.
"""
from argparse import ArgumentParser
import logging
import os
import re
import pandas as pd
from spotipy import Spotify, SpotifyClientCredentials
from functools import reduce
from ast import literal_eval

URL_MATCHER=re.compile('')
def extract_URL_matches(data_file, URL_matcher):
    """
    Extract all matching URLs from file.
    """
    URL_matches = []
    post_data = pd.read_csv(data_file, sep='\t', index_col=False, compression='gzip')
    post_data.fillna('', inplace=True)
    url_var = 'urls'
    # add URL var UGH
    if(url_var not in post_data.columns):
        post_data = post_data.assign(**{
            url_var : post_data.loc[:, 'text'].apply(lambda x: URL_matcher.search(x).group(0) if URL_matcher.search(x) is not None else '')
        })
    post_URLs = post_data[post_data.loc[:, url_var] != ''].loc[:, url_var].values
    combined_post_URLs = set()
    for post_URL_i in post_URLs:
        # extract list of URLs if needed
        try:
            post_URL_i = literal_eval(post_URL_i)
        # otherwise treat single URL as list
        except Exception as e:
            post_URL_i = [post_URL_i]
            pass
        post_URLs_i = set(post_URL_i)
        combined_post_URLs.update(post_URLs_i)
    URL_matches = list(filter(lambda x: URL_matcher.search(x) is not None, combined_post_URLs))
    return URL_matches

def collect_track_data(track_id, spotify_api):
    """
    Collect data for track from API.
    
    TODO: force sleep time to avoid rate limiting
    """
    # organize
    track_result_data = {}
    # track data
    track_keys = ['id', 'explicit', 'name']
    try:
        track_results = spotify_api.track(track_id)
        for k in track_keys:
            track_result_data[k] = track_results[k]
        # album data
        album_keys = ['id', 'name', 'release_date']
        album_data = track_results['album']
        for k in album_keys:
            track_result_data[f'album_{k}'] = album_data[k]
        # available markets
        track_result_data['album_available_markets'] = len(album_data['available_markets'])
        # artist data => all artists ;_;
        artist_keys = ['id', 'name']
        for k in artist_keys:
            track_result_data[f'artist_{k}'] = []
        artist_data = track_results['artists']
        for artist_data_i in artist_data:
            for k in artist_keys:
                track_result_data[f'artist_{k}'].append(artist_data_i[k])
    except Exception as e:
        print(f'ID {track_id} has exception {e}')
    return track_result_data

def collect_artist_data(artist_id, spotify_api):
    # collect artist data from Spotify API
    artist_data = {}
    data_keys = ['genres', 'id', 'name', 'popularity']
    try:
        artist_query_res = spotify_api.artist(artist_id=artist_id)
        artist_data['followers'] = artist_query_res['followers']['total']
        artist_data.update({k : artist_query_res[k] for k in data_keys})
    except Exception as e:
        print(f'bad ID {artist_id} yields error:\n{e}')
    artist_data['artist_id'] = artist_data['id']
    artist_data.pop('id')
    return artist_data

def main():
    parser = ArgumentParser()
    parser.add_argument('post_data_dirs', nargs='+')
    parser.add_argument('--music_API_auth_data', default='../../data/culture_metadata/spotify_auth.csv')
    parser.add_argument('--out_dir', default='../../data/culture_metadata/')
    args = vars(parser.parse_args())
    logging_file = '../../output/extract_spotify_musicians_from_posts.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    ## load post data
    ## and extract URLs
    post_data_dirs = args['post_data_dirs']
    file_matcher = re.compile('tweets.gz')
    music_URL_matcher = re.compile(r'(?<=spotify\.com/track/)[a-zA-Z0-9]+')
    combined_URL_matches = set()
    for post_data_dir in post_data_dirs:
        post_data_files = list(filter(lambda x: file_matcher.search(x) is not None, os.listdir(post_data_dir)))
        post_data_files = list(map(lambda x: os.path.join(post_data_dir, x), post_data_files))
        for post_data_file in post_data_files:
            URL_matches = extract_URL_matches(post_data_file, music_URL_matcher)
            combined_URL_matches.update(set(URL_matches))
    combined_URL_matches = list(combined_URL_matches)
    print('%d total URL matches'%(len(combined_URL_matches)))
    # extract music-related links
    track_IDs = set()
    for post_URL in combined_URL_matches:
        music_URL_match = music_URL_matcher.search(post_URL)
        track_ID = music_URL_match.group(0)
        track_IDs.update([track_ID])
    track_IDs = list(track_IDs)
    # clean up IDs
    bad_ID_matcher = re.compile('http[s:]{,}$')
    track_IDs = list(map(lambda x: bad_ID_matcher.sub('', x), track_IDs))
    print('%d music track IDs'%(len(track_IDs)))
    
    ## mine track data
    # set up API
    music_API_auth_data_file = args['music_API_auth_data']
    spotify_auth_data = pd.read_csv(music_API_auth_data_file, sep=',', header=None)
    spotify_auth_data = dict(zip(spotify_auth_data.loc[:, 0], spotify_auth_data.loc[:, 1]))
    spotify_creds = SpotifyClientCredentials(client_id=spotify_auth_data['client_id'], client_secret=spotify_auth_data['client_secret'])
    spotify_api = Spotify(client_credentials_manager=spotify_creds)
    out_dir = args['out_dir']
    track_data_file = os.path.join(out_dir, 'spotify_track_data.tsv')
    if(os.path.exists(track_data_file)):
        old_track_data = pd.read_csv(track_data_file, sep='\t', index_col=False)
        old_track_data.fillna('', inplace=True)
        old_track_data = old_track_data[old_track_data.loc[:, 'artist_id'] != '']
        for i in old_track_data.loc[:, 'artist_id'].values:
            try:
                literal_eval(i)
            except Exception as e:
                print('bad artist IDs %s'%(i))
                break
        old_track_data = old_track_data.assign(**{
            'artist_id' : old_track_data.loc[:, 'artist_id'].apply(lambda x: literal_eval(x))
        })
        old_track_IDs = old_track_data.loc[:, 'id'].values
        track_IDs = list(set(track_IDs) - set(old_track_IDs))
    else:
        old_track_data = []
    track_data = list(map(lambda x: collect_track_data(x, spotify_api), track_IDs))
    print('%d track IDs'%(len(track_data)))
    track_data = pd.concat(list(map(lambda x: pd.Series(x), track_data)), axis=1).transpose()
    if(len(old_track_data) > 0):
        track_data = pd.concat([old_track_data, track_data], axis=0)
    # write to file
    track_data.to_csv(track_data_file, sep='\t', index=False)
    
    ## mine musician data
    # get artist IDs
    track_data = track_data[track_data.loc[:, 'artist_id'].apply(lambda x: type(x) is list)]
    flat_musician_IDs = list(reduce(lambda x,y: x+y, track_data.loc[:, 'artist_id'].values))
    musician_IDs = list(set(flat_musician_IDs))
    print('%d musician IDs'%(len(musician_IDs)))
    musician_data_file = os.path.join(out_dir, 'spotify_musician_data.tsv')
    if(os.path.exists(musician_data_file)):
        old_musician_data = pd.read_csv(musician_data_file, sep='\t', index_col=False)
        old_musician_IDs = set(old_musician_data.loc[:, 'artist_id'].unique())
        musician_IDs = list(set(musician_IDs) - old_musician_IDs)
    else:
        old_musician_data = []
    musician_data = list(map(lambda x: collect_artist_data(x, spotify_api), musician_IDs))
    musician_data = pd.concat(list(map(lambda x: pd.Series(x), musician_data)), axis=1).transpose()
    if(len(old_musician_data) > 0):
        musician_data = pd.concat([old_musician_data, musician_data], axis=0)
    # write to file
    musician_data.to_csv(musician_data_file, sep='\t', index=False)
    
if __name__ == '__main__':
    main()