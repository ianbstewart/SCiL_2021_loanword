"""
Extract YouTube video data from posts
using Google API to query.
"""
from argparse import ArgumentParser
import logging
import os
from data_helpers import load_data_from_dirs, load_data_manual, AccessTokenHandler
import pandas as pd
import re
import googleapiclient.discovery
from math import ceil
from time import sleep

def query_youtube_videos(video_ids, access_token_handler, video_search_params=['snippet'], drop_keys=['thumbnails', 'localized'], verbose=False):
    """
    Query API from YouTube for video information.
    """
    api_service_name = "youtube"
    api_version = "v3"
    response_data = []
    max_chunk_size = 10
    N = len(video_ids)
    video_id_chunks = list(map(lambda x: video_ids[(x*max_chunk_size):((x+1)*max_chunk_size)], range(int(ceil(N / max_chunk_size)))))
    query_sleep_time = 60
    max_try_ctr = 5
    RATE_LIMIT_ERROR_CODE=403
    for video_id_chunk in video_id_chunks:
        if(access_token_handler.get_curr_token_rate_limited()):
            access_token_handler.next_token()
        if(access_token_handler.get_all_token_rate_limited()):
            logging.info('all tokens are rate limited, ending collection now')
            break
        api_key = access_token_handler.get_access_token_curr()
        youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
        request = youtube.videos().list(
            part=','.join(video_search_params),
            id=','.join(video_id_chunk)
        )
        if(verbose):
            print(f'searching for IDs {video_id_chunk}')
        # default response
        response = {'items' : []}
        success = False
        try_ctr = 0
        while(not success and try_ctr < max_try_ctr):
            try:
                response = request.execute()
                success = True
                # reset rate limiting info if already rate-limited
                if(access_token_handler.get_curr_token_rate_limited()):
                    access_token_handler.set_curr_token_rate_limited(False)
            except Exception as e:
                logging.info(f'failed mining because error {e}')
#                 print('error %s has attributes %s'%(e, dir(e)))
                logging.info(f'try={try_ctr}/{max_try_ctr}')
                try_ctr += 1
                # update rate limit info
                # TODO: how to determine whether error has rate limit code ;_;
#                 if(e.code == RATE_LIMIT_ERROR_CODE):
                access_token_handler.set_curr_token_rate_limited(True)
                sleep(query_sleep_time)
        try:
            for response_item in response['items']:
                if(verbose):
                    print(f'processing item {response_item}')
                data_i = []
                video_id = response_item['id']
                for video_search_param in video_search_params:
                    if(video_search_param in response_item.keys()):
                        data_i.append(pd.Series(response_item[video_search_param]))
                data_i = pd.concat(data_i, axis=0)
                data_i.loc['id'] = video_id
                response_data.append(data_i)
        except Exception as e:
            logging.info(f'ending collection early because error {e}')
            break
        if(len(response_data) % 100 == 0):
            logging.info('collected %d video data'%(len(response_data)))
        if(try_ctr == max_try_ctr):
            logging.info(f'ending collection early because timeout with try counters')
            break
    if(verbose):
        print('collected %d data'%(len(response_data)))
    response_data = pd.concat(response_data, axis=1).transpose()
    drop_keys = list(set(drop_keys) & set(response_data.columns))
    response_data.drop(drop_keys, axis=1, inplace=True)
    # clean text data
    txt_vars = ['description']
    for txt_var in txt_vars:
        response_data = response_data.assign(**{
            txt_var : response_data.loc[:, txt_var].apply(lambda x: x.replace('\n', ''))
        })
    return response_data

def main():
    parser = ArgumentParser()
    parser.add_argument('data_dir')
    parser.add_argument('--auth_data', default='../../data/culture_metadata/google_api_auth.csv')
    parser.add_argument('--youtube_video_data', default='../../data/culture_metadata/youtube_video_data.tsv')
    args = vars(parser.parse_args())
    logging_file = '../../output/extract_youtube_video_data_from_posts.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    ## load data
    data_dir = args['data_dir']
    file_matcher = re.compile('.*tweets\.gz')
    post_data = load_data_from_dirs([data_dir], file_matcher=file_matcher)
    post_data.fillna('', inplace=True)
    # restrict to valid data
    url_var = 'urls'
    post_data = post_data[post_data.loc[:, url_var] != '']
        
    ## extract URLs
    youtube_media_matcher = re.compile('(?<=youtube\.com/watch\?v=)[a-zA-Z0-9_\-]+|(?<=youtu\.be/)[a-zA-Z0-9_\-]+')
    post_data = post_data.assign(**{
        'youtube_id' : post_data.loc[:, 'urls'].apply(lambda x: youtube_media_matcher.search(x).group(0) if youtube_media_matcher.search(x) is not None else '')
    })
    # remove invalid videos
    post_data = post_data[post_data.loc[:, 'youtube_id'] != '']
    youtube_video_id_counts = post_data.loc[:, 'youtube_id'].value_counts()
    youtube_video_ids = list(post_data.loc[:, 'youtube_id'].unique())
    logging.info('%d YouTube IDs, %d total mentions'%(len(youtube_video_ids), post_data.shape[0]))
    # tmp debugging
#     youtube_video_ids = youtube_video_ids[:10000]
    
    ## query URLs for metadata
    youtube_video_data_file = args['youtube_video_data']
    if(os.path.exists(youtube_video_data_file)):
#         old_youtube_video_data = pd.read_csv(youtube_video_data_file, sep='\t')
        # malformed data??
#         old_youtube_video_data = load_data_manual(youtube_video_data_file)
        old_youtube_video_data = load_data_manual(youtube_video_data_file, max_cols=11, verbose=False, pad_val='')
        old_youtube_video_data.drop_duplicates('id', inplace=True)
    else:
        old_youtube_video_data = []
    logging.info('%d old YouTube video data'%(len(old_youtube_video_data)))
    if(len(old_youtube_video_data) > 0):
        youtube_video_ids = list(set(youtube_video_ids) - set(old_youtube_video_data.loc[:, 'id'].unique()))
    ## TODO: why are we overwriting the old data??
    auth_data_file = args['auth_data']
    auth_data = pd.read_csv(auth_data_file, sep=',')
    # TODO: key handler for multiple keys in case of rate limiting
#     api_key = auth_data.iloc[3, :].loc['api_key']
    access_token_handler = AccessTokenHandler(auth_data.loc[:, 'api_key'].values, auth_data.loc[:, 'app'].values)
    video_search_params = ['snippet', 'topicDetails']
    youtube_video_data = query_youtube_videos(youtube_video_ids, access_token_handler, video_search_params=video_search_params, verbose=False)
    logging.info('collected %d total video data'%(youtube_video_data.shape[0]))
        
    ## save data
    if(len(old_youtube_video_data) > 0):
        youtube_video_data = pd.concat([old_youtube_video_data, youtube_video_data], axis=0)
        logging.info('%d combined YouTube video data'%(youtube_video_data.shape[0]))
    youtube_video_data.to_csv(youtube_video_data_file, sep='\t', index=False)

if __name__ == '__main__':
    main()