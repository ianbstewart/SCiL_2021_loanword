"""
Combine media sharing data and balance
on the likely audience distribution.
Based on testing in this notebook: balance_musicians_age_distribution.ipynb
"""
from argparse import ArgumentParser
import logging
from data_helpers import try_literal_eval, get_matching_pairs
import pandas as pd
from ast import literal_eval
import os
import numpy as np

def assign_audience_pct(interests, audience_pct_lookup, audience_pct_count=4):
    """
    Assign aggregate audience percent to list of interests.
    """
    interest_pcts = list(map(audience_pct_lookup.get, interests))
    interest_pcts = list(filter(lambda x: x is not None, interest_pcts))
    mean_audience_pct = np.zeros(audience_pct_count)
    # compute mean over all interests
    if(len(interest_pcts) > 0):
        interest_pcts = list(map(lambda x: x.reshape(-1,1), interest_pcts))
        audience_pcts = np.concatenate(interest_pcts, axis=1).T
        mean_audience_pct = np.mean(audience_pcts, axis=0)
    return mean_audience_pct

def assign_genre_type(x, genre_types=['latin_american_music','us_american_music']):
    """
    Assign genre type from true/false columns.
    """
    for genre_type in genre_types:
        if(x.loc[genre_type]):
            return genre_type
    return ''

def collect_all_media_counts(data):
    us_american_media_data = data[data.loc[:, 'media_genre']=='us_american_music']
    latin_american_media_data = data[data.loc[:, 'media_genre']=='latin_american_music']
    video_data = data[data.loc[:, 'media_type']=='video']
    music_data = data[data.loc[:, 'media_type']=='music']
    # video data
    video_count = video_data.shape[0]
    latin_american_video_count = len(latin_american_media_data.index & video_data.index)
    us_american_video_count = len(us_american_media_data.index & video_data.index)
    if(video_count > 0):
        latin_american_video_pct = latin_american_video_count / video_count
    else:
        latin_american_video_pct = 0
    # music data
    music_count = music_data.shape[0]
    latin_american_music_count = len(latin_american_media_data.index & music_data.index)
    us_american_music_count = len(us_american_media_data.index & music_data.index)
    if(music_count > 0):
        latin_american_music_pct = latin_american_music_count / music_count
    else:
        latin_american_music_pct = 0
    # media data
    media_count = data.shape[0]
    us_american_media_count = us_american_media_data.shape[0]
    latin_american_media_count = latin_american_media_data.shape[0]
    latin_american_media_pct = latin_american_media_count / media_count
    media_count_data = [video_count, latin_american_video_count, us_american_video_count, latin_american_video_pct, music_count, latin_american_music_count, us_american_music_count, latin_american_music_pct, media_count, latin_american_media_count, us_american_media_count, latin_american_media_pct]
    media_count_data_names = ['total_video_count', 'latin_american_artist_video_count', 'us_american_artist_video_count', 'latin_american_artist_video_pct', 'total_music_count', 'latin_american_music_genre_count', 'us_american_music_genre_count', 'latin_american_music_genre_pct', 'media_count','latin_american_media_count', 'us_american_media_count', 'latin_american_media_pct']
    media_count_data = pd.Series(media_count_data, index=media_count_data_names)
    return media_count_data

def main():
    parser = ArgumentParser()
    parser.add_argument('--audience_data', default='../../data/culture_metadata/audience_size_FB_estimates.tsv')
    parser.add_argument('--music_artist_meta_data', default='../../data/culture_metadata/spotify_musician_data.tsv')
    parser.add_argument('--music_song_meta_data', default='../../data/culture_metadata/spotify_track_data.tsv')
    parser.add_argument('--music_share_data', default='../../data/mined_tweets/loanword_author_tweets_all_archives_author_spotify_link_data.tsv')
    parser.add_argument('--video_share_data', default='../../data/mined_tweets/loanword_author_tweets_all_archives_youtube_video_data_full.tsv')
    parser.add_argument('--video_meta_data', default='../../data/culture_metadata/youtube_video_music_genre_data.tsv')
    parser.add_argument('--data_name', default='loanword_author_tweets_all_archives')
    parser.add_argument('--out_dir', default='../../data/mined_tweets/')
#     parser.add_argument('music_link_share_data') #
#     parser.add_argument('music_meta_data') # 
    args = vars(parser.parse_args())
    logging_file = '../../output/combine_balance_author_media_sharing_data.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    ## load audience data
    audience_data = pd.read_csv(args['audience_data'], sep='\t', converters={'age_range' : literal_eval})
    # fix interest name
    interest_name_var = 'interest_name'
    audience_data = audience_data.assign(**{
        interest_name_var : audience_data.loc[:, interest_name_var].apply(lambda x: x.lower())
    })
    # compute percents
    audience_pct_data = []
    audience_count_var = 'estimate_mau'
    audience_pct_var = f'{audience_count_var}_pct'
    for interest_i, data_i in audience_data.groupby(interest_name_var):
        pct_i = data_i.loc[:, audience_count_var] / data_i.loc[:, audience_count_var].sum()
        data_i = data_i.assign(**{
            audience_pct_var : pct_i
        })
        audience_pct_data.append(data_i)
    audience_data = pd.concat(audience_pct_data, axis=0)
    audience_age_categories = audience_data.loc[:, 'age_range'].unique()
    # data should be organized by link
    # link | valid tags | 15_25_pct | 25_35_pct | etc.
    audience_pct_lookup = audience_data.groupby(interest_name_var).apply(lambda x: x.loc[:, audience_pct_var].values).to_dict()
    
    ## load music and video data
    # load music data
    music_artist_data = pd.read_csv(args['music_artist_meta_data'], sep='\t', converters={'genres' : try_literal_eval})
    music_artist_data = music_artist_data.assign(**{
        'name_clean' : music_artist_data.loc[:, 'name'].apply(lambda x: x.lower())
    })
    music_song_data = pd.read_csv(args['music_song_meta_data'], sep='\t', converters={'artist_name' : try_literal_eval})
    music_song_data = music_song_data.assign(**{
        'artist_name_clean' : music_song_data.loc[:, 'artist_name'].apply(lambda x: list(map(lambda y: y.lower(), x)))
    })
    music_song_data.rename(columns={'id' : 'track_id'}, inplace=True)
    music_artist_name_var = 'artist_name_clean'
    audience_music_category_data = music_song_data[music_song_data.loc[:, music_artist_name_var].apply(lambda x: len(set(x) & set(audience_pct_lookup.keys())) > 0)]
    audience_music_category_data = audience_music_category_data.loc[:, ['track_id', 'name', 'artist_name_clean']]
    music_audience_pct_means = music_song_data.loc[:, music_artist_name_var].apply(lambda x: assign_audience_pct(x, audience_pct_lookup, len(audience_age_categories)))
    # reorganize
    for i, age_category_i in enumerate(audience_age_categories):
        age_category_i_str = 'AGE={%s_%s}_pct'%(age_category_i[0], age_category_i[1])
        audience_music_category_data = audience_music_category_data.assign(**{
            age_category_i_str : music_audience_pct_means.apply(lambda x: x[i])
        })
    music_share_data = pd.read_csv(args['music_share_data'], sep='\t')
    author_music_link_category_data = pd.merge(music_share_data, audience_music_category_data, on='track_id', how='inner')
    author_music_link_category_data.rename(columns={'latin_american_genre' : 'latin_american_music', 
                                                      'us_american_genre' : 'us_american_music'}, inplace=True)
    # we need a single genre column
    author_music_link_category_data = author_music_link_category_data.assign(**{
        'media_genre' : author_music_link_category_data.apply(lambda x: assign_genre_type(x), axis=1),
        'media_type' : 'music',
    })
    author_music_link_category_data.rename(columns={'track_id' : 'media_id'}, inplace=True)
    logging.info('%d/%d music data'%(author_music_link_category_data.shape[0], music_share_data.shape[0]))
    logging.info('%d Latin American tracks'%(author_music_link_category_data.loc[:, 'latin_american_music'].sum()))
    logging.info('%d US American tracks'%(author_music_link_category_data.loc[:, 'us_american_music'].sum()))
    # import pandas as pd
    # # load video sharing data
    video_share_data = pd.read_csv(args['video_share_data'], sep='\t')
    # # load video metadata
    video_category_data = pd.read_csv(args['video_meta_data'], sep='\t', 
                                      converters={'us_american_artist_tags' : try_literal_eval, 'latin_american_artist_tags' : try_literal_eval})
    # combine all artist tags
    video_category_data = video_category_data.assign(**{
        'combined_artist_tags' : video_category_data.loc[:, ['us_american_artist_tags', 'latin_american_artist_tags']].apply(lambda x: set(x[0]) | set(x[1]), axis=1)
    })
    combined_tag_var = 'combined_artist_tags'
    audience_video_category_data = video_category_data[video_category_data.loc[:, combined_tag_var].apply(lambda x: len(x) > 0 and len(set(x) & set(audience_pct_lookup.keys())) > 0)]
    audience_video_category_data = audience_video_category_data.loc[:, ['youtube_id', 'video_genre', 'combined_artist_tags']]
    audience_pct_means = audience_video_category_data.loc[:, combined_tag_var].apply(lambda x: assign_audience_pct(x, audience_pct_lookup, len(audience_age_categories)))
    # reorganize
    for i, age_category_i in enumerate(audience_age_categories):
        age_category_i_str = 'AGE={%s_%s}_pct'%(age_category_i[0], age_category_i[1])
        audience_video_category_data = audience_video_category_data.assign(**{
            age_category_i_str : audience_pct_means.apply(lambda x: x[i])
        })
    audience_video_category_data.rename(columns={'video_genre' : 'media_genre', 'youtube_id' : 'media_id'}, inplace=True)
    audience_video_category_data = audience_video_category_data.assign(**{
        'media_type' : 'video'
    })
    # combine music and video data
    audience_pct_vars = list(filter(lambda x: x.endswith('_pct'), audience_video_category_data.columns))
    media_vars = ['media_id', 'media_genre', 'media_type'] + audience_pct_vars
    valid_author_music_link_category_data = author_music_link_category_data[author_music_link_category_data.loc[:, 'media_genre']!='']
    media_audience_data = pd.concat([audience_video_category_data.loc[:, media_vars], 
                                     valid_author_music_link_category_data.loc[:, media_vars]], axis=0)
    logging.info('%d music data + %d video data = %d valid media data'%(audience_video_category_data.shape[0], valid_author_music_link_category_data.shape[0], media_audience_data.shape[0]))
    logging.info('valid media genre counts\n%s'%(media_audience_data.loc[:, 'media_genre'].value_counts()))
    
    ## balance media
    latin_american_media_audience_data = media_audience_data[media_audience_data.loc[:, 'media_genre']=='latin_american_music']
    us_american_media_audience_data = media_audience_data[media_audience_data.loc[:, 'media_genre']=='us_american_music']
    latin_american_media_age_category_pcts = latin_american_media_audience_data.loc[:, audience_pct_vars].values
    us_american_media_age_category_pcts = us_american_media_audience_data.loc[:, audience_pct_vars].values
    treated_df = pd.DataFrame(latin_american_media_age_category_pcts, index=latin_american_media_audience_data.index)
    control_df = pd.DataFrame(us_american_media_age_category_pcts, index=us_american_media_audience_data.index)
    match_df = get_matching_pairs(treated_df, control_df, scaler=True)
    # map these back to the original data
    matched_media_audience_data = [
        latin_american_media_audience_data.loc[treated_df.index, :],
        us_american_media_audience_data.loc[match_df.index, :]
    ]
    matched_media_audience_data = pd.concat(matched_media_audience_data, axis=0)
    logging.info('matched media genre counts\n%s'%(str(matched_media_audience_data.loc[:, 'media_genre'].value_counts())))
    
    ## write sharing data to file
    # we need Latin American media percent as a variable
    # author | valid media count | valid Latin American media count | valid US American media count | media percent
    # combine sharing data
    video_share_data.rename(columns={'youtube_id' : 'media_id'}, inplace=True)
    music_share_data.rename(columns={'track_id' : 'media_id'}, inplace=True)
    video_share_data = video_share_data.assign(**{'media_type' : 'video'})
    music_share_data = music_share_data.assign(**{'media_type' : 'music'})
    media_share_data = pd.concat([video_share_data, music_share_data], axis=0)
    media_share_data = media_share_data.loc[:, ['media_id', 'media_type', 'screen_name']]
    # fix author name
    author_var = 'screen_name'
    media_share_data = media_share_data.assign(**{
        author_var : media_share_data.loc[:, author_var].apply(lambda x: x.lower())
    })
    # tmp debugging
#     media_share_data.to_csv('../../data/mined_tweets/author_media_sharing_tmp.tsv', sep='\t')
    # add genre data
    logging.info('combined media share types:\n%s'%(str(media_share_data.loc[:, 'media_type'].value_counts())))
    matched_media_ids = set(matched_media_audience_data.loc[:, 'media_id'].unique())
    valid_media_share_data = media_share_data[media_share_data.loc[:, 'media_id'].isin(matched_media_ids)]
    # add genre type
    media_id_genre_lookup = dict(zip(matched_media_audience_data.loc[:, 'media_id'].values, matched_media_audience_data.loc[:, 'media_genre']))
    valid_media_share_data = valid_media_share_data.assign(**{
        'media_genre' : valid_media_share_data.loc[:, 'media_id'].apply(media_id_genre_lookup.get)
    })
    logging.info('valid media share data\n%s'%(str(valid_media_share_data.head())))
    # compute percent sharing per author
    author_var = 'screen_name'
    author_media_sharing_pct = valid_media_share_data.groupby(author_var).apply(lambda x: collect_all_media_counts(x)).reset_index()
    # add total media counts: all media, superset of Latin American, US American media
    author_total_media_type_counts = media_share_data.groupby(author_var).apply(lambda x: x.loc[:, 'media_type'].value_counts()).reset_index().rename(columns={'level_1':'media_type', 'media_type':'media_count'})
    author_total_media_type_counts = author_total_media_type_counts.pivot(index='screen_name', columns='media_type', values='media_count').reset_index().rename(columns={'music' : 'total_music_count', 'video' : 'total_video_count'}).fillna(0, inplace=False)
    author_total_media_type_counts = author_total_media_type_counts.assign(**{
        'media_count' : author_total_media_type_counts.loc[:, ['total_music_count', 'total_video_count']].sum(axis=1)
    })
    # combine with old data
    author_media_sharing_pct.drop(['total_music_count', 'total_video_count', 'media_count'], axis=1, inplace=True)
    author_media_sharing_pct = pd.merge(author_media_sharing_pct, author_total_media_type_counts, on='screen_name').fillna(0., inplace=False)
    # add "other" category
    author_media_sharing_pct = author_media_sharing_pct.assign(**{
        'other_media_count' : author_media_sharing_pct.loc[:, 'media_count'] - author_media_sharing_pct.loc[:, ['latin_american_media_count', 'us_american_media_count']].sum(axis=1)
    })
    
    ## save to file...whew!
    out_dir = args['out_dir']
    data_name = args['data_name']
    out_file = os.path.join(out_dir, f'{data_name}_author_media_sharing_balanced.tsv')
    author_media_sharing_pct.to_csv(out_file, sep='\t', index=False)

if __name__ == '__main__':
    main()