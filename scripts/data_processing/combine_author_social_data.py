"""
Combine social data in authors for regression:
author | location | age | lang use | media sharing | light verb use (binary)
"""
from argparse import ArgumentParser
import logging
import os
import numpy as np
import pandas as pd
from data_helpers import bin_data_var

def main():
    parser = ArgumentParser()
    parser.add_argument('location_data') # ../../data/mined_tweets/loanword_author_tweets/loanword_author_descriptive_data_location_data.tsv
    parser.add_argument('lang_data') # ../../data/mined_tweets/loanword_author_tweets_LANG=es_pct.tsv 
    parser.add_argument('media_data') # ../../data/mined_tweets/loanword_author_tweets_author_media_sharing.tsv
    parser.add_argument('native_verb_data') # ../../data/mined_tweets/loanword_author_tweets/native_integrated_light_verbs_per_author.tsv
    parser.add_argument('activity_data') # ../../data/mined_tweets/loanword_author_activity_data.tsv
    parser.add_argument('--balanced_media_data', default='../../data/mined_tweets/loanword_author_tweets_all_archives_author_media_sharing_balanced.tsv')
    parser.add_argument('--author_var', default='screen_name')
    parser.add_argument('--out_dir', default='../../data/mined_tweets')
    parser.add_argument('--file_name', default='loanword_authors_combined')
    args = vars(parser.parse_args())
    logging_file = '../../output/combine_author_social_data.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    ## load, clean data
    location_data = pd.read_csv(args['location_data'], sep='\t')
    lang_data = pd.read_csv(args['lang_data'], sep='\t')
    media_data = pd.read_csv(args['media_data'], sep='\t')
    native_verb_data = pd.read_csv(args['native_verb_data'], sep='\t')
    activity_data = pd.read_csv(args['activity_data'], sep='\t')
    if(args.get('balanced_media_data') is not None):
        balanced_media_data = pd.read_csv(args['balanced_media_data'], sep='\t')
        balanced_media_data.rename(columns={'latin_american_media_pct' : 'balanced_latin_american_media_pct'}, inplace=True)
    else:
        balanced_media_data = None
    # clean locations
    location_data.rename(columns={'user_screen_name' : 'screen_name'}, inplace=True)
    # add bin for langs
#     bin_ranges = [0.25, 0.75]
#     bin_names = {0 : 'es_low', 1 : 'es_mid', 2 : 'es_high'}
#     lang_var = 'es'
#     lang_data = bin_data_var(lang_data, bin_ranges, bin_names, bin_var=lang_var)
    # clean native verb data
    native_verb_data.fillna('', inplace=True)
    author_var = args['author_var']
    native_verb_data = native_verb_data[native_verb_data.loc[:, author_var]!='']
    # weighted average over all verbs
    native_verb_data = native_verb_data.groupby(author_var).apply(lambda x: x.loc[:, 'integrated_verb_pct'].mean()).reset_index().rename(columns={0:'integrated_verb_pct'})
    
    ## clean all data
    author_var = 'screen_name'
    all_author_data = [location_data, lang_data, media_data, native_verb_data, activity_data]
    data_vars = ['description_location_region', 'es', 'latin_american_media_pct', 'integrated_verb_pct', 'post_pct']
    if(balanced_media_data is not None):
        all_author_data.append(balanced_media_data)
        data_vars.append('balanced_latin_american_media_pct')
    clean_author_data = []
    for data, data_var in zip(all_author_data, data_vars):
        data.fillna('', inplace=True)
        data = data[data.loc[:, data_var] != '']
        data = data[data.loc[:, author_var] != '']
        data.drop_duplicates(author_var, inplace=True)
        data = data.assign(**{
            author_var : data.loc[:, author_var].apply(lambda x: x.lower())
        })
        clean_author_data.append(data)
    # combine into per-author data
    combined_data = []
    author_var = 'screen_name'
    clean_data_var_lists = [
        ['description_location_region'], # location
        ['es'],  # lang
        ['total_video_count', 'latin_american_artist_video_count', 'us_american_artist_video_count', 'latin_american_artist_video_pct', 'total_music_count', 'latin_american_music_genre_pct', 'latin_american_music_genre_count', 'us_american_music_genre_count', 'latin_american_media_pct', 'media_URL_pct'], # media
        ['integrated_verb_pct'],
        ['post_pct', 'URL_share_pct', 'RT_pct']
    ]
    if(balanced_media_data is not None):
        clean_data_var_lists.append(['balanced_latin_american_media_pct', 'other_media_count', 'media_count', 'latin_american_media_count', 'us_american_media_count'])
    for data, clean_data_vars in zip(clean_author_data, clean_data_var_lists):
        if(len(combined_data) == 0):
            combined_data = data.loc[:, [author_var]+clean_data_vars].copy()
        else:
#             print('data vars %s'%(str(clean_data_vars)))
#             print('valid cols %s'%(str(set(data.columns) & set(clean_data_vars))))
            combined_data = pd.merge(combined_data, data.loc[:, [author_var]+clean_data_vars], on=author_var, how='outer')
        logging.info('vars=%s, combined data=%d'%(str(clean_data_vars), combined_data.shape[0]))
    combined_data.fillna('', inplace=True)
    # deduplicate??
    logging.info('pre-dedup data %d'%(combined_data.shape[0]))
    combined_data.drop_duplicates(author_var, inplace=True)
    logging.info('post-dedup data %d'%(combined_data.shape[0]))
    
    ## get log values for all scalar variables
    scalar_vars = ['post_pct', 'URL_share_pct', 'RT_pct', 'integrated_verb_pct', 'latin_american_media_pct', 'balanced_latin_american_media_pct', 'es', 'media_URL_pct']
    scalar_vars = list(filter(lambda x: x in combined_data.columns, combined_data))
    smooth_val = 1e-2
    for scalar_var in scalar_vars:
        combined_data = combined_data.assign(**{
                f'log_{scalar_var}' : combined_data.loc[:, scalar_var].apply(lambda x: np.log(x+smooth_val) if type(x) is float else x)
            })
    
    ## bin data
    # lang use, media sharing
    # NOTE: language data is comically skewed such that >75% of authors use 100% Spanish
#     lang_var = 'es'
#     media_pct_var = 'balanced_latin_american_media_pct'
#     lang_var_vals = combined_data[combined_data.loc[:, lang_var] != ''].loc[:, lang_var].values
#     media_pct_var_vals = combined_data[combined_data.loc[:, media_pct_var] != ''].loc[:, media_pct_var].values
#     lang_percentiles = [np.percentile(lang_var_vals, 33), np.percentile(lang_var_vals, 66)]
#     media_pct_percentiles = [np.percentile(media_var_vals, 50), np.percentile(media_pct_var_vals, 75)]
    # tmp debugging
#     print(combined_data.columns)
    percentile_vars = ['latin_american_media_count', 'us_american_media_count', 'other_media_count', 'media_count']
    percentile_var_cutoffs = [[0, 50], [0, 50], [0, 50], [0, 50]]
    percentile_var_cutoff_vals = []
    for percentile_var, percentile_var_cutoff in zip(percentile_vars, percentile_var_cutoffs):
        valid_combined_data = combined_data[combined_data.loc[:, percentile_var]!='']
        valid_percentile_vals = valid_combined_data[valid_combined_data.loc[:, percentile_var] > 0.].loc[:, percentile_var]
        percentile_var_vals = np.percentile(valid_percentile_vals, percentile_var_cutoff).tolist()
        percentile_var_cutoff_vals.append(percentile_var_vals)
    bin_vars = ['es', 'latin_american_media_pct', 'balanced_latin_american_media_pct'] + percentile_vars 
    bin_ranges_list = [
#         lang_percentiles, # lang: percentiles
        [0.50, 0.99], # lang: manual
#         [0.50, 0.9], # lang: manual
        [0.1, 0.5], # unbalanced media: manual
#         media_percentiles, # balanced media: percentiles
        [0.1, 0.5], # balanced media: manual,
    ] + percentile_var_cutoff_vals
    bin_names_list = [
        {0 : 'es_low', 1 : 'es_mid', 2 : 'es_high'},
        {0 : 'media_low', 1 : 'media_mid', 2 : 'media_high'},
        {0 : 'media_low', 1 : 'media_mid', 2 : 'media_high'},
        {0 : 'no_media', 1 : 'media_low', 2 : 'media_high'},
        {0 : 'no_media', 1 : 'media_low', 2 : 'media_high'},
        {0 : 'no_media', 1 : 'media_low', 2 : 'media_high'},
        {0 : 'no_media', 1 : 'media_low', 2 : 'media_high'},
    ]
    for bin_var, bin_ranges, bin_names in zip(bin_vars, bin_ranges_list, bin_names_list):
        combined_data = bin_data_var(combined_data, bin_ranges, bin_names, bin_var=bin_var)
        logging.info('bin var %s has bins %s and distribution: \n%s'%(bin_var, str(bin_ranges), combined_data.loc[:, f'{bin_var}_bin'].value_counts()))
    combined_data.fillna('', inplace=True)
    for bin_var in bin_vars:
        # tmp debugging: missing bins?
        binned_var = f'{bin_var}_bin'
        bad_data = combined_data[(combined_data.loc[:, bin_var]!='') & (combined_data.loc[:, binned_var]=='')]
        if(bad_data.shape[0] > 0):
            print('bad data has bin vals:\n%s'%(bad_data.loc[:, [bin_var, binned_var]]))
            rebinned_vals = np.digitize(bad_data[bad_data.loc[:, bin_var]!=''].loc[:, bin_var].values, bin_ranges)
            print('binned vals = %s'%(str(rebinned_vals)))
    
    ## add "no_media" to count vars
    count_vars = ['latin_american_media_count', 'us_american_media_count', 'other_media_count', 'media_count']
    for count_var in count_vars:
#         print(f'{count_var} has %d NAN vals'%(combined_data[combined_data.loc[:, count_var].apply(lambda x: x=='')].shape[0]))
        bin_count_var = f'{count_var}_bin'
        combined_data = combined_data.assign(**{
            bin_count_var : combined_data.loc[:, bin_count_var].replace('', 'no_media')
        })
    
    ## TODO: compute residuals for correlated variables? to handle confounds
    
    ## save to file
    file_name = args['file_name']
    out_dir = args['out_dir']
    out_file = os.path.join(out_dir, f'{file_name}_full_social_data.tsv')
    combined_data.to_csv(out_file, sep='\t', index=False)
    
if __name__ == '__main__':
    main()