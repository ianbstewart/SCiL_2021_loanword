"""
Compute popularity of hashtags and mentions from
prior tweets of all authors.

Yes this is a biased sample, it's better than nothing.
"""
from argparse import ArgumentParser
import logging
from data_helpers import load_data_from_dirs
import re
import os
import pandas as pd
import numpy as np
from functools import reduce

def generate_counts(item_lists):
    items = []
    for item_list in item_lists:
        items += item_list
    counts = pd.Series(items).value_counts()
    return counts

def bin_counts(counts, count_pct_bins=[50], count_pct_bin_names=[]):
    counts = np.log(counts)
    count_bins = list(map(lambda x: np.percentile(counts, x), count_pct_bins))
    counts = pd.DataFrame(counts).reset_index()
    print('binning counts with bins %s'%(str(count_bins)))
    print('pre-bin counts %s'%(counts.head(10)))
#     print(counts.head(5))
    counts = counts.assign(**{
        'freq_category' : np.digitize(counts.loc[:, 0], count_bins)
    })
    print('bin counts = %s'%(counts.loc[:, 'freq_category'].value_counts()))
    # rename categories
    if(len(count_pct_bin_names) > 0):
        counts = counts.assign(**{
            'freq_category' : counts.loc[:, 'freq_category'].apply(lambda x: count_pct_bin_names[int(x)])
    })
    print('category counts = %s'%(counts.loc[:, 'freq_category'].value_counts()))
    return counts

def main():
    parser = ArgumentParser()
    parser.add_argument('data_dir')
    parser.add_argument('--out_dir', default='../../data/mined_tweets/')
    args = vars(parser.parse_args())
    logging_file = '../../output/compute_hashtag_mention_popularity_from_prior_tweets.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    ## load all prior tweets
    data_dir = args['data_dir']
    use_cols = ['text']
    combined_data = load_data_from_dirs([data_dir], use_cols=use_cols)
    print(combined_data.head(5))
    # clean text
    text_var = 'text'
    clean_text_var = f'clean_{text_var}'
    combined_data = combined_data[combined_data.loc[:, text_var].apply(lambda x: type(x) is str)]
    combined_data = combined_data.assign(**{
        clean_text_var : combined_data.loc[:, text_var].apply(lambda x: x.lower())
    })
#     print(combined_data.shape[0])
    
    ## identify hashtags, @-mentions
    # remove RTs
    RT_matcher = re.compile('^RT @[a-z0-9_]{3,}:')
    combined_data = combined_data[combined_data.loc[:, text_var].apply(lambda x: RT_matcher.search(x) is None)]
    hashtag_matcher = re.compile('#[a-z0-9_]{3,}')
    mention_matcher = re.compile('@[a-z0-9_]{3,}')
    combined_data = combined_data.assign(**{
        'hashtags' : combined_data.loc[:, clean_text_var].apply(lambda x: hashtag_matcher.findall(x)),
        'mentions' : combined_data.loc[:, clean_text_var].apply(lambda x: mention_matcher.findall(x))
    })
    
    ## compute hashtag, mention frequency
    hashtag_counts = generate_counts(combined_data.loc[:, 'hashtags'])
    mention_counts = generate_counts(combined_data.loc[:, 'mentions'])
    # label with high/low frequency based on threshold above midpoint
    count_pct_bins = [90]
#     count_pct_bin_names = ['low', 'high']
    hashtag_counts = bin_counts(hashtag_counts, count_pct_bins=count_pct_bins).rename(columns={'index' : 'hashtag', 0 : 'freq'})
    mention_counts = bin_counts(mention_counts, count_pct_bins=count_pct_bins).rename(columns={'index' : 'mention', 0 : 'freq'})
    print(mention_counts.head(5))
    print(mention_counts.loc[:, 'freq_category'].value_counts())
    
    ## save to file
    out_file_base = os.path.basename(os.path.dirname(data_dir))
    out_dir = args['out_dir']
    hashtag_out_file_name = os.path.join(out_dir, f'{out_file_base}_hashtag_freq.tsv')
    mention_out_file_name = os.path.join(out_dir, f'{out_file_base}_mention_freq.tsv')
    hashtag_counts.to_csv(hashtag_out_file_name, sep='\t', index=False)
    mention_counts.to_csv(mention_out_file_name, sep='\t', index=False)
    
if __name__ == '__main__':
    main()