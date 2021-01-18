"""
Extract author activity data:

- daily activity (posts per day)
- re-sharing activity (RT %)
- info sharing activity (URL %)
- account age (time since first recorded post)
"""
from argparse import ArgumentParser
import logging
import os
from data_helpers import load_data_from_dirs
from datetime import datetime
import re
import pandas as pd

def parse_date_all_formats(date_str, date_fmts):
    """
    Try all possible formats to parse date.
    """
    date_parsed = ''
    for date_fmt in date_fmts:
        try:
            date_parsed = datetime.strptime(date_str, date_fmt)
        except Exception as e:
            pass
    return date_parsed

def main():
    parser = ArgumentParser()
    parser.add_argument('data_dir')
    parser.add_argument('--out_dir', default='../../data/mined_tweets/')
    args = vars(parser.parse_args())
    logging_file = '../../output/extract_author_activity_data.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    ## load data
    data_dir = args['data_dir']
    use_cols = set(['id', 'created_at', 'text', 'date', 'screen_name', 'date'])
    use_col_matcher = lambda x: x in use_cols
    file_matcher = re.compile('.*tweets\.gz')
    author_data = load_data_from_dirs([data_dir], use_cols=use_col_matcher, file_matcher=file_matcher)
    author_data.fillna('', inplace=True)
    author_var = 'screen_name'
    author_data = author_data[author_data.loc[:, author_var]!='']
    # remove duplicates
    txt_var = 'text'
    author_data = author_data.drop_duplicates([author_var, txt_var], inplace=False)
    logging.info('loaded %d author data with %d authors'%(author_data.shape[0], author_data.loc[:, author_var].nunique()))
    # convert date var
    date_fmt_1 = '%Y-%m-%d %H:%M:%S'
    date_fmt_2 = '%a %b %d %H:%m:%S %z %Y'
    date_fmts = [date_fmt_1, date_fmt_2]
    # clean dates
    date_var = 'created_at'
    clean_date_var = 'clean_date'
    mid_date_matcher = re.compile('(?<=[0-9])T(?=[0-9])')
    author_data = author_data.assign(**{
        clean_date_var : author_data.loc[:, date_var].apply(lambda x: mid_date_matcher.sub(' ', x).split('+')[0])
    })
    author_data = author_data.assign(**{
        clean_date_var : author_data.loc[:, clean_date_var].apply(lambda x: parse_date_all_formats(x, date_fmts))
    })
    
    ## compute activity levels
    author_var = 'screen_name'
    # date range
    # date range
    author_date_ranges = author_data.groupby(author_var).apply(lambda x: (x.loc[:, clean_date_var].max() - x.loc[:, clean_date_var].min()).days)
    # round up 0 to 1
    author_date_ranges = author_date_ranges.apply(lambda x: max(1, x))
    author_post_counts = author_data.loc[:, author_var].value_counts()
    author_post_pct = author_post_counts / author_date_ranges
    # URL shares
    URL_matcher = re.compile('https?://[\w \./]+|[a-z]+\.[a-z]+\.(com|org|net|gov)/[a-zA-Z0-9/\-_]+|')
    clean_sub_matcher = re.compile('https?://|\s')
    txt_var = 'text'
    author_data = author_data.assign(**{
        'url_match' : author_data.loc[:, txt_var].apply(lambda x: clean_sub_matcher.sub('', URL_matcher.search(x).group(0)) if URL_matcher.search(x) is not None else '')
    })
    author_URL_pct = author_data.groupby(author_var).apply(lambda x: x[x.loc[:, 'url_match']!=''].shape[0] / x.shape[0])
    share_matcher = re.compile('^RT:?\s?@\w+')
    author_data = author_data.assign(**{
        'shared_content' : author_data.loc[:, txt_var].apply(lambda x: share_matcher.search(x) is not None)
    })
    author_share_pct = author_data.groupby(author_var).apply(lambda x: x[x.loc[:, 'shared_content']].shape[0] / x.shape[0])
    
    ## combine, save to file
    out_dir = args['out_dir']
    author_activity_data = pd.concat([author_post_pct, author_URL_pct, author_share_pct], axis=1)
    author_activity_data.columns= ['post_pct', 'URL_share_pct', 'RT_pct']
    author_activity_data = author_activity_data.reset_index()
    out_file = os.path.join(out_dir, 'loanword_author_activity_data.tsv')
    author_activity_data.to_csv(out_file, sep='\t', index=False)
    
if __name__ == '__main__':
    main()