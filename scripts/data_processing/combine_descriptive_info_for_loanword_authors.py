"""
Combine loanword author descriptor data for:

- demographics
    - location
    - age (?)
"""
from argparse import ArgumentParser
import logging
import os
import pandas as pd


def main():
    parser = ArgumentParser()
    parser.add_argument('author_data_files', nargs='+')
    parser.add_argument('--out_dir', default='../../data/mined_tweets/')
    parser.add_argument('--out_file_name', default='loanword_author_descriptive_data')
    args = vars(parser.parse_args())
    logging_file = '../../output/combine_descriptive_info_for_loanword_authors.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    ## load data
    author_data_files = args['author_data_files']
    author_data = []
    for author_data_file in author_data_files:
        author_data_i = pd.read_csv(author_data_file, sep='\t', index_col=False)
        author_data.append(author_data_i)
    author_data = pd.concat(author_data, axis=0)
    author_var = 'user_screen_name'
    if('screen_name' in author_data.columns):
        author_data.rename(columns={'screen_name' : author_var}, inplace=True)
    # drop duplicates
    author_data.drop_duplicates(author_var, inplace=True)
    # limit to useful info
    author_data_cols = ['loanword_type', author_var, 'user_id', 'user_description', 'user_location']
    author_data_cols = list(filter(lambda x: x in author_data.columns, author_data_cols))
    author_data = author_data.loc[:, author_data_cols]
    
    ## write to file
    out_dir = args['out_dir']
    out_file_name = args['out_file_name']
    out_file = os.path.join(out_dir, f'{out_file_name}.tsv')
    author_data.to_csv(out_file, sep='\t', index=False)
    
if __name__ == '__main__':
    main()