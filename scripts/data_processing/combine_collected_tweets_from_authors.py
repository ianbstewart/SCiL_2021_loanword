"""
Combine collected tweets from loanword authors
from different directories.
"""
from argparse import ArgumentParser
import logging
import os
import re
import numpy as np
import pandas as pd

def get_all_files(data_dir, file_matcher):
    data_files = list(map(lambda x: os.path.join(data_dir, x), os.listdir(data_dir)))
    data_files = list(filter(lambda x: file_matcher.search(x) is not None, data_files))
    return data_files

def combine_source_target_files(source_dir, target_dir, out_dir, file_matcher, original_ids=None):
    """
    Combine all matching source and target data files,
    then write to file.
    """
    source_files = get_all_files(source_dir, file_matcher)
    target_files = get_all_files(target_dir, file_matcher)
    target_file_bases = np.array(list(map(lambda x: os.path.basename(x).lower(), target_files)))
    id_var = 'id'
    dedup_vars = [id_var]
    all_txt_vars = ['text', 'user_description', 'user_location']
    RETURN_CHAR_MATCHER = re.compile('[\n\r\t]')
    if(not os.path.exists(out_dir)):
        os.mkdir(out_dir)
    for source_file in source_files:
        # find matching target file
        source_file_base = os.path.basename(source_file).lower()
        target_file_base_idx = np.where(target_file_bases == source_file_base)[0]
        combined_data_file_name = os.path.join(out_dir, source_file_base)
#         if(not os.path.exists(combined_data_file_name)):
        # if target file exists, then combine source/target
        if(len(target_file_base_idx) > 0):
            target_file_base_idx = target_file_base_idx[0]
            target_file = target_files[target_file_base_idx]
            try:
                source_data = pd.read_csv(source_file, sep='\t', compression='gzip')
                if('Unnamed: 0' in source_data.columns):
                    source_data.drop('Unnamed: 0', axis=1, inplace=True)
                # fix column name mismatches
                source_data.rename(columns={'user_screen_name' : 'screen_name', 'user_id' : 'author_id'}, inplace=True)
                target_data = pd.read_csv(target_file, sep='\t', compression='gzip')
                # combine!
                logging.info(f'combining files for {source_file_base}')
                combined_data = pd.concat([source_data, target_data], axis=0)
                # deduplicate!
                combined_data.drop_duplicates(dedup_vars, inplace=True)
                # clean
                combined_data.fillna('', inplace=True)
                # filter original IDs
                if(original_ids is not None):
                    combined_data = combined_data[~combined_data.loc[:, id_var].isin(original_ids)]
                # remove return characters
                for txt_var_i in all_txt_vars:
                    combined_data = combined_data.assign(**{
                        txt_var_i : combined_data.loc[:, txt_var_i].apply(lambda x: RETURN_CHAR_MATCHER.sub('', str(x)))
                    })
                logging.info('%d/%d source/target'%(source_data.shape[0], target_data.shape[0]))
                logging.info('combined data has %d/%d data'%(combined_data.shape[0], source_data.shape[0]+target_data.shape[0]))
                # write to file
                combined_data.to_csv(combined_data_file_name, sep='\t', compression='gzip', index=False)
            except Exception as e:
                logging.info(f'going to skip file {source_file_base} because error {e}')
        # if target file does not exist, copy the source data
        else:
            logging.info(f'copying {source_file} without combining')
            source_data = pd.read_csv(source_file, sep='\t', compression='gzip')
            if('Unnamed: 0' in source_data.columns):
                source_data.drop('Unnamed: 0', axis=1, inplace=True)
            # fix column name mismatches
            source_data.rename(columns={'user_screen_name' : 'screen_name', 'user_id' : 'author_id'}, inplace=True)
            source_data.to_csv(combined_data_file_name, sep='\t', compression='gzip', index=False)
            
def main():
    parser = ArgumentParser()
    parser.add_argument('source_dir') # ../../data/mined_tweets/loanword_author_tweets_elasticsearch/
    parser.add_argument('target_dir') # ../../data/mined_tweets/loanword_author_tweets/
    parser.add_argument('--original_author_data', default='../../data/mined_tweets/loanword_verb_posts_CLUSTER=twitter_posts_STARTDATE=2017_7_9_ENDDATE=2019_4_6.tsv')
    parser.add_argument('--out_dir', default='../../data/mined_tweets/loanword_author_tweets_all_archives/')
    parser.add_argument('--file_matcher', default='.*_tweets.gz')
    args = vars(parser.parse_args())
    logging_file = '../../output/combine_collected_tweets_from_authors.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    ## combine data
    # get IDs to remove duplicates
    original_author_data = pd.read_csv(args['original_author_data'], sep='\t')
    original_ids = set(original_author_data.loc[:, 'id'].unique())
    # add each data file from source dir to corresponding file in target dir
    source_dir = args['source_dir']
    target_dir = args['target_dir']
    file_matcher = re.compile(args['file_matcher'])
    out_dir = args['out_dir']
    combine_source_target_files(source_dir, target_dir, out_dir, file_matcher, original_ids=original_ids)
    # also combine lang ID data
    source_lang_id_file = os.path.join(source_dir, 'lang_id.gz')
    target_lang_id_file = os.path.join(target_dir, 'lang_id.gz')
    source_lang_id_data = pd.read_csv(source_lang_id_file, sep='\t', compression='gzip')
    target_lang_id_data = pd.read_csv(target_lang_id_file, sep='\t', compression='gzip')
    lang_id_data = pd.concat([source_lang_id_data, target_lang_id_data], axis=0)
    lang_id_data.drop_duplicates('id', inplace=True)
    lang_id_data_file = os.path.join(out_dir, 'lang_id.gz')
    lang_id_data.to_csv(lang_id_data_file, sep='\t', compression='gzip', index=False)
    
if __name__ == '__main__':
    main()