"""
Collect extra word matching data from authors' tweet data.
E.g. get all instances of loanword use in authors' prior tweets.
"""
from argparse import ArgumentParser
import logging
import os
import re
import pandas as pd
from data_helpers import BasicTokenizer, BasicCounter
import gzip
from math import ceil
from itertools import repeat
from multiprocessing import Pool

def write_matching_lines_file(data_files, out_dir, data_dir_base, data_name, old_data_ids, file_count, lang, word_matchers_by_type):
    """
    Write all data that matches at least one word in word matchers
    to temporary file.
    """
    lang_long_lookup = {'es' : 'spanish', 'en' : 'english'}
    lang_long = lang_long_lookup[lang]
    lang_var = 'lang'
    non_original_matcher = re.compile('^RT @[a-zA-Z0-9_]+')
    tokenizer = BasicTokenizer(lang=lang_long)
    txt_var = 'text'
    id_var = 'id'
    RETURN_MATCHER = re.compile('[\n\r]')
    ctr = 0
    out_file_name = os.path.join(out_dir, f'{data_dir_base}_{data_name}_{file_count}.gz')
    if(os.path.exists(out_file_name)):
        os.remove(out_file_name)
#     with gzip.open(out_file_name, 'wt') as out_file:
    for data_file in data_files:
        logging.info('processing %s'%(os.path.basename(data_file)))
        data = pd.read_csv(data_file, sep='\t', compression='gzip', index_col=False)
        data.fillna('', inplace=True)
        data = data[~data.loc[:, id_var].isin(old_data_ids)]
        data = data[data.loc[:, txt_var] != '']
        data = data[data.loc[:, lang_var] == lang]
        if(data.shape[0] > 0):
            data = data[data.loc[:, txt_var].apply(lambda x: non_original_matcher.search(x) is None)]
            # fix text data to write to file
            data = data.assign(**{
                txt_var : data.loc[:, txt_var].apply(lambda x: RETURN_MATCHER.sub(' ', x)),
                'user_description' : data.loc[:, txt_var].apply(lambda x: RETURN_MATCHER.sub(' ', x)),
            })
            # get clean text
            data = data.assign(**{
                'clean_txt' : data.loc[:, txt_var].apply(lambda x: ' '.join(tokenizer.tokenize(x.lower())))
            })
            # fix nan vals
            data.fillna('', inplace=True)
            match_data = []
            for word_type_i, word_matchers_i in word_matchers_by_type.items():
                for word_j, word_matcher_j in word_matchers_i.items():
#                     print(word_matcher_j)
#                         match_data_j = data[data.loc[:, 'clean_txt'].apply(lambda x: word_matcher_j.search(x) is not None)]
                    if(word_j is None):
                        print(f'bad word of type {word_type_i}')
                    word_matches_j = data.loc[:, 'clean_txt'].apply(lambda x: word_matcher_j.search(x))
                    word_match_idx_j = word_matches_j.apply(lambda x: x is not None)
                    if(any(word_match_idx_j)):
                        match_data_j = data[word_match_idx_j]
                        match_data_j = match_data_j.assign(**{'loanword' : word_j, 'loanword_type' : word_type_i})
                        word_matches_j = list(filter(lambda x: x is not None, word_matches_j.apply(lambda x: x.group(0) if x is not None else None)))
                        # tmp debugging
                        if(len(word_matches_j) == 0):
                            print(f'bad word matches with word {word_j} and matcher {word_matcher_j}')
#                             match_data_j.loc[:, 'clean_txt'].apply(lambda x: word_matcher_j.search(x).group(0))
                        match_data_j = match_data_j.assign(**{'loanword_verb' : word_matches_j})
                        match_data.append(match_data_j)
            # write matches to file
            if(len(match_data) > 0):
                match_data = pd.concat(match_data, axis=0)
                logging.info('match data has shape %s'%(str(match_data.shape)))
                if(os.path.exists(out_file_name)):
                    combined_match_data = pd.read_csv(out_file_name, sep='\t', compression='gzip', index_col=False)
                    combined_match_data = pd.concat([combined_match_data, match_data], axis=0)
                else:
                    combined_match_data = match_data.copy()
                combined_match_data.fillna('', inplace=True)
                combined_match_data.to_csv(out_file_name, sep='\t', compression='gzip', index=False)
#                 print('collected %d match data'%(match_data.shape[0]))
#                 print(match_data.loc[:, ['loanword', 'clean_txt']].values)
#                     if(ctr == 0):
#                         out_file.write('%s\n'%(match_data.to_csv(sep='\t', index=False, header=True)))
#                     else:
#                         out_file.write('%s\n'%(match_data.to_csv(sep='\t', index=False, header=False)))
#                     ctr += 1

def generate_begin_middle_end(x):
    return '^%s | %s | %s$'%((x,)*3)

def main():
    parser = ArgumentParser()
    parser.add_argument('data_dir')
    parser.add_argument('--old_data', default='../../data/mined_tweets/loanword_verb_posts_CLUSTER=twitter_posts_STARTDATE=2017_7_9_ENDDATE=2019_4_6.tsv')
    parser.add_argument('--word_data', nargs='+', default=['../../data/loanword_resources/wiktionary_twitter_reddit_loanword_verbs_integrated_verbs_query_phrases.tsv', '../../data/loanword_resources/wiktionary_twitter_reddit_loanword_verbs_light_verbs_query_phrases.tsv'])
    parser.add_argument('--word_data_types', nargs='+', default=['integrated_verb', 'light_verb'])
    parser.add_argument('--out_dir', default='../../data/mined_tweets/')
    parser.add_argument('--data_name', default='extra_loanword_tweets')
    args = vars(parser.parse_args())
    logging_file = '../../output/collect_extra_word_data_from_author_tweets.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    ## load word data
    word_data_files = args['word_data']
    word_data_types = args['word_data_types']
    word_data = []
    for word_data_file, word_data_type in zip(word_data_files, word_data_types):
        word_data_i = pd.read_csv(word_data_file, sep='\t')
        word_data_i = word_data_i.assign(**{'word_type' : word_data_type})
        word_data.append(word_data_i)
    word_data = pd.concat(word_data, axis=0)
#     print(word_data.head().values)
    # get word matchers
    word_data = word_data.assign(**{
        'matcher' : word_data.loc[:, 'verb'].apply(lambda x: re.compile(generate_begin_middle_end(x)))
    })
    word_matchers_by_type = {}
    for type_i, data_i in word_data.groupby('word_type'):
        word_matchers_by_type[type_i] = dict(zip(data_i.loc[:, 'loanword'].values, data_i.loc[:, 'matcher'].values))
#     print(word_matchers_by_type['light_verb']['access'].pattern)
#     print('word matchers %s'%(str(word_matchers)))
    
    ## query all data files
    # remove existing data
    old_data_file = args['old_data']
    old_data = pd.read_csv(old_data_file, sep='\t')
    id_var = 'id'
    old_data_ids = set(old_data.loc[:, id_var].unique())
    data_dir = args['data_dir']
    data_dir_base = os.path.basename(os.path.normpath(data_dir))
    file_matcher = re.compile('.+tweets\.gz')
    data_files = list(map(lambda x: os.path.join(data_dir,x), os.listdir(data_dir)))
    data_files = list(filter(lambda x: file_matcher.search(x) is not None, data_files))
    out_dir = args['out_dir']
    data_name = args['data_name']
    lang = 'es'
    
    # TODO: parallelize and then re-combine, why not
#     data_ctr = BasicCounter()
    file_chunks = 10
    chunk_size = int(ceil(len(data_files) / file_chunks))
    data_file_chunks = [data_files[(i*chunk_size):((i+1)*chunk_size)] for i in range(file_chunks)]
    # tmp debugging
#     data_file_chunks = data_file_chunks[:1]
    num_processes = 10
    data_ctr = list(range(num_processes))
    pool = Pool(processes=num_processes)
    pool_output = pool.starmap(write_matching_lines_file, zip(data_file_chunks, repeat(out_dir), repeat(data_dir_base), repeat(data_name), repeat(old_data_ids), data_ctr, repeat(lang), repeat(word_matchers_by_type)))
    # re-collect data files
    out_file_matcher = re.compile(f'{data_dir_base}_{data_name}_\d+.gz')
    out_files = list(filter(lambda x: out_file_matcher.search(x) is not None, os.listdir(out_dir)))
    out_files = list(map(lambda x: os.path.join(out_dir, x), out_files))
    # write data to combined file
    out_file_name = os.path.join(out_dir, f'{data_dir_base}_{data_name}.gz')
#     with gzip.open(out_file_name, 'wt') as out_file:
    combined_out_data = []
    for i, out_file_i in enumerate(out_files):
        data_i = pd.read_csv(out_file_i, sep='\t', compression='gzip', index_col=False)
        combined_out_data.append(data_i)
    combined_out_data = pd.concat(combined_out_data, axis=0)
    combined_out_data.to_csv(out_file_name, sep='\t', compression='gzip', index=False)
    # remove old data files
    for out_file_i in out_files:
        os.remove(out_file_i)
#         if(i == 0):
#             out_file.write('%s\n'%(data_i.to_csv(sep='\t', index=False, header=True)))
#         else:
#             out_file.write('%s\n'%(data_i.to_csv(sep='\t', index=False, header=False)))
#         os.remove(out_file_i)
    
    # old serial code
#     lang_long_lookup = {'es' : 'spanish', 'en' : 'english'}
#     lang_long = lang_long_lookup[lang]
#     lang_var = 'lang'
#     out_file_name = os.path.join(out_dir, f'{data_dir_base}_{data_name}.gz')
#     non_original_matcher = re.compile('^RT @[a-zA-Z0-9_]+')
#     tokenizer = BasicTokenizer(lang=lang_long)
#     txt_var = 'text'
#     ctr = 0
#     with gzip.open(out_file_name, 'wt') as out_file:
#         for data_file in data_files:
#             logging.info('processing %s'%(os.path.basename(data_file)))
#             data = pd.read_csv(data_file, sep='\t', compression='gzip')
#             data.fillna('', inplace=True)
#             data = data[~data.loc[:, id_var].isin(old_data_ids)]
#             data = data[data.loc[:, txt_var] != '']
#             data = data[data.loc[:, lang_var] == lang]
#             if(data.shape[0] > 0):
#                 data = data[data.loc[:, txt_var].apply(lambda x: non_original_matcher.search(x) is None)]
#                 # get clean text
#                 data = data.assign(**{
#                     'clean_txt' : data.loc[:, txt_var].apply(lambda x: ' '.join(tokenizer.tokenize(x.lower())))
#                 })
#                 match_data = []
#                 for word_type_i, word_matchers_i in word_matchers_by_type.items():
#                     for word_j, word_matcher_j in word_matchers_i.items():
#     #                     print(word_matcher_j)
# #                         match_data_j = data[data.loc[:, 'clean_txt'].apply(lambda x: word_matcher_j.search(x) is not None)]
#                         word_matches_j = data.loc[:, 'clean_txt'].apply(lambda x: word_matcher_j.search(x))
#                         word_match_idx_j = word_matches_j.apply(lambda x: x is not None)
#                         if(any(word_match_idx_j)):
#                             match_data_j = data[word_match_idx_j]
#                             match_data_j = match_data_j.assign(**{'loanword' : word_j, 'loanword_type' : word_type_i})
#                             word_matches_j = list(filter(lambda x: x is not None, word_matches_j.apply(lambda x: x.group(0) if x is not None else None)))
#                             # tmp debugging
#                             if(len(word_matches_j) == 0):
#                                 print(f'bad word matches with word {word_j} and matcher {word_matcher_j}')
# #                             match_data_j.loc[:, 'clean_txt'].apply(lambda x: word_matcher_j.search(x).group(0))
#                             match_data_j = match_data_j.assign(**{'loanword_verb' : word_matches_j})
#                             match_data.append(match_data_j)
#                 # write matches to file
#                 if(len(match_data) > 0):
#                     match_data = pd.concat(match_data, axis=0)
#     #                 print('collected %d match data'%(match_data.shape[0]))
#     #                 print(match_data.loc[:, ['loanword', 'clean_txt']].values)
#                     if(ctr == 0):
#                         out_file.write('%s'%(match_data.to_csv(sep='\t', index=False, header=True)))
#                     else:
#                         out_file.write('%s'%(match_data.to_csv(sep='\t', index=False, header=False)))
#                     ctr += 1
                
if __name__ == '__main__':
    main()