"""
Generate word counts per day and convert into sparse matrix.
Format: row = day (with index), column = word. 
This will involve a lot of for-loops.
"""
from argparse import ArgumentParser
import logging
import os
from sklearn.feature_extraction.text import CountVectorizer
import re
# from nltk.corpus import stopwords # incomplete list
from stop_words import get_stop_words
from unidecode import unidecode
import gzip
from bz2 import BZ2File
import numpy as np
import pandas as pd
import json
from nltk.tokenize import word_tokenize
import lzma
from data_helpers import ZstdWrapper

HASH_MATCHER = re.compile('#[^\s#]+')
USER_MATCHER = re.compile('@\w+')
URL_MATCHER = re.compile('https?://[^\s]+|t\.co[^\s]+|pic.twitter.com[^\s]+')
NUM_MATCHER = re.compile('\d+')
MATCHER_PAIRS = [[HASH_MATCHER, '#HASH'], [USER_MATCHER, '@USER'], [URL_MATCHER, '<URL>'], [NUM_MATCHER, '<NUM>']]
def clean_txt(txt):
    txt = txt.lower()
    for matcher, replacement in MATCHER_PAIRS:
        txt = matcher.sub(replacement, txt)
    return txt

def main():
    parser = ArgumentParser()
    parser.add_argument('data_file_name')
    parser.add_argument('--lang_id_dir', default='lang_id')
    parser.add_argument('--lang', default='en')
    parser.add_argument('--out_dir', default='../../data/mined_tweets/freq_data/')
    parser.add_argument('--min_df', type=int, default=20)
    parser.add_argument('--txt_var', default='text')
    parser.add_argument('--id_var', default='id')
    args = vars(parser.parse_args())
    data_file_dir = os.path.dirname(args['data_file_name'])
    data_file_name_base = os.path.basename(args['data_file_name'])
    data_file_ext = data_file_name_base.split('.')[-1]
    logging_file = '../../output/generate_word_counts_from_data_%s.txt'%(data_file_name_base.replace('.gz',''))
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.DEBUG)
    
    ## set up counter
    # get stop words
    lang_lookup = {'en' : 'english', 'fr' : 'french', 'es' : 'spanish'}
    lang_long = lang_lookup[args['lang']]
    stop_words = get_stop_words(lang_long)
    # also add de-accented stops
    stop_words = list(set(stop_words) & set(map(unidecode, stop_words)))
    max_df = 0.75 # default high max DF for more complete frequency counts (more stable frequency normalization)
    cv = CountVectorizer(min_df=args['min_df'], max_df=max_df, stop_words=stop_words, tokenizer=lambda x: word_tokenize(x, language=lang_long))

    ## if provided, load language lookup for filtering
    if('lang_id_dir' in args):
        lang_id_file = os.path.join(os.path.join(data_file_dir, args['lang_id_dir']), data_file_name_base.replace('.%s'%(data_file_ext), '_lang_id.tsv.gz'))
        lang_id_df = pd.read_csv(lang_id_file, sep='\t', index_col=False, compression='gzip', header=None, names=['id', 'lang', 'score'])
        lang_ids = set(lang_id_df[lang_id_df.loc[:, 'lang']==args['lang']].loc[:, 'id'].unique())
    
    ## collect text
    ## TODO: make this an iterator
    
    filter_lang = 'lang' in args
    text_combined = []
    delete_val = '[deleted]'
    post_ctr = 0
    valid_post_ctr = 0
    try:
        if(data_file_ext == 'gz'):
            data_input = gzip.open(args['data_file_name'], 'r')
        elif(data_file_ext == 'bz2'):
            data_input = BZ2File(args['data_file_name'], 'r')
        elif(data_file_ext == 'xz'):
            data_input = lzma.open(args['data_file_name'])
        elif(data_file_ext == 'zst'):
            data_input = ZstdWrapper(args['data_file_name']) # WARNING returns list of lines to process
        for lines in data_input:
            if(type(lines) is str or type(lines) is bytes):
                lines = [lines]
            for l in lines:
                try:
                    l_data = json.loads(l)
                    if('delete' not in l_data):
                        l_id = l_data[args['id_var']]
                        l_txt = l_data[args['txt_var']]
                        if(filter_lang and l_id in lang_ids and l_txt != delete_val):
                            l_txt = clean_txt(l_txt)
                            text_combined.append(l_txt)
                            valid_post_ctr += 1
                        post_ctr += 1
                        if(post_ctr % 1000000 == 0):
                            logging.debug('captured %d/%d posts'%(valid_post_ctr, post_ctr))
                except Exception as e:
                    logging.debug('exception %s'%(e))
                    pass
    except Exception as e:
        logging.debug('closed input file %s'%(args['data_file_name']))
        data_input.close()
    logging.debug('collected %d posts'%(len(text_combined)))
    
    ## fit
    dtm = cv.fit_transform(text_combined)
    logging.debug('generated DTM %s'%(str(dtm.shape)))
    # condense
    word_counts = np.asarray(dtm.sum(axis=0))[0]
    # add word indices
    word_counts = pd.Series(word_counts, index=sorted(cv.vocabulary_, key=cv.vocabulary_.get)).sort_values(inplace=False, ascending=False)
    logging.debug('top word counts:\n%s'%(word_counts.head(50)))
    
    ## save to file
    if(not os.path.exists(args['out_dir'])):
        os.mkdir(args['out_dir'])
    meta_data_str = ''
    if('lang' in args):
        meta_data_str += '_LANG=%s'%(args['lang'])
    out_file_name = os.path.join(args['out_dir'], data_file_name_base.replace('.%s'%(data_file_ext), '%s_word_counts.gz'%(meta_data_str)))
    logging.debug('writing word counts to %s'%(out_file_name))
    word_counts.to_csv(out_file_name, sep='\t', compression='gzip', encoding='utf-8', index=True, header=False)
    
if __name__ == '__main__':
    main()