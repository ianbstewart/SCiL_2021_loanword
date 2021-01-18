"""
Index posts from raw files into Elasticsearch.
This includes adding language ID.
"""
from argparse import ArgumentParser
import logging
import os
from subprocess import Popen
from elasticsearch import Elasticsearch, helpers
from time import sleep
import re
import cld2
from data_helpers import get_file_iter, clean_txt_simple
import json
from math import ceil
from datetime import datetime
from collections import deque
from dateutil import parser as date_parser

URL_MATCHER=re.compile('\(http://[^\)]+\)|\(www\.[^\)]+\)')
USER_MATCHER=re.compile('u/\w+')
SUB_MATCHER=re.compile('r/\w+')
# replace URLs, usernames, subreddits
REDDIT_TXT_MATCHERS=[(URL_MATCHER, ''), (USER_MATCHER, ''), (SUB_MATCHER, '')]
def clean_txt_reddit(txt):
    for txt_matcher, txt_sub in REDDIT_TXT_MATCHERS:
        txt = txt_matcher.sub(txt_sub, txt)
    return txt

def get_val_from_recursive_key(key, data):
    """
    Get value from dict using recursive key.
    E.g. get "user.bio" from {"user" : {"bio" : "yes"}}
    """
    val = data.copy()
    key_tuple = key.split('.')
    for key_i in key_tuple:
        if(key_i in val):
            val = val[key_i]
        else:
            val = None
            break
    return val

# tmp debugging
# need global because function is used as iterator
LINE_CTR=0
FILE_CTR=0
def convert_file_to_dict(post_files, ES_index, post_type, post_subset, txt_var='body', data_fields=['body', 'subreddit', 'id', 'author', 'author_flair_text', 'created_utc', 'parent_id', 'score'], valid_langs=None):
    """
    Read JSON from file and convert to dict
    to be indexed by ES.
    """
    global LINE_CTR
    global FILE_CTR
    # separate regular and recursive data fields
    # e.g. user.bio
    recursive_data_fields = list(filter(lambda x: '.' in x, data_fields))
    recursive_data_field_tuples = list(map(lambda x: x.split('.'), recursive_data_fields))
    data_fields = list(filter(lambda x: x not in recursive_data_fields, data_fields))
    # tmp debugging
    line_cutoff = 100000
    test_valid_lang = valid_langs is not None
    # convert fields when needed, e.g. date
    data_field_converters = {'created_at' : date_parser.parse}
    data_fields_to_convert = list(set(data_fields) & set(data_field_converters.keys()))
    # TODO: experiment with 2-gram indexing (shingles??) for faster queries
#     tokenized_txt_var = f'{txt_var}_tokenized'
    for post_file in post_files:
        logging.warning('starting to process file %s'%(post_file))
        try:
            post_iter = get_file_iter(post_file)
            for lines in post_iter:
                if(type(lines) is not list):
                    lines = [lines]
                for line_i in lines:
                    try:
                        complete_data_i = json.loads(line_i)
                        if('delete' not in complete_data_i.keys()):
                            data_i = {data_field : complete_data_i[data_field] for data_field in data_fields}
                            # convert fields when needed, e.g. date
                            for data_field in data_fields_to_convert:
                                data_i[data_field] = data_field_converters[data_field](data_i[data_field])
                            # collect recursive data, e.g. user.bio
                            for recursive_data_field in recursive_data_fields:
                                data_i_j = get_val_from_recursive_key(recursive_data_field, complete_data_i)
                                if(data_i_j is not None):
                                    data_i[recursive_data_field.replace('.', '_')] = data_i_j
                            # add tokenized text
#                             data_i[tokenized_txt_var] = data_i[txt_var]
                            data_i['_type'] = post_type
                            data_i['_index'] = ES_index
                            data_i['_id'] = "t%d_%s"%(post_subset, data_i['id']) #setting fullname as ID to avoid duplicates
                            txt = data_i[txt_var]
                            if(post_type == 'reddit'):
                                txt_clean = clean_txt_reddit(txt)
                            else:
                                txt_clean = clean_txt_simple(txt)
                            # add lang ID and score
                            txt_lang_results = cld2.detect(txt).details
                            txt_lang = txt_lang_results[0].language_code
                            txt_lang_conf_score = txt_lang_results[0].percent
                            data_i['lang'] = txt_lang
                            data_i['lang_score'] = txt_lang_conf_score
                            if(not test_valid_lang or txt_lang in valid_langs):
                                LINE_CTR += 1
                                if(LINE_CTR % 100000 == 0):
                                    logging.warning('processed %d lines'%(LINE_CTR))
                                yield(data_i)
                    except Exception as e:
                        logging.warning('bad line with error %s'%(e))
#                 if(LINE_CTR >= line_cutoff):
#                     logging.warning('line cutoff at lines=%d'%(LINE_CTR))
#                     break
            post_iter.close()
        except Exception as e:
            logging.warning('exception when reading file %s = %s'%(post_file, e))
            pass
        FILE_CTR += 1
        logging.warning('%d files done; finished file %s'%(FILE_CTR, post_file))
#         if(LINE_CTR >= line_cutoff):
#             logging.warning('line cutoff at lines=%d'%(LINE_CTR))
#             break

def main():
    parser = ArgumentParser()
    parser.add_argument('ES_index') # example on conair: reddit_comments_2012_m_7_12
    parser.add_argument('--ES_dir', default='/hg190/elastic_search/elasticsearch-2.1.1/bin/')
    parser.add_argument('--ES_mapping_file', default='/hg190/elastic_search/comment_mapping_reduced_lang_full_body.json')
    parser.add_argument('--post_dir', default='/hg190/corpora/reddit_full_comment_data/')
    parser.add_argument('--post_year', type=int, default=2017)
    parser.add_argument('--post_year_subset', type=int, default=2)
    parser.add_argument('--post_start_month', type=int, default=1)
    parser.add_argument('--post_end_month', type=int, default=6)
    parser.add_argument('--post_type', default='reddit')
    parser.add_argument('--valid_langs', nargs='+', default=['en'])
    args = vars(parser.parse_args())
    logging_file = '../../output/index_ES_posts.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.WARNING)
    
    ## collect post files corresponding to month/year subset
    post_type = args['post_type']
    if(post_type == 'reddit'):
        post_prefix = 'RC_'
    elif(post_type == 'reddit_submissions'):
        post_prefix = 'RS_'
    post_dir = args['post_dir']
    post_year = str(args['post_year'])
    post_year_subset = args['post_year_subset']
    # get IDs corresponding to subset of year
    post_start_month = args['post_start_month']
    post_end_month = args['post_end_month']
    if(post_type == 'reddit'):
        post_year_subset_ids = list(map(lambda x: '%.2d'%(x), range(post_start_month, post_end_month+1)))
        post_file_matcher = re.compile('%s%s-(%s)'%(post_prefix, post_year, '|'.join(post_year_subset_ids)))
        ES_index = 'reddit_comments_%s_m_%d_%d'%(post_year, post_start_month, post_end_month)
        post_dir = os.path.join(post_dir, post_year)
    elif(post_type == 'reddit_submissions'):
        post_year_subset_ids = list(map(lambda x: '%.2d'%(x), range(post_start_month, post_end_month+1)))
        post_file_matcher = re.compile('%s%s-(%s)'%(post_prefix, post_year, '|'.join(post_year_subset_ids)))
        ES_index = 'reddit_submissions_%s_m_%d_%d'%(post_year, post_start_month, post_end_month)
        post_dir = os.path.join(post_dir, post_year)
    elif(post_type == 'twitter'):
#         months_per_year = 12
#         subsets_per_year = 4
#         post_year_months = list(map(lambda x: datetime.strftime(datetime.strptime(str(x), '%m'), '%b'), range(1, months_per_year+1)))
#         post_year_subset_months = post_year_months[((post_year_subset-1)*subsets_per_year):(post_year_subset*subsets_per_year)]
        post_year_subset_months = list(map(lambda x: datetime.strftime(datetime.strptime(str(x), '%m'), '%b'), range(post_start_month, post_end_month)))
        post_year_str = str(post_year)[2:]
        post_file_matcher = re.compile('tweets-(%s)-\d{2}-%s'%('|'.join(post_year_subset_months), post_year_str))
    post_files = list(map(lambda x: os.path.join(post_dir, x), list(filter(lambda y: post_file_matcher.search(y) is not None, os.listdir(post_dir)))))
    # tmp debugging
#     post_files = post_files[:1]
    logging.warning('processing files %s'%(', '.join(post_files)))
    
    ## create index
    ES_index = args['ES_index']
    es = Elasticsearch(timeout=600)
    # tmp debugging: remove index
    if(es.indices.exists(ES_index)):
        es.indices.delete(index=ES_index, ignore=[400, 404])
    # get text var for analyzer and for generating data for indexer
    if(post_type == 'reddit'):
        txt_var = 'body'
    elif(post_type == 'reddit_submissions'):
        txt_var = 'title'
    else: 
        txt_var = 'text'
    # set up index
    if(not es.indices.exists(ES_index)):
        ES_mapping_file = args['ES_mapping_file']
        # get mapping if saved, otherwise use default analyzer for text
        if(os.path.exists(ES_mapping_file)):
            ## custom map for all relevant fields, including a non-analyzed body field!!
            body = json.load(open(args['ES_mapping_file'], 'r'))
        else:
            body = {
                'analysis' : [
                    # keep text in raw form
                    # TODO: how to do this without breaking memory AND allowing multi-word search??
#                     {
#                         'analyzer' : {
#                             txt_var : { 
#         #                 'filter' : {
#         #                     'text_stops' : {
#         #                         'type' : 'stop',
#         #                         'stopwords': '_english_'
#         #                     },
#                                 'filter' : ['lowercase'],
#         #                 },
#                             }
#                         }
#                     },
                    # tokenize text
                    {
                         'analyzer' : {
                            txt_var : {
                                'tokenizer' : 'classic', 
                                'filter' : ['lowercase'],
                            },
                         }
                    }
                ]

    #                 'analysis' : {
    #                     'analyzer' : {
    #                         f'{txt_var}.tokenized' : {
    #                             'tokenizer' : 'classic', 
    #                             'filter' : ['lowercase'],
    #                         },
    #                     }
    #                 }
            }
        index_create_response = es.indices.create(index=ES_index, body=body)
        
#     logging.warning(index_create_response)
    
    ## load data into index
    # specify fields to include in data
    if(post_type == 'reddit'):
        data_fields = ['body', 'subreddit', 'id', 'author', 'author_flair_text', 'created_utc', 'parent_id', 'score']
    elif(post_type == 'reddit_submissions'):
        data_fields = ['title', 'subreddit', 'id', 'author', 'author_flair_text', 'created_utc', 'score']
    else: # TODO: Twitter fields?? how to handle nested fields (user.bio)
        data_fields = ['text', 'id', 'created_at', 'user.id', 'user.description', 'user.screen_name', 'user.location']
#     post_data_iter = convert_file_to_dict(post_files, ES_index, post_type, post_year_subset)
    ES_dir = args['ES_dir']
#     out_file_name = os.path.join(ES_dir, ES_index)
    valid_langs = args['valid_langs']
    if(len(valid_langs) == 0 or valid_langs[0] == ''):
        valid_langs = None
    logging.warning('es info:\n%s'%(es.info()))
    # serial
#     es_output = helpers.bulk(es, convert_file_to_dict(post_files, ES_index, post_type, post_year_subset, data_fields=data_fields, txt_var=txt_var, valid_langs=valid_langs), yield_ok=False)
    MAX_THREADS=30
    deque(helpers.parallel_bulk(es, convert_file_to_dict(post_files, ES_index, post_type, post_year_subset, data_fields=data_fields, txt_var=txt_var, valid_langs=valid_langs), thread_count=MAX_THREADS, raise_on_exception=True), maxlen=0)
    logging.warning('finished parallel load')
    
    ## execute query
    # test query: subreddit counts
    if(post_type == 'reddit' or post_type == 'reddit_submissions'):
        ES_TIMEOUT=600
    #     es = Elasticsearch(timeout=ES_TIMEOUT)
        query_subreddit = 'news'
        query_lang = 'en'
        query = { #aggregate query
          "query": {
            "filtered": {
              "filter": { "term": {"lang":query_lang} }
            }
          }
        ,
          "aggs": { "link_id": { "terms": { "field": "link_id", "size":0} } }
          , "size": 0
        }
        es_results = es.search(index=ES_index, body=query)
        logging.warning('query test results %s'%(es_results))
    
if __name__ == '__main__':
    main()