"""
Sample post text with matching phrases => need this for annotation,
e.g. does "tweetear" mean the same thing as "hacer un tweet"? if not, what phrase would you use?
"""
from argparse import ArgumentParser
import logging
import os
import pandas as pd
from data_helpers import get_file_iter, BasicTokenizer, clean_txt_for_matching, clean_txt_emojis
from nltk.tokenize import sent_tokenize
import re
import json
from elasticsearch import Elasticsearch
from subprocess import Popen
import numpy as np
from time import sleep
np.random.seed(123)

def collect_scroll_results(es, es_instance_name, es_query, MAX_SCROLL_SIZE=1000):
    """
    Collect full query results using Elasticsearch scroll.
    """
    res = es.search(index=es_instance_name, body=es_query, size=MAX_SCROLL_SIZE, scroll='2m')
    scroll_id = res['_scroll_id']
    scroll_size = len(res['hits']['hits'])
    full_results = []
    while(scroll_size > 0):
#         for i, res_i in enumerate(res['hits']['hits']):
#             full_results.append(res_i)
        full_results += res['hits']['hits']
        res = es.scroll(scroll_id=scroll_id, scroll='2m')
        scroll_id = res['_scroll_id']
        scroll_size = len(res['hits']['hits'])
        # TODO: memory error??
#         break
    return full_results
    
MONTHS=['jan', 'feb', 'mar', 'apr', 'may', 'june', 'july', 'aug', 'sep', 'oct', 'nov', 'dec']
ES_SETUP_SLEEP_TIME=150
MAX_TIMEOUT=300
def sample_post_text_from_ES(es_year, es_start_month, es_end_month, txt_var, lang, word_list_pairs, es_cluster_name='reddit_comments', es_dir='/hg190/elastic_search/es_instances_for_reddit_comments/', word_query_filters=None, sample_size=10, max_subtypes_per_word=5):
    """
    Sample post text from ES instance.
    
    :param word_list_pairs: pairs of group words and query words (e.g. "tweet" | ["tuitear", "tuiteo"])
    """
    # start ES index
    es_start_month_str = MONTHS[es_start_month-1]
    es_end_month_str = MONTHS[es_end_month-1]
    es_year_str = str(es_year)
    es_dir_full = os.path.join(es_dir, es_year_str, '%s-%s-%s'%(es_start_month_str, es_end_month_str, es_year_str), 'elasticsearch-2.1.1/bin')
    es_index = '%s_%d_m_%d_%d'%(es_cluster_name, es_year, es_start_month, es_end_month)
    es_command = './elasticsearch --cluster.name %s --node.name %s'%(es_cluster_name, es_index)
    combined_samples = []
    long_lang_lookup = {'en' : 'english', 'es' : 'spanish'}
    lang_long = long_lang_lookup[lang]
    TKNZR = BasicTokenizer(lang=lang_long)
    # restrict to short contexts
#     MAX_TXT_LEN=100
    try:
        process = Popen(es_command.split(' '), cwd=es_dir_full)
        try:
            sleep(ES_SETUP_SLEEP_TIME)
        except Exception as e:
            pass
        es = Elasticsearch(timeout=MAX_TIMEOUT)
        for word, word_list in word_list_pairs:
            word_results = []
            for word_i in word_list:
                es_query = {
                    "query" : {
                        "bool" : {
                            # one-word match
                            "must" : [
                                {
                                    "match" : {
                                        "body" : word_i,
                                    }
                                },
                                {
                                    "match" : {
                                        "lang" : lang,
                                    }
                                }
                            ],
                        }
                    }
                }
                results_i = collect_scroll_results(es, es_index, es_query)
                # get word context, i.e. sentence containing word_i
                word_i_matcher = re.compile(' %s '%(word_i))
                clean_results_i = []
                result_ctr = 0
                for res_j in results_i:
                    txt_j = res_j['_source'][txt_var]
                    txt_j = clean_txt_for_matching(txt_j, TKNZR=TKNZR)
                    txt_j = clean_txt_emojis(txt_j)
                    sents_j = sent_tokenize(txt_j)
                    matching_sent = ''
                    for sent_k in sents_j:
                        if(word_i_matcher.search(sent_k) is not None):
                            matching_sent = sent_k
                            break
                    if(matching_sent != ''):
                        res_j['_source'][txt_var] = matching_sent
                        res_j['_source']['word_match'] = word_i
                        clean_results_i.append(res_j)
                    # restrict to M tokens per subtype
                    # prevent us from getting stuck with 
                    # all "acceso" subtypes for "access"
                    # i.e. enforces diversity of subtypes!
                    result_ctr += 1
                    if(result_ctr > max_subtypes_per_word):
                        break
                word_results += clean_results_i
            # get random sample size N
            if(len(word_results) > sample_size):
                # get max of N random samples
                word_results = np.random.choice(word_results, size=sample_size, replace=False)
            # get word, word match, word match context
#             word_results_txt = list(map(lambda x: x['_source'][txt_var], word_results))
            word_results_data = list(map(lambda x: [word, x['_source']['word_match'], x['_source'][txt_var]], word_results))
#             word_results_txt = list(map(lambda x: [word, x], word_results_txt))
            combined_samples += word_results_data
        process.terminate()
    except Exception as e:
        print('exception %s'%(e))
        logging.debug('terminating process early')
        process.terminate()
    combined_samples = pd.DataFrame(combined_samples, columns=['word', 'word_match', 'text'])
    return combined_samples

def sample_post_text(post_file, txt_var, id_var, lang, lang_id_dir, word_query_pairs):
    """
    Sample post text that matches a query phrase.
    
    :param post_file: post file to sample from => TODO: replace with ES instance because SLOW
    :param txt_var: post text var
    :param id_var: post ID var
    :param lang: valid post lang from which to sample
    :param lang_id_dir: lang ID directory
    
    :returns post_txt_samples:: post txt sample data
    """
    ## TODO: replace with ES for speedup??
    ## 
    post_txt_samples = []
    long_lang_lookup = {'en' : 'english', 'es' : 'spanish'}
    lang_long = long_lang_lookup[lang]
    score_thresh = 75.
    tokenizer = BasicTokenizer(lang=lang_long)
    # tmp debugging
    line_cutoff = 100000
    logging.info('processing file %s'%(post_file))
    post_file_base = os.path.basename(post_file).split('.')[0]
    post_dir = os.path.dirname(post_file)
    lang_id_file = os.path.join(post_dir, lang_id_dir, '%s_lang_id.tsv.gz'%(post_file_base))
    lang_id_data = pd.read_csv(lang_id_file, sep='\t', index_col=False, compression='gzip', header=None, names=['id', 'lang', 'score'])
    valid_lang_ids = lang_id_data[(lang_id_data.loc[:, 'score'] > score_thresh) & (lang_id_data.loc[:, 'lang'] == lang)].loc[:, 'id'].unique()
    logging.info('%d valid lang IDs'%(len(valid_lang_ids)))
    file_iter = get_file_iter(post_file)
#     logging.info('file iter %s'%(file_iter))
    line_ctr = 0
    try:
        for lines in file_iter:
            if(type(lines) is not list):
                lines = [lines]
            for line in lines:
#                 logging.info('line=%s'%(line))
                try:
                    data = json.loads(line)
                    txt = data[txt_var]
                    data_id = data[id_var]
                    if(data_id in valid_lang_ids):
                        txt_clean = txt.lower()
                        txt_clean = clean_txt_emojis(txt_clean)
                        txt_sents = sent_tokenize(txt_clean, language=lang_long)
                        # tokenize for matching
                        txt_sents = list(map(lambda x: ' %s '%(' '.join(tokenizer.tokenize(x))), txt_sents))
                        # try to match text
                        for sent in txt_sents:
#                             print(sent)
                            for word, query in word_query_pairs:
                                query_match = query.search(sent)
                                if(query_match is not None):
                                    logging.info('query match word=%s sent="%s"'%(word, sent))
                                    post_txt_samples.append([word, sent])
                        line_ctr += 1
                        if(line_ctr % 100000 == 0):
                            logging.info('processed %d lines, %d matches'%(line_ctr, len(post_txt_samples)))
                        if(line_ctr >= line_cutoff):
                            break
                except Exception as e:
                    print('data exception %s'%(e))
                    pass
    except Exception as e:
        logging.info('ending file processing')
    try:
        file_iter.close()
    except Exception as e:
        pass
    return post_txt_samples

def main():
    parser = ArgumentParser()
    parser.add_argument('phrase_file') # integrated verbs: ../../data/loanword_resources/wiktionary_twitter_reddit_loanword_verbs_integrated_verbs_query_phrases.tsv
    # light verbs: ../../data/loanword_resources/wiktionary_twitter_reddit_loanword_verbs_light_verbs_query_phrases.tsv
#     parser.add_argument('post_file')
    parser.add_argument('--es_year', type=int, default=2018)
    parser.add_argument('--es_start_month', type=int, default=7)
    parser.add_argument('--es_end_month', type=int, default=9)
    parser.add_argument('--es_cluster_name', default='reddit_comments')
    parser.add_argument('--es_dir', default='/hg190/elastic_search/es_instances_for_reddit_comments/')
    parser.add_argument('--out_dir', default='../../data/mined_reddit_comments/')
#     parser.add_argument('--lang_id_dir', default='lang_id')
    parser.add_argument('--lang', default='es')
    parser.add_argument('--txt_var', default='body')
    parser.add_argument('--id_var', default='id')
    args = vars(parser.parse_args())
#     post_file = args['post_file']
#     post_file_base = os.path.basename(post_file).split('.')[0]
    es_year = args['es_year']
    es_start_month = args['es_start_month']
    es_end_month = args['es_end_month']
    es_cluster_name = args['es_cluster_name']
    file_base = '%s_%d_m_%d_%d'%(es_cluster_name, es_year, es_start_month, es_end_month)
    logging_file = '../../output/sample_post_txt_by_phrase_%s.txt'%(file_base)
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    ## get loanword phrases
    phrase_file = args['phrase_file']
    phrase_file_base = os.path.basename(phrase_file).split('.')[0]
    phrase_data = pd.read_csv(args['phrase_file'], sep='\t', index_col=False)
    # add string buffer for matching
    phrase_data = phrase_data.assign(**{
        'verb' : phrase_data.loc[:, 'verb'].apply(lambda x: ' %s '%(x))
    })
    phrase_data = phrase_data.assign(**{
        'query_matcher' : phrase_data.loc[:, 'verb'].apply(re.compile)
    })
    word_query_pairs = list(zip(phrase_data.loc[:, 'loanword'].values, phrase_data.loc[:, 'query_matcher'].values))
    # get word + word list pairs
    # "tweet" / ["tuiteo", "tuitear"]
    paren_matcher = re.compile('(?<=\()[^\)]+(?=\))')
    phrase_data = phrase_data.assign(**{
        'verb_list' : phrase_data.loc[:, 'verb'].apply(lambda x:  paren_matcher.search(x).group(0).split('|'))
    })
    word_list_pairs = phrase_data.loc[:, ['loanword', 'verb_list']].values
    logging.warning('word queries: %s'%(word_query_pairs))
    
    ## query post files
    txt_var = args['txt_var']
    id_var = args['id_var']
    es_dir = args['es_dir']
    lang = args['lang']
#     post_txt_samples = sample_post_text(post_file, txt_var, id_var, lang, lang_id_dir, word_query_pairs)
#     post_txt_samples = pd.DataFrame(post_txt_samples, names=['word', 'context'])
    post_txt_samples = sample_post_text_from_ES(es_year, es_start_month, es_end_month, txt_var, lang, word_list_pairs, es_cluster_name=es_cluster_name, es_dir=es_dir, word_query_filters=None, sample_size=10)
    logging.info('%d post text samples'%(post_txt_samples.shape[0]))
    
    ## write to file
    es_index = '%s_%d_m_%d_%d'%(es_cluster_name, es_year, es_start_month, es_end_month)
    out_file = os.path.join(args['out_dir'], '%s_samples.tsv'%(es_index))
    post_txt_samples.to_csv(out_file, sep='\t', index=False)
    ## example ES query => faster!!
#     query = {
#     "query" : {
#         "bool" : {
#             # should == OR??
# #             "should" : [
# #                     {
# #                         "match" : {
# #                             "body" : "tweet"
# #                         }
# #                     },
# #                     {
# #                         "match" : {
# #                             "body" : "estar"
# #                         }
# #                     },
# #             ],
#             "must" : [
#                 {
#                     "match" : {
#                         "body" : "tweet",
#                     }
#                 },
# #                 {
# #                     "or" : [
# #                         {
# #                             "match" : {
# #                                 "body" : "comer"
# #                             }
# #                         },
# #                         {
# #                             "match" : {
# #                                 "body" : "estar"
# #                             }
# #                         },
# #                     ]
# #                 },
# #                 {
# #                     "match" : {
# #                         "body" : "beber agua"
# #                     }
# #                 },
#                 {
#                     "match" : {
#                         "lang" : "es"
#                     }
#                 }
#             ],
#         }
#     }
# }
    
if __name__ == '__main__':
    main()