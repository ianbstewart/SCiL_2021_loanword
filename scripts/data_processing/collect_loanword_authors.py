"""
Collect data for all authors who have used a 
loanword form (integrated or light verb).
author | text | matching loanword | integrated/light verb | id | date | lang | lang_score
TODO: convert to database storage ;_; to avoid repeated mining
"""
from argparse import ArgumentParser
import logging
import os
import pandas as pd
from data_helpers import execute_queries_all_instances
from functools import reduce
from math import ceil
import re

def generate_chunk_txt_queries(txt_vals, txt_var='text', lang='es', max_chunk_size=100):
    """
    Generate chunk queries to match text values and specified language.
    
    """
    chunk_count = int(ceil(len(txt_vals) / max_chunk_size))
    queries = []
    for i in range(chunk_count):
        txt_val_chunk = txt_vals[(i*max_chunk_size):((i+1)*max_chunk_size)]
        txt_val_chunk_str = '|'.join(txt_val_chunk)
        query = {
            'query' : {
                'bool' : {
                    'must' : [
                        {
                            'match' : {
                                txt_var : txt_val_chunk_str
                            }
                        },
                        {
                            'match' : {
                                'lang' : lang
                            }
                        }
                    ]
                }
            }
        }
        
#             {
#                 'match' : {
#                     txt_var : txt_val_chunk_str
#                 }
#             }
#         }
        queries.append(query)
    return queries

def generate_begin_mid_end(txt):
    begin = f'^{txt} '
    mid = f' {txt} '
    end = f' {txt}$'
    return [begin, mid, end]

def main():
    parser = ArgumentParser()
    parser.add_argument('--es_cluster_name', default='twitter_posts')
    parser.add_argument('--loanword_integrated_data', default='../../data/loanword_resources/wiktionary_twitter_reddit_loanword_verbs_integrated_verbs_query_phrases.tsv')
    parser.add_argument('--loanword_phrase_data', default='../../data/loanword_resources/wiktionary_twitter_reddit_loanword_verbs_light_verbs_query_phrases.tsv')
    parser.add_argument('--out_dir', default='../../data/mined_tweets/')
    args = vars(parser.parse_args())
    es_cluster_name = args['es_cluster_name']
    logging_file = f'../../output/collect_loanword_authors_{es_cluster_name}.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    ## get loanword verbs and phrases
    loanword_integrated_data = pd.read_csv(args['loanword_integrated_data'], sep='\t', index_col=False)
    loanword_phrase_data = pd.read_csv(args['loanword_phrase_data'], sep='\t', index_col=False)
    # extract verb list
    integrated_verb_matcher = re.compile('(?<=^\()[\w\|]+(?=\)$)')
    loanword_integrated_data = loanword_integrated_data.assign(**{
        'verb_list' : loanword_integrated_data.loc[:, 'verb'].apply(lambda x: integrated_verb_matcher.search(x).group(0).split('|'))
    })
    # get integrated verbs, phrases, flatten
    integrated_verb_loanword_lookup = {v : k for k,vs in zip(loanword_integrated_data.loc[:, 'loanword'].values, loanword_integrated_data.loc[:, 'verb_list'].values) for v in vs}
    loanword_integrated_verbs = list(reduce(lambda x,y: x+y, loanword_integrated_data.loc[:, 'verb_list'].values))
    # make sure verbs can only occur in specified positions
    loanword_integrated_verb_phrases = list(reduce(lambda x,y: x+y, map(generate_begin_mid_end, loanword_integrated_verbs)))
    loanword_integrated_verb_matcher = re.compile('|'.join(loanword_integrated_verb_phrases))
#     print('integrated verb matcher \n%s'%(loanword_integrated_verb_matcher.pattern))
    # to find light verb phrases: (1) match noun; (2) verify phrase match with regular expression
    light_verb_noun_matcher = re.compile('\w+$|(?<=\()[\w\|\-]+(?=\)$)')
    loanword_light_verb_phrase_nouns = loanword_phrase_data.loc[:, 'verb'].apply(lambda x: light_verb_noun_matcher.search(x).group(0))
    # flatten in case of multiple possible nouns per loanword, e.g. "vape|vaping"
    loanword_light_verb_phrase_nouns_flat = []
    light_verb_noun_loanword_lookup = {}
    light_verb_noun_phrase_matcher_lookup = {}
    for noun, loanword, phrase in zip(loanword_light_verb_phrase_nouns, loanword_phrase_data.loc[:, 'loanword'].values, loanword_phrase_data.loc[:, 'verb'].values):
        for noun_i in noun.split('|'):
            loanword_light_verb_phrase_nouns_flat.append(noun_i)
#             light_verb_noun_phrase_lookup[noun_i] = phrase
            light_verb_noun_phrase_matcher_lookup[noun_i] = re.compile(phrase)
            light_verb_noun_loanword_lookup[noun_i] = loanword
    light_verb_noun_matcher = re.compile('|'.join(loanword_light_verb_phrase_nouns_flat))
#     light_verb_noun_phrase_lookup = dict(zip(loanword_light_verb_phrase_nouns, loanword_phrase_data.loc[:, 'verb'].values))
#     print(f'lookup=\n{light_verb_noun_loanword_lookup}')
    
    # create queries
    if(es_cluster_name == 'twitter_posts'):
        txt_var = 'text'
        author_var = 'user_screen_name'
        author_id_var = 'user_id'
        id_var = 'id'
        date_var = 'created_at'
        author_descriptive_data_fields = ['user_description', 'user_location']
    else:
        txt_var = 'body'
        author_var = 'author'
        author_id_var = 'author_id'
        id_var = 'id'
        date_var = 'date'
        author_descriptive_data_fields = []    
    max_chunk_size = 100
    lang = 'es'
    integrated_verb_queries = generate_chunk_txt_queries(loanword_integrated_verbs, txt_var=txt_var, lang=lang, max_chunk_size=max_chunk_size)
    light_verb_noun_queries = generate_chunk_txt_queries(loanword_light_verb_phrase_nouns_flat, txt_var=txt_var, lang=lang, max_chunk_size=max_chunk_size)
    
    ## query for author counts
    ## AND author info! bio/location/description
    ## store: LOANWORD_QUERY_STR,AUTHOR,COUNT,TIME,LOCATION,DESCRIPTION
    # author | text | matching loanword | integrated/light verb phrase | verb type (integrated/light) | id | date | lang | lang_score
    # define data
    # TODO: add ID var to avoid duplicate tweets collected later
    result_author_data_fields = [author_var, author_id_var, txt_var, date_var, id_var] + author_descriptive_data_fields
    result_data_fields = ['loanword', 'loanword_verb', 'loanword_type',] + result_author_data_fields
    result_text_fields = [txt_var] + author_descriptive_data_fields
    non_original_post_matcher = re.compile('RT @[a-zA-Z0-9]+')
    
    ## integrated verbs
#     es_year_month_pairs = [(2017, 7, 9)]
    es_year_month_pairs = [(2017, 1, 3), (2017, 4, 6), (2017, 7, 9), (2017, 10, 12), (2018, 1, 3), (2018, 4, 6), (2018, 7, 9), (2018, 10, 12), (2019, 1, 3), (2019, 4, 6), (2019, 7, 9), (2019, 10, 12)]
#     es_year_month_pairs = [(2018, 7, 9), (2018, 10, 12), (2019, 1, 3), (2019, 4, 6)]
    integrated_verb_query_results = execute_queries_all_instances(integrated_verb_queries, es_year_month_pairs=es_year_month_pairs, es_cluster_name=es_cluster_name, verbose=False)
    # collect counts
    integrated_verb_author_data = []
    verb_type = 'integrated_loanword'
    for es_year_month_pair, integrated_verb_query_result in zip(es_year_month_pairs, integrated_verb_query_results):
        es_year_month_pair_str = '%d_m_%d_%d'%(es_year_month_pair[0], es_year_month_pair[1], es_year_month_pair[2])
        for result_list in integrated_verb_query_result:
            for result in result_list:
                result_txt = result[txt_var]
                loanword_verb_match = loanword_integrated_verb_matcher.search(result_txt)
                non_original_post_match = non_original_post_matcher.search(result_txt)
                if(loanword_verb_match is not None and non_original_post_match is None):
                    loanword_verb = loanword_verb_match.group(0).strip()
                    loanword = integrated_verb_loanword_lookup[loanword_verb]
                    result_relevant_data = list(map(result.get, result_author_data_fields))
                    combined_result_data = [loanword, loanword_verb, verb_type,] + result_relevant_data
                    integrated_verb_author_data.append(combined_result_data)
    integrated_verb_author_data = pd.DataFrame(integrated_verb_author_data, columns=result_data_fields)
    integrated_verb_author_data.fillna("", inplace=True)
    out_dir = args['out_dir']
    ## clean text vars!
    RETURN_CHAR_MATCHER = re.compile('[\n\r\t]')
    for result_text_field in result_text_fields:
        integrated_verb_author_data = integrated_verb_author_data.assign(**{
            result_text_field : integrated_verb_author_data.loc[:, result_text_field].apply(lambda x: RETURN_CHAR_MATCHER.sub('', x))
        })
    start_date_str = '%d_%d_%d'%(es_year_month_pairs[0])
    end_date_str = '%d_%d_%d'%(es_year_month_pairs[-1])
    integrated_verb_out_file = os.path.join(out_dir, f'loanword_integrated_verb_posts_CLUSTER={es_cluster_name}_STARTDATE={start_date_str}_ENDDATE={end_date_str}.tsv')
    integrated_verb_author_data.to_csv(integrated_verb_out_file, sep='\t', index=False)
    
    ## light verbs
    es_year_month_pairs = [(2017, 7, 9), (2017, 10, 12), (2018, 1, 3), (2018, 4, 6), (2018, 7, 9), (2018, 10, 12), (2019, 1, 3), (2019, 4, 6)]
#     es_year_month_pairs = [(2018, 7, 9), (2018, 10, 12), (2019, 1, 3), (2019, 4, 6)]
    light_verb_noun_query_results = execute_queries_all_instances(light_verb_noun_queries, es_year_month_pairs=es_year_month_pairs, es_cluster_name=es_cluster_name)
    light_verb_author_data = []
    verb_type = 'light_verb_loanword'
    for light_verb_noun_query_result, es_year_month_pair in zip(light_verb_noun_query_results, es_year_month_pairs):
        for result_list in light_verb_noun_query_result:
            for result in result_list:
                result_txt = result[txt_var].lower()
#                 result_author = result[author_var]
#                 result_author_id = result[author_id_var]
#                 result_author_descriptive_data = list(map(lambda x: result.get(x), author_descriptive_data_fields))
# #                 clean text
#                 result_author_descriptive_data = list(map(lambda x: x.replace('\n','') if type(x) is str else x, result_author_descriptive_data))
                noun_match = light_verb_noun_matcher.search(result_txt)
                if(noun_match is not None):
                    noun = noun_match.group(0)
                    phrase_matcher = light_verb_noun_phrase_matcher_lookup[noun]
                    phrase_match = phrase_matcher.search(result_txt)
                    non_original_post_match = non_original_post_matcher.search(result_txt)
                    if(phrase_match is not None and non_original_post_match is None):
                        loanword = light_verb_noun_loanword_lookup[noun]
                        loanword_verb = phrase_match.group(0)
                        result_relevant_data = list(map(result.get, result_author_data_fields))
                        combined_result_data = [loanword, loanword_verb, verb_type,] + result_relevant_data
                        light_verb_author_data.append(combined_result_data)
    light_verb_author_data = pd.DataFrame(light_verb_author_data, columns=result_data_fields)
    # cleanup 
    light_verb_author_data.fillna("", inplace=True)
    ## clean text vars!
    RETURN_CHAR_MATCHER = re.compile('[\n\r\t]')
    for result_text_field in result_text_fields:
        light_verb_author_data = light_verb_author_data.assign(**{
            result_text_field : light_verb_author_data.loc[:, result_text_field].apply(lambda x: RETURN_CHAR_MATCHER.sub('', x))
        })
    out_dir = args['out_dir']
    start_date_str = '%d_%d_%d'%(es_year_month_pairs[0])
    end_date_str = '%d_%d_%d'%(es_year_month_pairs[-1])
    light_verb_out_file = os.path.join(out_dir, f'loanword_light_verb_posts_CLUSTER={es_cluster_name}_STARTDATE={start_date_str}_ENDDATE={end_date_str}.tsv')
    light_verb_author_data.to_csv(light_verb_out_file, sep='\t', index=False)
    
    ## combine files
    combined_out_file = os.path.join(out_dir, f'loanword_verb_posts_CLUSTER={es_cluster_name}_STARTDATE={start_date_str}_ENDDATE={end_date_str}.tsv')
    combined_author_data = pd.concat([integrated_verb_author_data, light_verb_author_data], axis=0)
    combined_author_data.to_csv(combined_out_file, sep='\t', index=False)
    
if __name__ == '__main__':
    main()