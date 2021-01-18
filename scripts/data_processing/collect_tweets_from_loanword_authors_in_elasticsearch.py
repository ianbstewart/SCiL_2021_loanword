"""
Collect tweets from loanword authors via Elasticsearch.
"""
from argparse import ArgumentParser
import logging
from data_helpers import generate_chunk_queries
from data_helpers import execute_queries_all_instances
import os
import pandas as pd
import re

def main():
    parser = ArgumentParser()
    parser.add_argument('author_data') # ../../data/mined_tweets/loanword_verb_posts_CLUSTER=twitter_posts_STARTDATE=2017_7_9_ENDDATE=2019_4_6.tsv
    parser.add_argument('--es_cluster_name', default='twitter_posts')
    parser.add_argument('--out_dir', default='../../data/mined_tweets/loanword_author_tweets_elasticsearch/')
    args = vars(parser.parse_args())
    logging_file = '../../output/collect_tweets_from_loanword_authors_in_elasticsearch.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    ## load author data
    author_data_file = args['author_data']
    author_data = pd.read_csv(author_data_file, sep='\t')
    author_var = 'user_screen_name'
    author_list = author_data.loc[:, author_var].apply(lambda x: x.lower()).unique()
    id_var = 'id'
    loanword_post_ids = author_data.loc[:, id_var]
    
    ## generate queries
    es_cluster_name = args['es_cluster_name']
    if(es_cluster_name == 'twitter_posts'):
        txt_var = 'text'
        author_var = 'user_screen_name'
    else:
        txt_var = 'body'
        author_var = 'author'
    extra_query_params = {'lang' : 'es'}
    author_queries = generate_chunk_queries(author_list, extra_query_params=extra_query_params, MAX_QUERY_CHUNK_SIZE=50, search_var=author_var)
    logging.info(f'generated author queries {author_queries}')
    
    ## mine that shit
    # tmp debugging
#     es_year_month_pairs = [(2017, 7, 9),]
    es_year_month_pairs = [(2017, 7, 9), (2017, 10, 12), (2018, 1, 3), (2018, 4, 6), (2018, 7, 9), (2018, 10, 12), (2019, 1, 3), (2019, 4, 6)]
    author_results = execute_queries_all_instances(author_queries, es_year_month_pairs=es_year_month_pairs, es_cluster_name=es_cluster_name, verbose=False)
#     author_result_data = []
#     for query_results in author_results:
#         for query_result_list in query_results:
#             author_result_data += list(map(pd.Series, query_result_list))
#     author_results = pd.concat(author_result_data, axis=1).transpose()
    author_result_data = pd.DataFrame(author_results[0][0])
    author_results.fillna('', inplace=True)
    # simplify
    ## clean text vars!
    RETURN_CHAR_MATCHER = re.compile('[\n\r\t]')
    all_txt_vars = [txt_var, 'user_description', 'user_location']
    for txt_var_i in all_txt_vars:
        author_results = author_results.assign(**{
            txt_var_i : author_results.loc[:, txt_var_i].apply(lambda x: RETURN_CHAR_MATCHER.sub('', x))
        })
    # remove existing posts
    author_results = author_results[~author_results.loc[:, 'id'].isin(loanword_post_ids)]

    ## save to file
    # different file per author to match current setup ;_;
    out_dir = args['out_dir']
    for author_i, data_i in author_results.groupby(author_var):
        logging.info(f'writing data for author={author_i}')
        out_file_i = os.path.join(out_dir, f'{author_i}_tweets.gz')
        data_i.to_csv(out_file_i, sep='\t', compression='gzip', index=False)
        
    ## also write lang ID data so that we don't have to tag it later
    lang_id_data = author_results.loc[:, ['id', 'lang', 'lang_score']]
    lang_id_data_file = os.path.join(out_dir, 'lang_id.gz')
    lang_id_data.to_csv(lang_id_data_file, sep='\t', compression='gzip', index=False)
    
if __name__ == '__main__':
    main()