"""
Query ES instance for 
specific text/user/lang/etc.
"""
from argparse import ArgumentParser
import logging
import os
from elasticsearch import Elasticsearch

"""
sample query: all users in subreddit "fulbo"

TODO: TEST!!
query = {
    "size" : 0,
    "filtered" : {"filter" : {"term" : {"subreddit" : "fulbo"}}},
    "aggs" : {
        "author" : {
            "terms" : {"field" : "author", "size" : 1000000},
        }
    }
 }
"""

def main():
    parser = ArgumentParser()
    parser.add_argument('ES_index')
    parser.add_argument('--query_file', default=None)
    parser.add_argument('--lang', default=None)
    parser.add_argument('--phrases', nargs='+', default=None)
    args = vars(parser.parse_args())
    logging_file = '../../output/query_ES_instance.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.DEBUG)

    ## build filter query
#     filter_key_vals = {'filtered' : {'filter' : {'term' : {}}}}
#     match_key_vals = {}
#     if(args.get('lang') is not None):
#         filter_key_vals['filtered']['filter']['term']['lang'] = args['lang']
#     if(args.get('phrases') is not None):
#         phrase_query_str = ' OR '.join(map(lambda x: '(%s)'%(x), args['phrases']))
#         match_key_vals = {'query_string' : {'query' : phrase_query_str, 'default_field':'body'}}
    ES_TIMEOUT=600
    es = Elasticsearch(timeout=ES_TIMEOUT)
    query_match_vals = []
    exact_match_args = ['lang', 'subreddit', 'user']
    for exact_match_arg in exact_match_args:
        if(args.get(exact_match_arg) is not None):
            query_match_vals.append({ 'term' : { exact_match_arg : args[exact_match_arg] } })
    contain_match_args = ['phrases']
    arg_query_var_lookup = {
        'phrases' : 'body'
    }
    for contain_match_arg in contain_match_args:
        if(args.get(contain_match_arg) is not None):
            contain_match_str = ' OR '.join(list(map(lambda x: '(%s)'%(x), args[contain_match_arg])))
            if(contain_match_arg in arg_query_var_lookup):
                query_var = arg_query_var_lookup[contain_match_arg]
            else:
                query_var = contain_match_arg
            query_match_vals.append({ 'match' :  { query_var : contain_match_str}})
#     print(query_key_vals)
#     query = { #aggregate query
#         "query": query_key_vals,
# #             "filtered": {
# #                 "filter": { 
# #                     "term": filter_key_vals
# #                 }
# #             }
#       "aggs": { "link_id": { "terms": { "field": "link_id", "size":0} } }
#       , "size": 0
#     }
    query = {
        "query" : {
            "bool" : {
                "must" : query_match_vals
            }
        }
    }
#     query = {
#         "query" :{
#             "bool" : {
#                 "must" : [
#                     {
#                         "term" : {
#                             "lang" : "en"
#                         }
#                     },
#                     {
#                         "match" : {
#                             "body" : "(reddit) OR (spaghetti)"
#                         }
#                     }
#                 ]
#             }
#         }
#     }
    print(query)
    
    ## run query
    ES_index = args['ES_index']
    es_results = es.search(index=ES_index, body=query)
    logging.debug('query test results %s'%(es_results))
    
if __name__ == '__main__':
    main()