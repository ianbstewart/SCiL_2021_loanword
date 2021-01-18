"""
Extract word counts from Google Ngrams.
This assumes that you have already downloaded this package 
to the working directory:
https://github.com/econpy/google-ngrams
"""
from argparse import ArgumentParser
import logging
import os
import sys
if('google-ngrams' not in sys.path):
    sys.path.append('google-ngrams')
from getngrams import getNgrams
from data_helpers import conjugate_verb, conjugate_light_verb
import pandas as pd

from time import sleep
# # test query
# import pandas as pd
def extract_ngram_counts(query, corpus='spa_2012', start_year=1980, end_year=2012, smooth=3):
    success = False
    MAX_QUERY_COUNT = 2
    QUERY_SLEEP_TIME = 600 # min sleep time = 10 mins?? 103*5 sec = 515 sec ~ 10 min
    query_ctr = 0
    while(not success and query_ctr < MAX_QUERY_COUNT):
        query_results = getNgrams(query, corpus, start_year, end_year, smooth, caseInsensitive=True)
        # empty data => failed search (because rate limit?)
        success = query_results[2].shape[0]!=0
        if(not success):
#             if(verbose):
            logging.info(f'bad query, sleeping for {QUERY_SLEEP_TIME} sec')
            sleep(QUERY_SLEEP_TIME)
            query_ctr += 1
    logging.info(f'results={query_results}')
    query_counts = query_results[2]
    year_col = 'year'
    # if no results, add 0 counts
    NULL_COUNT = 0.
    if(query_counts.shape[0] == 0):
        year_range = list(range(start_year, end_year+1))
        query_counts = query_counts.assign(**{
            year_col : year_range
        })
    if(query_counts.shape[1] == 1):
        query_counts = query_counts.assign(**{
            query : NULL_COUNT
        })
        
    # restrict to combined query
    # either (All) or _INF (infinitive)
    elif(query_counts.shape[1] > 2):
        query_col = list(filter(lambda x: '(All)' in x or '_INF' in x, query_counts.columns))[0]
        query_counts = query_counts.loc[:, [year_col, query_col]]
    query_counts.columns = [year_col, query]
    # reshape to be less dumb
    query_counts = pd.melt(query_counts, value_vars=[query], id_vars=[year_col], value_name='count', var_name='query')
    return query_counts

def query_all_forms(query_list):
    """
    Query for each phrase form, then combine total counts per-year.
    """
    combined_query_counts = []
    corpus = 'spa_2012'
    start_year = 1980
    end_year = 2008 # 2008-2012 shows decline for all words...bad
    smooth = 3
    for query in query_list:
        query_counts = extract_ngram_counts(query, corpus=corpus, start_year=start_year, end_year=end_year, smooth=smooth)
        combined_query_counts.append(query_counts)
    combined_query_counts = pd.concat(combined_query_counts, axis=0)
    time_var = 'year'
    count_var = 'count'
    agg_query_counts = combined_query_counts.groupby(time_var).apply(lambda x: x.loc[:, count_var].sum())
    return agg_query_counts

def query_and_clean(data, query_col='integrated_verb_queries', query_type='integrated_verb'):
    """
    Query word forms and clean data.
    """
    query_data = query_all_forms(data.loc[query_col])
    query_data = query_data.reset_index().rename(columns={0:'count'}).assign(**{'query': data.loc['word'], 'type' : query_type})
    return query_data

def main():
    parser = ArgumentParser()
    parser.add_argument('loanword_data') # ../../data/loanword_resources/wiktionary_twitter_reddit_loanword_integrated_verbs_light_verbs.tsv
    parser.add_argument('native_verb_data') # ../../data/loanword_resources/native_verb_light_verb_pairs.csv
    parser.add_argument('--out_dir', default='../../data/google_ngram_data/')
    args = vars(parser.parse_args())
    logging_file = '../../output/extract_counts_from_google_ngrams.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    ## load query data
    loanword_verb_query_data = pd.read_csv(args['loanword_data'], sep='\t')
    native_verb_query_data = pd.read_csv(args['native_verb_data'], sep=',')
    # combine
    loanword_verb_query_data.rename(columns={'loanword' : 'word'}, inplace=True)
    native_verb_query_data = native_verb_query_data.assign(**{
        'word' : native_verb_query_data.loc[:, 'integrated_verb'].values
    })
    loanword_verb_query_data = loanword_verb_query_data.assign(**{
        'word_type' : 'loanword',
    })
    native_verb_query_data = native_verb_query_data.assign(**{
        'word_type' : 'native_verb',
    })
    combined_query_data = pd.concat([loanword_verb_query_data, native_verb_query_data])
    # conjugate integrated, light verbs
    combined_query_data = combined_query_data.assign(**{
        'integrated_verb_queries' : combined_query_data.loc[:, 'integrated_verb'].apply(conjugate_verb)
    })
    # filter ambiguous verb forms
    ambiguous_integrated_verbs = ['accesar', 'auditar', 'boxear', 'chequear', 'formear', 'frizar']
    for verb_i in ambiguous_integrated_verbs:
        verb_forms_i = [verb_i.replace('ar', 'o'), verb_i.replace('ar', 'a')]
        data_i = combined_query_data[combined_query_data.loc[:, 'integrated_verb']==verb_i]
        data_i = data_i.assign(**{
            'integrated_verb_queries' : list(filter(lambda x: x not in verb_forms_i, data_i.loc[:, 'integrated_verb_queries']))
        })
    combined_query_data = combined_query_data.assign(**{
        'light_verb_queries' : combined_query_data.loc[:, 'light_verb'].apply(conjugate_light_verb)
    })
    # get clean light verb queries
    # hago acces => hacer_INF acces
    # faster queries!!
    combined_query_data = combined_query_data.assign(**{
        'light_verb_queries_INF' : combined_query_data.loc[:, 'light_verb'].apply(lambda x: conjugate_light_verb(x, add_inf=True))
    })
    
    ## get counts!
    integrated_verb_query_col = 'integrated_verb_queries'
    integrated_verb_query_type = 'integrated_verb'
    light_verb_query_col = 'light_verb_queries_INF'
    light_verb_query_type = 'light_verb'
    light_verb_query_counts = combined_query_data.apply(lambda x: query_and_clean(x, query_col=light_verb_query_col, query_type=light_verb_query_type), axis=1)
    integrated_verb_query_counts = combined_query_data.apply(lambda x: query_and_clean(x, query_col=integrated_verb_query_col, query_type=integrated_verb_query_type), axis=1)
    # combine
    query_counts = pd.concat([integrated_verb_query_counts, light_verb_query_counts], axis=0)
    
    ## write to file
    out_dir = args['out_dir']
    query_file_name = os.path.join(out_dir, 'google_ngram_loanword_native_verb_counts.tsv')
    query_counts.to_csv(query_file_name, sep='\t', index=False)

if __name__ == '__main__':
    main()