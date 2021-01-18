"""
Collect screen names for people/groups,
e.g. musicians.
Uses data from Wikipedia collected 
in collect_pages_from_wikipedia_by_category.sh
"""
from argparse import ArgumentParser
import logging
import os
import pandas as pd
from data_helpers import load_twitter_API
from unidecode import unidecode
from time import sleep

def clean_txt(txt):
    return unidecode(txt.lower())

def search_twitter_for_names(names, twitter_api):
    """
    Search Twitter for names.
    We restrict the search results to
    (1) verified accounts and (2) accounts whose
    reported name matches the search name.
    We then take the top result as sorted
    by follower count.
    High precision, possibly lower recall.
    """
    combined_search_results = []
    SEARCH_FAIL_LIMIT = 5
    RATE_LIMIT_SLEEP_TIME=60
    for name in names:
        clean_name = clean_txt(name)
        search_success = False
        search_fails = 0
        search_results  = []
        while(not search_success and search_fails < SEARCH_FAIL_LIMIT):
            try:
                search_results = twitter_api.GetUsersSearch(term=name)
                search_success = True
            # catch rate-limiting
            except Exception as e:
#                 if(e['code'] == 88):
                logging.info(f'error {e}')
                logging.info(f'hit limit; {search_fails} fails; sleeping for {RATE_LIMIT_SLEEP_TIME} sec')
                search_fails += 1
                sleep(RATE_LIMIT_SLEEP_TIME)
        # only keep verified accounts
        search_results = list(filter(lambda x: x.verified, search_results))
        # only keep name matches
        search_results = list(filter(lambda x: clean_txt(x.name) == clean_name, search_results))
        # sort by followers
        search_results = list(sorted(search_results, key=lambda x: x.followers_count, reverse=True))
        if(len(search_results) > 0):
            most_likely_match = search_results[0]
            combined_search_results.append([name, most_likely_match.screen_name, most_likely_match.followers_count])
        else:
            logging.info(f'name {name} had 0 results')
    combined_search_results = pd.DataFrame(combined_search_results, columns=['name', 'screen_name', 'followers'])
    return combined_search_results

def main():
    parser = ArgumentParser()
    parser.add_argument('name_file')
    parser.add_argument('--auth_file', default='../../data/mined_tweets/twitter_auth.csv')
    parser.add_argument('--out_dir', default='../../culture_metadata')
    args = vars(parser.parse_args())
    logging_file = '../../output/collect_twitter_screen_names.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    ## load names
    name_file = args['name_file']
    if(name_file.endswith('.tsv')):
        name_base = os.path.basename(name_file).replace('_wiki_data', '').replace('.tsv', '')
        name_data = pd.read_csv(name_file, sep='\t', index_col=False)
        names = name_data.loc[:, 'name'].values.tolist()
        logging.info('%d names: %s'%(len(names), str(names[:10])))
    elif(name_file.endswith('.txt')):
        names = list(map(lambda x: x.strip(), open(name_file, 'r')))
    
    ## mine twitter
    twitter_api = load_twitter_API(args['auth_file'])
    screen_name_data = search_twitter_for_names(names, twitter_api)
    
    ## write to file
    out_file = os.path.join(args['out_dir'], f'{name_base}_twitter_screen_names.tsv')
    screen_name_data.to_csv(out_file, sep='\t', index=False)
    
if __name__ == '__main__':
    main()