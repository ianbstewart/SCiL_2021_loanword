"""
Collect subreddits based on topic assigned by Snoop Snoo.
e.g. https://www.snoopsnoo.com/subreddits/entertainment/celebrities/
WARNING: the membership counts are outdated.
"""
from argparse import ArgumentParser
import logging
import os
from bs4 import BeautifulSoup
import requests
import pandas as pd

def main():
    parser = ArgumentParser()
    parser.add_argument('--subreddit_list_website', default='https://www.snoopsnoo.com/subreddits/')
    parser.add_argument('--subreddit_topic', default='entertainment/celebrities/')
    parser.add_argument('--out_dir', default='../../data/mined_reddit_comments/')
    args = vars(parser.parse_args())
    logging_file = '../../output/collect_subreddits_by_topic.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.DEBUG)

    ## get page info
    has_next_page = True
    subreddit_data = []
    subreddit_topic_str = args['subreddit_topic'].replace('/', '_')
    if(subreddit_topic_str.endswith('_')):
        subreddit_topic_str = subreddit_topic_str[:-1]
    subreddit_list_website_base = '%s%s'%(args['subreddit_list_website'], args['subreddit_topic'])
    subreddit_list_website = subreddit_list_website_base
    while(has_next_page):
        logging.debug('processing page %s'%(subreddit_list_website))
        page_raw_html = requests.get(subreddit_list_website).text
        page_soup = BeautifulSoup(page_raw_html, features="lxml")
        subreddit_listings = page_soup.find_all('div', class_='panel panel-default subreddit-listing')
        for listing in subreddit_listings:
            subreddit_name = listing.a.text.strip()
            subreddit_title = listing.find_all('div', class_='title')[0].text
            subreddit_subscribers = int(listing.find_all('li', class_='margin-btm-10')[0].text.strip().replace(' subscribers', '').replace(',', ''))
            logging.debug('got subreddit %s'%(subreddit_name))
            subreddit_data.append([subreddit_name, subreddit_title, subreddit_subscribers])
        next_page_url_matches = page_soup.find_all('li', class_='next')
        if(len(next_page_url_matches) > 0):
#             print('next page %s'%(next_page_url_matches[0]))
            next_page_url = next_page_url_matches[0].a['href']
            subreddit_list_website = f'{subreddit_list_website_base}{next_page_url}'
        else:
            has_next_page = False
    subreddit_data = pd.DataFrame(subreddit_data, columns=['name', 'title', 'subscribers'])
    logging.debug(subreddit_data.head())
    
    ## save to file
    subreddit_data_file_name = os.path.join(args['out_dir'], '%s_subreddits.tsv'%(subreddit_topic_str))
    subreddit_data.to_csv(subreddit_data_file_name, sep='\t', index=False)
    
if __name__ == '__main__':
    main()