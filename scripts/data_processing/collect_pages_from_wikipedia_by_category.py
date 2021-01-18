"""
Collect Wikipedia pages according
to U.S. and Latin American categories.

TODO: use DBPedia? would be cleaner but maybe less interpretable http://dbpedia.org/sparql/ 
something like this
**PREFIX  dct: <http://purl.org/dc/terms/> select ?name where {?person foaf:name ?name . ?person dct:subject dbc:American_female_pop_singers. } LIMIT 100 **
** select ?name where {?person foaf:name ?name . ?person dbo:birthPlace dbr:Colombia . ?person rdf:type dbo:MusicalArtist} LIMIT 100 **
TODO: add birth year restriction
"""
from argparse import ArgumentParser
import logging
import os
from urllib import request
from urllib import parse as url_parse
from bs4 import BeautifulSoup
import re
from functools import reduce
import pandas as pd

def clean_url(url):
    """
    Clean URL for Unicode characters.
    """
    scheme, netloc, path, query, fragment = url_parse.urlsplit(url)
    path = url_parse.quote(path)
    url = url_parse.urlunsplit((scheme, netloc, path, query, fragment))
    return url

def div_contains_category_members(div, div_class='mw-content-ltr'):
    """
    Determine that div contains category 
    members AND that it's not recursive.
    """
    div_list_children = div.find('ul').find_all('li')
    test_child = list(div_list_children[0].children)[0]
    div_is_recursive = len(div.find_all('div', class_=div_class)) > 0
    # tmp debugging
#     print(list(div_list_children[0].children))
#     print(f'test child {test_child}')
#     print('test child dir %s'%(','.join(dir(test_child))))
#     if(test_child.get('title') is not None):
#         print(f'test child has title=%s text=%s'%(test_child.get('title'), test_child.text))
    contains_category_members = test_child.name == 'a' and test_child.get('title') == test_child.text and not div_is_recursive
    return contains_category_members
MAX_TITLE_LEN = 30
PAREN_MATCHER = re.compile(' \(.+\)$')
def extract_category_members(div):
    """
    Extract URL and name.
    """
    category_members = div.find_all('a')
    # remove any members with class
    category_members = list(filter(lambda x: x.get('class') is None, category_members))
    category_member_urls = list(map(lambda x: x['href'], category_members))
    for category_member in category_members:
        if(category_member.get('title') is None):
            print(f'bad category member {category_member}')
    category_member_names = list(map(lambda x: x['title'], category_members))
    # remove parens
    category_member_names = list(map(lambda x: PAREN_MATCHER.sub('', x), category_member_names))
    # combine
    category_member_info = list(zip(category_member_names, category_member_urls))
    # remove long names like 'Lo Nuestro Award for Pop New Artist of the Year'
    category_member_info = list(filter(lambda x: len(x[1]) <= MAX_TITLE_LEN, category_member_info))
    return category_member_info

def extract_category_pages(category, wiki_lang='en'):
    full_wiki_base_url = f'https://{wiki_lang}.wikipedia.org/wiki/'
    mini_wiki_base_url = f'https://{wiki_lang}.wikipedia.org/'
    category_members_combined = []
    has_next_page = True
    category_url = f'{full_wiki_base_url}Category:{category}'
#     div_class = 'mw-category-group' # can't use!! some pages don't have it ;_;
    div_class = 'mw-content-ltr'
    print('category=%s'%(category))
    while(has_next_page):
        # clean category URL
        clean_category_url = clean_url(category_url)
        res = request.urlopen(clean_category_url).read().decode('utf-8')
        soup = BeautifulSoup(res, features='lxml')
        divs = soup.find_all(class_=div_class)
        ## collect category members
#         print(f'divs = {divs}')
        divs_clean = list(filter(lambda x: div_contains_category_members(x, div_class=div_class), divs))
#         print(f'divs clean = {divs_clean}')
        if(len(divs_clean) > 0):
            category_members = list(reduce(lambda x,y: x+y, map(lambda x: extract_category_members(x), divs_clean)))
            category_members_combined += category_members
        ## check for next page
        next_page_ref = soup.find('a', text='next page')
        has_next_page = next_page_ref is not None and next_page_ref.get('href') is not None
        if(has_next_page):
            next_page_url = next_page_ref['href']
            category_url = f'{mini_wiki_base_url}{next_page_url}'
    category_members_data = pd.concat(list(map(lambda x: pd.DataFrame(x, index=['name', 'wiki_url']).transpose(), category_members_combined)), axis=0)
    return category_members_data

def main():
    parser = ArgumentParser()
    parser.add_argument('page_categories', nargs='+') 
    parser.add_argument('--wiki_lang', default='en')
    parser.add_argument('--out_dir', default='../../data/wiki_data/')
    parser.add_argument('--page_category_group', default='')
    # Latin American (en): Argentine_pop_singers, Brazilian_pop_singers, Chilean_pop_singers, Colombian_pop_singers, Ecuadorian_pop_singers, Mexican_pop_singers, Uruguayan_pop_singers, Latin_music_songwriters
    # Latin American (es): Cantantes_de_pop_de_Argentina, Cantantes_de_pop_de_Brasil, Cantantes_de_pop_de_Chile, Cantantes_de_pop_de_Colombia, Cantantes_de_pop_de_Cuba, Cantantes_de_pop_de_Guatemala, Cantantes_de_pop_de_México, Cantantes_de_pop_de_Perú, Cantantes_de_pop_de_la_República_Dominicana, Cantantes_de_pop_de_Venezuela
    # US American: American_male_pop_singers, American_female_pop_singers
    args = vars(parser.parse_args())
    logging_file = '../../output/collect_artists_from_wikipedia.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    ## load categories
    page_categories = args['page_categories']
    wiki_lang = args['wiki_lang']
    
    ## mine list of pages from categories
    combined_category_data = []
    for page_category in page_categories:
        category_page_data = extract_category_pages(page_category, wiki_lang=wiki_lang)
        # add category as data
        category_page_data = category_page_data.assign(**{
            'page_category' : page_category
        })
        combined_category_data.append(category_page_data)
    combined_category_data = pd.concat(combined_category_data, axis=0)
    # deduplicate => this erases data about multiple categories but NEED TO SIMPLIFY
    combined_category_data.drop_duplicates('name', inplace=True)
    
    ## write to file
    out_dir = args['out_dir']
    page_category_group = args['page_category_group']
    if(page_category_group != ''):
        out_file_name = os.path.join(out_dir, f'{page_category_group}_wiki_data.tsv')
        combined_category_data.to_csv(out_file_name, sep='\t', index=False)
    
if __name__ == '__main__':
    main()