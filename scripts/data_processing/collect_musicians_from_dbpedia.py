"""
Collect musician data from DBPedia.

Get American musicians from all genres: http://dbpedia.org/page/Category:American_singers_by_genre
Get Latin American musicians from all countries: http://dbpedia.org/page/Category:Singers_by_nationality
"""
from argparse import ArgumentParser
import logging
import os
from SPARQLWrapper import SPARQLWrapper, JSON
import re
import pandas as pd
from functools import reduce

def get_query_results(query, var_names=['page'], query_url='http://dbpedia.org/sparql/', verbose=False):
    """
    Get query results from DBPedia.
    """
    sparql = SPARQLWrapper(query_url)
    sparql.setReturnFormat(JSON)
    sparql.setQuery(query)
    results = sparql.query().convert()
    if(verbose):
        print('results = %s'%(results))
    res_list = results['results']['bindings']
    result_vals = list(map(lambda x: list(map(lambda y: x[y]['value'], var_names)), res_list))
    return result_vals
    
def get_category_page_name_data(category, var_names=['page', 'name'], verbose=False, filter_terms=None):
    """
    Collect name/URL for all pages that fit a specific category.
    """
    if(filter_terms is None):
        query = """
        select distinct ?page ?name where {
        ?page dct:subject <http://dbpedia.org/resource/Category:%s> .
        ?page foaf:name ?name
        } LIMIT 5000
        """%(category)
    else:
        query = """
        select distinct ?page ?name where {
        ?page dct:subject <http://dbpedia.org/resource/Category:%s> .
        ?page foaf:name ?name .
        ?page rdf:type ?type .
        FILTER %s
        } LIMIT 5000
        """%(category, filter_terms)
        print(query)
    query_results = get_query_results(query, var_names=var_names, verbose=verbose)
    query_result_data = pd.DataFrame(query_results, columns=var_names)
    query_result_data = query_result_data.assign(**{
        'page_category' : category
    })
    return query_result_data
    
def get_categories_page_name_data(categories, var_names=['page', 'name'], verbose=False, filter_terms=None):
    category_page_data = []
    for category in categories:
        query_data = get_category_page_name_data(category, var_names=var_names, verbose=verbose, filter_terms=filter_terms)
        category_page_data.append(query_data)
        logging.info('%d pages for category=%s'%(query_data.shape[0], category))
    category_page_data = pd.concat(category_page_data, axis=0)
    return category_page_data
    
def collect_all_subcategories(categories, var_names=['page'], verbose=False):
    """
    Collect all sub-categories below the given categories, 
    collect the sub-categories for those sub-categories,
    and repeat until no more sub-categories.
    """
    category_queue = []
    collected_categories = set()
    existing_categories = set(categories)
    category_queue += categories
    if(verbose):
        print('getting sub-categories for %s'%(str(categories)))
    while(len(category_queue) > 0):
        category = category_queue.pop()
        category_query = """
        PREFIX category: <http://dbpedia.org/resource/Category:>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        select distinct ?page where {
        ?page skos:broader <http://dbpedia.org/resource/Category:%s>
        }
        """%(category)
        query_results = get_query_results(category_query, var_names=var_names, verbose=verbose)
        # fix format
        query_categories = set(map(lambda x: x[0].split('/')[-1].replace('Category:', ''), query_results))
        # remove existing categories
        query_categories = query_categories - existing_categories
        collected_categories.update(query_categories)
        category_queue += list(query_categories)
    collected_categories = list(collected_categories)
    return collected_categories
    
def get_all_sub_category_names(category_name, var_names=['page']):
    """
    Get all pages matching the sub-categories of 
    the provided category on DBPedia.
    """
    query = """
    PREFIX category: <http://dbpedia.org/resource/Category:>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    select distinct ?page where {
    ?page skos:broader category:%s
    }
    """%(category_name)
    query_results = get_query_results(query, var_names=var_names)
    # fix format
    categories = list(map(lambda x: x[0].split('/')[-1].replace('Category:', ''), query_results))
    return categories
    
def main():
    parser = ArgumentParser()
    parser.add_argument('--us_american_musician_seed_category', default='American_singers_by_genre')
    parser.add_argument('--us_american_musician_group_seed_category', default='American_musical_groups_by_state')
    parser.add_argument('--latin_american_musician_seed_category', default='Singers_by_nationality')
    parser.add_argument('--latin_american_musician_group_seed_category', default='Latin_music_groups_by_genre')
    parser.add_argument('--out_dir', default='../../data/culture_metadata/')
    args = vars(parser.parse_args())
    logging_file = '../../output/collect_musicians_from_dbpedia.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    ## get all sub-categories for seed categories
    var_names = ['page']
    ## US American musicians + groups
    us_american_musician_seed_category = args['us_american_musician_seed_category']
    us_american_musician_group_seed_category = args['us_american_musician_group_seed_category']
    us_american_seed_categories = [us_american_musician_seed_category, us_american_musician_group_seed_category]
    us_american_musician_combined_categories = list(set(reduce(lambda x,y: x+y, map(lambda z: get_all_sub_category_names(z, var_names=var_names), us_american_seed_categories))))
    logging.info(f'US American musician sub-categories {us_american_musician_combined_categories}')
    ## Latin American musicians + groups
    # Latin American musicians (by nationality)
    latin_american_musician_seed_category = args['latin_american_musician_seed_category']
    latin_american_musician_categories = get_all_sub_category_names(latin_american_musician_seed_category, var_names=var_names)
    # filter musicians to Latin American countries
    latin_american_country_demonyms = ['Argentinian', 'Bolivian', 'Brazilian', 'Bahamian', 'Chilean', 'Colombian', 'Costa Rican', 'Cuban', 'Dominican', 'Ecuadorian', 'Grenada', 'Guatemalan', 'Guyanese', 'Honduran', 'Haitian', 'Jamaican', 'Mexican', 'Nicaraguan', 'Panamanian', 'Peruvian', 'Paraguayan', 'Surinamese', 'Salvadoran', 'Uruguayan', 'Venezuelan']
    latin_american_country_demonym_matcher = re.compile('|'.join(latin_american_country_demonyms))
    latin_american_musician_categories = list(filter(lambda x: latin_american_country_demonym_matcher.search(x) is not None, latin_american_musician_categories))
    # Latin American musical groups
    latin_american_musician_group_seed_category = args['latin_american_musician_group_seed_category']
    latin_american_musician_group_categories = get_all_sub_category_names(latin_american_musician_group_seed_category, var_names=var_names)
    latin_american_musician_combined_categories = latin_american_musician_categories + latin_american_musician_group_categories
    logging.info(f'Latin American musician categories {latin_american_musician_combined_categories}')
    
    ## improve coverage by recursively searching for all sub-categories
    us_american_musician_sub_categories = collect_all_subcategories(us_american_musician_combined_categories)
    latin_american_musician_sub_categories = collect_all_subcategories(latin_american_musician_combined_categories)
    logging.info('collected %d sub-categories for US musicians'%(len(us_american_musician_sub_categories)))
    logging.info('collected %d sub-categories for Latin American musicians'%(len(latin_american_musician_sub_categories)))
    # combine seed categories
    us_american_musician_combined_categories += us_american_musician_sub_categories
    latin_american_musician_combined_categories += latin_american_musician_sub_categories
    
    ## get all musicians who match sub-category
    # name | URL | category
    var_names = ['page', 'name']
    musician_filter = "((?type = schema:MusicGroup) || (?type = dbo:MusicalArtist))"
    us_american_musician_page_data = get_categories_page_name_data(us_american_musician_combined_categories, var_names=var_names, filter_terms=musician_filter)
    latin_american_musician_page_data = get_categories_page_name_data(latin_american_musician_combined_categories, var_names, filter_terms=musician_filter)
    # clean up
    us_american_musician_page_data.drop_duplicates('name', inplace=True)
    latin_american_musician_page_data.drop_duplicates('name', inplace=True)
    out_dir = args['out_dir']
    us_american_musician_out_file = os.path.join(out_dir, 'us_american_musician_subcategory_dbpedia_data.tsv')
    latin_american_musician_out_file = os.path.join(out_dir, 'latin_american_musician_subcategory_dbpedia_data.tsv')
    us_american_musician_page_data.to_csv(us_american_musician_out_file, sep='\t', index=False)
    latin_american_musician_page_data.to_csv(latin_american_musician_out_file, sep='\t', index=False)

if __name__ == '__main__':
    main()