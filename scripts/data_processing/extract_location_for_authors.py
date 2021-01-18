"""
Extract locations for loanword authors.
"""
from argparse import ArgumentParser
import logging
import os
import pandas as pd
import re
from data_helpers import clean_txt_simple

def estimate_location_from_txt(txt, us_state_matcher, country_matcher, country_name_code_lookup, unambig_city_matcher, unambig_city_country_lookup):
    """
    Estimate location from text:
    
    1. If text matches US state, assign "USA"
    2. If text matches country, assign {COUNTRY}
    3. If text matches unambiguous city, assign {COUNTRY}
    """
    location = ''
    if(us_state_matcher.search(txt) is not None):
        location = 'US'
    else:
        country_match = country_matcher.search(txt)
        if(country_match is not None):
            location_country = country_match.group(0).strip()
            location = country_name_code_lookup[location_country]
        else:
            unambig_city_match = unambig_city_matcher.search(txt)
            if(unambig_city_match is not None):
                unambig_city = unambig_city_match.group(0).strip()
                if(unambig_city in unambig_city_country_lookup):
                    location = unambig_city_country_lookup[unambig_city]
    return location

def main():
    parser = ArgumentParser()
    parser.add_argument('author_data') # ../../data/mined_tweets/loanword_author_descriptive_data.tsv
    parser.add_argument('--unambig_city_data', default='../../data/control_var_data/unambig_city_data.tsv')
    parser.add_argument('--geo_data', default='/hg190/corpora/GeoNames/allCountriesSimplified.tsv')
    parser.add_argument('--out_dir', default='../../data/mined_tweets/')
    args = vars(parser.parse_args())
    logging_file = '../../output/extract_location_for_authors.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    ## load data
    # author data
    author_data_file = args['author_data']
    author_data = pd.read_csv(author_data_file, sep='\t', index_col=False)
    # remove bad data
    author_data.fillna('', inplace=True)
    author_data = author_data[author_data.loc[:, 'user_location'] != '']
    # clean text: could combine some different names e.g. "M`exico"
    author_data = author_data.assign(**{
        'user_location' : author_data.loc[:, 'user_location'].apply(clean_txt_simple)
    })
    # get description info
    description_txt = author_data.loc[:, 'user_location'].unique()
    # gazetteer data
    geo_data = pd.read_csv(args['geo_data'], sep='\t', index_col=False)
    # state names
    state_feat_code = 'ADM1'
    us_geonames = geo_data[geo_data.loc[:, 'country']=='US']
    us_state_geonames = us_geonames[us_geonames.loc[:, 'feature_code']==state_feat_code]
    # get names and alternate name (2-letter state abbreve)
    us_state_names = us_state_geonames.loc[:, 'name'].values.tolist()
    state_abbreve_matcher = re.compile('[A-Z]{2}')
    us_state_abbreve_names = us_state_geonames.loc[:, 'alternate_names'].apply(lambda x: list(filter(lambda y: state_abbreve_matcher.search(y) is not None, x.split(',')))[0]).values.tolist()
    # combine, clean
    us_state_names_combined = us_state_names + us_state_abbreve_names
    us_state_names_combined = list(map(lambda x: x.lower(), us_state_names_combined))
    # also add USA
    us_state_names_combined += ['us', 'usa']
    # country names
    country_feat_code = 'PCLI'
    country_geo_data = geo_data[geo_data.loc[:, 'feature_code']==country_feat_code]
    country_names = list(country_geo_data.loc[:, 'name'].values)
    country_codes = list(country_geo_data.loc[:, 'country'].values)
    # add extra names
    extra_country_names = ['Argentina', 'Brasil', 'Italy', 'Espana']
    extra_country_codes = []
    for country_name_i in extra_country_names:
        country_geo_data_i = country_geo_data[country_geo_data.loc[:, 'alternate_names'].apply(lambda x: country_name_i in x)].iloc[0, :]
        country_code_i = country_geo_data_i.loc['country']
        extra_country_codes.append(country_code_i)
    country_names += extra_country_names
    country_codes += extra_country_codes
    # clean
    prefix_country_matcher = re.compile('^.*republic of |^kingdom of |^state of ')
    country_names = list(map(lambda x: x.lower(), country_names))
    country_names = list(map(lambda x: prefix_country_matcher.sub('', x), country_names))
    country_name_code_lookup = dict(zip(country_names, country_codes))
#     print(f'country names {country_names}')
    # unambiguous cities
    unambig_city_data_file = args['unambig_city_data']
    unambig_city_data = pd.read_csv(unambig_city_data_file, sep='\t', index_col=False)
    unambig_city_data.rename(columns={'index' : 'name'}, inplace=True)
    # remove bad vals
    unambig_city_data = unambig_city_data[unambig_city_data.loc[:, 'name'].apply(lambda x: type(x) is str)]
    # need at least 4 chars for a reasonable estimate
    MIN_CITY_LEN = 4
    unambig_city_data = unambig_city_data[unambig_city_data.loc[:, 'name'].apply(lambda x: len(x) > MIN_CITY_LEN)]
    # get lookup
    unambig_city_country_lookup = dict(zip(unambig_city_data.loc[:, 'name'].values, unambig_city_data.loc[:, 'country'].values))
    # get location matchers
    us_state_matcher = re.compile('|'.join(list(map(lambda x: f'(?<=,)\s?{x}$|^{x}$', us_state_names_combined))))
    country_matcher = re.compile('|'.join(list(map(lambda x: f'(?<=,)\s?{x}$|^{x}$', country_names))))
    unambig_city_matcher = re.compile('|'.join(unambig_city_data.iloc[:, 0].apply(lambda x: f'^{x}$')))
    
    ## extract country as closely as possible
    ## if location ends with US state => USA
    ## if location ends with country => country
    ## if location matches unambiguous city => country (e.g. "Houston" => USA)
    ## else "other"
    description_locations = list(map(lambda x: estimate_location_from_txt(x, us_state_matcher, country_matcher, country_name_code_lookup, unambig_city_matcher, unambig_city_country_lookup), description_txt))
    description_location_data = pd.DataFrame([description_txt, description_locations], index=['user_location', 'description_location_country']).transpose()
    
    ## add region
    latin_american_country_codes = ['AR', 'BO', 'BR', 'CL', 'CO', 'CR', 'CU', 'DO', 'EC', 'SV', 'GT', 'HT', 
                                    'HN', 'MX', 'NI', 'PA', 'PY', 'PE', 'PR', 'UY', 'VE']
    us_country_codes = ['US']
    europe_country_codes = ['ES']
    other_country_codes = set(country_codes) - set(set(latin_american_country_codes) | set(us_country_codes) | set(europe_country_codes))
    country_region_lookup = {c : 'latin_america' for c in latin_american_country_codes}
    country_region_lookup.update({c : 'us_america' for c in us_country_codes})
    country_region_lookup.update({c : 'europe' for c in europe_country_codes})
    country_region_lookup.update({c : 'other' for c in other_country_codes})
    description_location_data = description_location_data.assign(**{
        'description_location_region' : description_location_data.loc[:, 'description_location_country'].apply(lambda x: country_region_lookup.get(x) if x in country_region_lookup else 'UNK')
    })
    
    ## write to file
    out_dir = args['out_dir']
    author_data_file_base = os.path.basename(author_data_file).split('.')[0]
    ## write author/location data for simpler loading later
    author_location_data = pd.merge(author_data, description_location_data, on='user_location')
    author_location_file_name = os.path.join(out_dir, f'{author_data_file_base}_location_data.tsv')
    author_location_data.to_csv(author_location_file_name, sep='\t', index=False)
    
if __name__ == '__main__':
    main()