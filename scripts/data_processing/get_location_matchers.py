"""
Get matcher data for all locations:
- US state names
- Latin America country names
- unambiguous city names
    - unambiguous = most popular candidate is at least 10x more popular than next candidate
We use this to identify the location of
loanword authors based on their self-reported location.
"""
from argparse import ArgumentParser
import logging
import os
import pandas as pd
import re
from data_helpers import clean_txt_simple

def collect_unambiguous_city_name_data(geonames_data):
    """
    Collect data for unambiguous city names.
    """
    importance_var = 'population'
    importance_ratio = 10
    city_feature_codes = ['PPL', 'PPLA', 'PPLA2', 'PPLA3']
    city_geonames = geonames_data[geonames_data.loc[:, 'feature_code'].isin(city_feature_codes)].sort_values(importance_var, inplace=False, ascending=False)
    city_geonames = city_geonames.assign(**{
        'name_clean' : city_geonames.loc[:, 'name'].apply(clean_txt_simple)
    })
    unambig_city_geonames = []
    for name_i, data_i in city_geonames.groupby('name_clean'):
        data_i.sort_values(importance_var, inplace=True, ascending=False)
        if(data_i.shape[0] > 1):
            match_1_i = data_i.iloc[0, :]
            match_2_i = data_i.iloc[1, :]
            if(match_1_i.loc['population'] >= match_2_i.loc['population']*importance_ratio):
                unambig_city_geonames.append(match_1_i)
        else:
            unambig_city_geonames.append(data_i.iloc[0, :])
    unambig_city_geonames = pd.concat(unambig_city_geonames, axis=1).transpose()
    return unambig_city_geonames

def main():
    parser = ArgumentParser()
    parser.add_argument('--geonames_data', default='/hg190/corpora/GeoNames/allCountriesSimplified.tsv')
    parser.add_argument('--out_dir', default='../../data/control_var_data/')
    args = vars(parser.parse_args())
    logging_file = '../../output/get_location_matchers.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    ## load data
    geonames_data = pd.read_csv(args['geonames_data'], sep='\t', index_col=False)
    # collect Latin America data
    country_feat_code = 'PCLI'
    latin_america_bounds = [[26.512, -58.354], [-116.893, -32.444]]
    latin_america_geonames = geonames_data[(geonames_data.loc[:, 'latitude'] <= latin_america_bounds[0][0]) &
                                           (geonames_data.loc[:, 'latitude'] >= latin_america_bounds[0][1]) &
                                           (geonames_data.loc[:, 'longitude'] >= latin_america_bounds[1][0]) &
                                           (geonames_data.loc[:, 'longitude'] <= latin_america_bounds[1][1])]
    country_prefix = re.compile('^.+ of ')
    country_rename_pairs = [(re.compile('Argentine Republic'), 'Argentina')]
    latin_america_country_geonames = latin_america_geonames[latin_america_geonames.loc[:, 'feature_code']==country_feat_code]
    latin_america_country_geonames = latin_america_country_geonames.assign(**{
        'name' : latin_america_country_geonames.loc[:, 'name'].apply(lambda x: country_prefix.sub('', x))
    })
    for country_name_matcher, country_rename in country_rename_pairs:
        latin_america_country_geonames = latin_america_country_geonames.assign(**{
            'name' : latin_america_country_geonames.loc[:, 'name'].apply(lambda x: country_name_matcher.sub(country_rename, x))
        })
    # add extra names
    latin_america_country_names = latin_america_country_geonames.loc[:, 'name'].values.tolist()
    extra_country_names = ['Brasil']
    latin_america_country_names += extra_country_names
    latin_america_country_codes = list(latin_america_country_geonames.loc[:, 'country'].unique())
    latin_america_country_code_lookup = {}
    for latin_america_country_name in latin_america_country_names:
        latin_america_country_name_geonames = latin_america_country_geonames[latin_america_country_geonames.loc[:, 'name']==latin_america_country_name]
        if(latin_america_country_name_geonames.shape[0] > 0):
            latin_america_country_name_geonames = latin_america_country_name_geonames.iloc[0, :]
            latin_america_country_code_lookup[latin_america_country_name] = latin_america_country_name_geonames.loc['country']
    # add extra for Argentina
    latin_america_country_code_lookup['Argentina'] = 'AR'
    latin_america_country_data = pd.Series(latin_america_country_code_lookup).reset_index(name='country_code').rename(columns={0 : 'country'})
    # get US states
    state_feat_code = 'ADM1'
    us_geonames = geonames_data[geonames_data.loc[:, 'country']=='US']
    us_state_geonames = us_geonames[us_geonames.loc[:, 'feature_code']==state_feat_code]
    # get names and alternate name (2-letter state abbreve)
    us_state_names = us_state_geonames.loc[:, 'name'].values.tolist()
    state_abbreve_matcher = re.compile('[A-Z]{2}')
    us_state_abbreve_names = us_state_geonames.loc[:, 'alternate_names'].apply(lambda x: list(filter(lambda y: state_abbreve_matcher.search(y) is not None, x.split(',')))[0]).values.tolist()
#     latin_america_country_matcher = re.compile('(%s)$'%('|'.join(list(map(lambda x: x.lower(), latin_america_country_names)))))
    us_state_data = pd.DataFrame([us_state_names, us_state_abbreve_names], index=['state_name', 'state_code']).transpose()
#     us_state_matcher = re.compile(', (%s)$|^(%s)$'%('|'.join(list(map(lambda x: x.lower(), us_state_abbreve_names_combined))), '|'.join(list(map(lambda x: x.lower(), us_state_names)))))

    ## get unambiguous city names
    unambig_city_geonames = collect_unambiguous_city_name_data(geonames_data)
    # remove invalid names with bad chars e.g. "*"
    valid_unambig_city_geonames = []
    for name_i, data_i in unambig_city_geonames.groupby('name_clean'):
        try:
            re.compile(name_i)
            valid_unambig_city_geonames.append(data_i)
        except Exception as e:
            pass
    valid_unambig_city_geonames = pd.concat(valid_unambig_city_geonames, axis=0)
#     unambig_city_matcher = re.compile('|'.join(map(lambda x: f'^{x}$', valid_unambig_city_geonames.loc[:, 'name_clean'].values)))
    unambig_city_country_lookup = dict(zip(valid_unambig_city_geonames.loc[:, 'name_clean'].values, 
                                           valid_unambig_city_geonames.loc[:, 'country'].values))
    unambig_city_data = pd.Series(unambig_city_country_lookup).reset_index(name='country').rename(columns={0 : 'city_name'})

    ## write everything to file
    out_dir = args['out_dir']
    latin_america_out_file = os.path.join(out_dir, 'latin_american_country_data.tsv')
    us_state_out_file = os.path.join(out_dir, 'us_state_data.tsv')
    unambig_city_out_file = os.path.join(out_dir, 'unambig_city_data.tsv')
    latin_america_country_data.to_csv(latin_america_out_file, sep='\t', index=False)
    us_state_data.to_csv(us_state_out_file, sep='\t', index=False)
    unambig_city_data.to_csv(unambig_city_out_file, sep='\t', index=False)
    
if __name__ == '__main__':
    main()