"""
Mine the age distribution on Facebook for given interests.
E.g. What is median age of a Taylor Swift fan?
"""
from argparse import ArgumentParser
import logging
import os
import sys
# pySocialWatcher does NOT want to be installed ;_;
# probably because of Python 2 stupidity
# if('pySocialWatcher' not in sys.path):
#     sys.path.append('pySocialWatcher')
# from pysocialwatcher import watcherAPI
# from pysocialwatcher.constants import TOKENS
import numpy as np
import pandas as pd
import json
import requests
from time import sleep
import re
from data_helpers import AccessTokenHandler

"""
Query structure

{
    "ages_ranges" : [
        "min" : MIN, 
        "max" : MAX,
    ],
    "interests" : [
        {
            "or" : [ID],
            "name" : [NAME],
        }
    ],
    "languages": [
        null
    ], 
    "name": "Broad list of interest per country", 
    "publisher_platforms": [
        "facebook"
    ]
}
"""
class FatalException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

FAKE_DATA_RESPONSE_CONTENT = '{"mockResponse":true, "data":[{"bid_estimate":{"min_bid":0,"median_bid":0,"max_bid":0},"daily_outcomes_curve":[{"spend":0,"reach":0,"impressions":0,"actions":0}],"estimate_dau":0,"estimate_mau":0,"estimate_ready":true}]}'
def get_fake_response():
    response = requests.models.Response()
    response._content = FAKE_DATA_RESPONSE_CONTENT
    response.status_code = 200
    logging.warn("Fake Response created: " + response.content)
    return response

API_UNKOWN_ERROR_CODE_1 = 1
API_UNKOWN_ERROR_CODE_2 = 2
INITIAL_TRY_SLEEP_TIME = 300
INVALID_PARAMETER_ERROR = 100
FEW_USERS_IN_CUSTOM_LOCATIONS_SUBCODE_ERROR = 1885036
INGORE_INVALID_ZIP_CODES = True
TARGETING_SPEC_FIELD = "targeting_spec"
def handle_send_request_error(response, url, params, tryNumber):
    try:
        error_json = json.loads(response.text)
        if error_json["error"]["code"] == API_UNKOWN_ERROR_CODE_1 or error_json["error"]["code"] == API_UNKOWN_ERROR_CODE_2:
            print_error_warning(error_json, params)
            time.sleep(INITIAL_TRY_SLEEP_TIME * tryNumber)
            return send_request(url, params, tryNumber)
        elif error_json["error"]["code"] == INVALID_PARAMETER_ERROR and "error_subcode" in error_json["error"] and error_json["error"]["error_subcode"] == FEW_USERS_IN_CUSTOM_LOCATIONS_SUBCODE_ERROR:
            return get_fake_response()
        elif "message" in error_json["error"] and "Invalid zip code" in error_json["error"]["message"] and INGORE_INVALID_ZIP_CODES:
            print_warning("Invalid Zip Code:" + str(params[TARGETING_SPEC_FIELD]))
            return get_fake_response()
        else:
            logging.error("Could not handle error.")
            logging.error("Error Code:" + str(error_json["error"]["code"]))
            if "message" in error_json["error"]:
                logging.error("Error Message:" + str(error_json["error"]["message"]))
            if "error_subcode" in error_json["error"]:
                logging.error("Error Subcode:" + str(error_json["error"]["error_subcode"]))
            raise FatalException(str(error_json["error"]))
    except Exception as e:
        logging.error(e)
        raise FatalException(str(response.text))

MAX_NUMBER_TRY = 10
REQUESTS_TIMEOUT = 60
def send_request(url, params, tryNumber = 0):
    tryNumber += 1
    if tryNumber >= MAX_NUMBER_TRY:
        print_warning("Maxium Number of Tries reached. Failing.")
        raise FatalException("Maximum try reached.")
    try:
        response = requests.get(url, params=params, timeout=REQUESTS_TIMEOUT)
    except Exception as error:
        raise RequestException(error.message)
    if response.status_code == 200:
        return response
    else:
        return handle_send_request_error(response, url, params, tryNumber)

REACHESTIMATE_URL = "https://graph.facebook.com/v6.0/act_{}/delivery_estimate"
def call_request_fb(query, token, account):
    payload = {
        'currency': 'USD',
        'optimize_for': "NONE",
        'optimization_goal': "AD_RECALL_LIFT",
        'targeting_spec': json.dumps(query),
        'access_token': token,
    }
#    payload_str = str(payload)
#    print_warning("\tSending in request: %s"%(payload_str))
    url = REACHESTIMATE_URL.format(account)
    response = send_request(url, payload)
    return response.content

# def query_facebook_audience(access_token, user_id, query_file, extra_auth_data=[], response_file=None):
#     """
#     Build manual query and execute request.
    
#     access_token :: FB access token
#     user_id :: FB user ID
#     query_file :: JSON file containing query
#     extra_auth_data :: List of auth data pairs.
#     response_file :: Name of existing response file, if needed.
    
#     response :: DataFrame with query response(s) => one response per row
#     """
#     watcher = watcherAPI()
#     if(not (access_token, user_id) in TOKENS):
#         watcher.add_token_and_account_number(access_token, user_id)
#     for (access_token_i, user_id_i) in extra_auth_data:
#         if(not (access_token_i, user_id_i) in TOKENS):
#             watcher.add_token_and_account_number(access_token_i, user_id_i)
# #     print('%d FB tokens'%(len(TOKENS)))
    
#     ## execute data collection
#     if(response_file is not None and os.path.exists(response_file)):
#         print('using response file %s'%(response_file))
#         response = watcher.load_data_and_continue_collection(response_file)
#     else:
#         response = watcher.run_data_collection(query_file)
    
#     ## clean up temporary dataframes
#     file_matcher = re.compile('dataframe_.*.csv')
#     tmp_files = filter(lambda f: file_matcher.search(f) is not None, os.listdir('.'))
#     for f in tmp_files:
#         os.remove(f)
#     return response

REACHESTIMATE_URL = "https://graph.facebook.com/v6.0/act_{}/delivery_estimate"
def mine_audience_size(interest_ID, interest_name, access_token_handler, params, audience_param='estimate_mau'):
    """
    Mine audience size for a specified interest and parameter combinations.
    
    :param interest_ID: interest ID
    :param interest_name: interest name
    :param access_token_handler: access token handler
    :param params: parameter combinations
    :returns audience_estimates:: audience size estimates
    """
    query_sleep_time = 2
    rate_limit_sleep_time = 300
    max_try_count = 10
    audience_estimates = []
    for param_i in params:
    # for age_min_i, age_max_i in age_range_pairs:
        audience_estimate = -1
        try_ctr = 0
        while(audience_estimate == -1 and try_ctr < max_try_count):
            # if we hit rate limit, cycle to next token
            if(access_token_handler.get_curr_token_rate_limited()):
                access_token_handler.next_token()
            access_token = access_token_handler.get_access_token_curr()
            user_ID = access_token_handler.get_user_ID_curr()
            query = {
                "age_min" : param_i['age_min'],
                "age_max" : param_i['age_max'],
                "flexible_spec" : [
                    {
                        "interests" : [
                            {
                                "id" : interest_ID,
                                "name" : interest_name,
                            }
                        ]
                    }
                ],
                "genders" : [
                    param_i['gender']
                ],
                "geo_locations": {
                    "country_groups": [
                        param_i['geo_location'],
                    ],
                    "location_types": [
                        "home",
                    ]
                },
                "publisher_platforms": [
                    "facebook"
                ]
            }
            logging.info(f'running query {query}')
            payload = {
                'optimization_goal': "AD_RECALL_LIFT",
                'targeting_spec': json.dumps(query),
                'access_token': access_token,
            }
            request_URL = REACHESTIMATE_URL.format(user_ID)
            request_results = requests.get(request_URL, params=payload, timeout=60)
            logging.info('queried URL %s'%(request_results.url))
            logging.info('results text %s'%(request_results.text))
            request_results_data = json.loads(request_results.text)
            if('data' in request_results_data):
                audience_estimate = request_results_data['data'][0][audience_param]
                access_token_handler.set_curr_token_rate_limited(False)
            else:
                logging.info(f'bad reponse {request_results_data}')
                logging.info(f'sleeping for {rate_limit_sleep_time} sec')
                access_token_handler.set_curr_token_rate_limited(True)
                try_ctr += 1
                sleep(rate_limit_sleep_time)
        sleep(query_sleep_time)
        audience_estimates.append(audience_estimate)
    return audience_estimates

def main():
    parser = ArgumentParser()
    parser.add_argument('interest_data') # collected from Spotify/YouTube
#     parser.add_argument('--auth_data', default='../../data/culture_metadata/facebook_auth_tmp.csv')
    parser.add_argument('--auth_data', default='../../data/culture_metadata/facebook_auth_multi.csv')
    parser.add_argument('--out_dir', default='../../data/culture_metadata/')
    args = vars(parser.parse_args())
    logging_file = '../../output/mine_FB_age_distribution_for_interests.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    ## collect age distribution data
    # restrict to exact query/name matches ("Shakira" but not "Shakira (album)")
    out_dir = args['out_dir']
    interest_data_out_file = os.path.join(out_dir, args['interest_data'])
    interest_data = pd.read_csv(interest_data_out_file, sep='\t', index_col=False)
    interest_data.fillna('', inplace=True)
    interest_data = interest_data[interest_data.loc[:, 'id'] != -1]
    interest_name_var = 'name'
    # get old data and remove old interests
    audience_size_data_file = os.path.join(out_dir, 'interest_audience_size_FB_estimates.tsv')
    if(os.path.exists(audience_size_data_file)):
        old_audience_size_data = pd.read_csv(audience_size_data_file, sep='\t', index_col=False)
        old_audience_interest_names = old_audience_size_data.loc[:, 'interest_name'].unique()
        interest_data = interest_data[~interest_data.loc[:, 'name'].isin(old_audience_interest_names)]
        # remove invalid data
        interest_data = interest_data[interest_data.loc[:, 'name'] != '']
        assert 'Smash Mouth' in set(interest_data.loc[:, 'name'].unique())
    else:
        old_audience_size_data = []
    # get names and IDs for mining
    interest_name_ids = []
    for name_i, data_i in interest_data.groupby('query'):
        if(data_i.shape[0] > 1):
            # limit data to 
            data_i = data_i[data_i.loc[:, 'query'] == data_i.loc[:, 'name']]
        if(data_i.shape[0] > 0):
            id_i = int(data_i.iloc[0, :].loc['id'])
            interest_name_ids.append([name_i, id_i])
    # tmp debugging
#     interest_name_ids = interest_name_ids[:75]
    # get access token data
    auth_data_file = args['auth_data']
    auth_data = pd.read_csv(auth_data_file, sep=',', index_col=False)
    # get all token/ID pairs
    access_tokens = auth_data.loc[:, 'access_token'].values
    user_IDs = auth_data.loc[:, 'user_id'].values
    access_token_handler = AccessTokenHandler(access_tokens, user_IDs)
    min_age = 15
    max_age = 45
    bin_size = 10
    bin_count = int((max_age - min_age) / bin_size) + 1
    age_ranges = np.linspace(min_age, max_age, bin_count)
    age_ranges = list(map(int, age_ranges))
    # add irregular bins for the olds
    age_ranges += [65, 66] # this will yield [45,64] and [65, 65] (for 65+ FB users)
    age_range_pairs = list(zip(age_ranges[:-1], age_ranges[1:]))
    default_geo_location = 'worldwide'
    default_gender = 0
    params = list(map(lambda x: {
        'age_min' : x[0], 
        'age_max' : x[1]-1, # max-age is inclusive except for 65 (indicates 65+)
        'geo_location' : default_geo_location,
        'gender' : default_gender,
        }, age_range_pairs))
    audience_size_data = []
    if(len(old_audience_size_data) > 0):
        audience_size_data.append(old_audience_size_data)
    audience_param = 'estimate_mau'
    write_ctr = 0
    interests_per_write = 50
    for interest_name, interest_id in interest_name_ids:
        audience_sizes = mine_audience_size(interest_id, interest_name, access_token_handler, params, audience_param=audience_param)
        audience_sizes = pd.DataFrame([audience_sizes, age_range_pairs], index=[audience_param, 'age_range']).transpose()
        audience_sizes = audience_sizes.assign(**{
            'interest_name' : interest_name,
        })
        audience_size_data.append(audience_sizes)
        write_ctr += 1
        if(write_ctr % interests_per_write == 0):
            combined_audience_size_data = pd.concat(audience_size_data, axis=0)
            combined_audience_size_data.to_csv(audience_size_data_file, sep='\t', index=False)
    combined_audience_size_data = pd.concat(audience_size_data, axis=0)
    logging.debug('writing %d audience sizes to file'%(combined_audience_size_data.shape[0]))
    # save to file
    combined_audience_size_data.to_csv(audience_size_data_file, sep='\t', index=False)
    
if __name__ == '__main__':
    main()