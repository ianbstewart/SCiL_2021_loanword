"""
Mine archive of Twitter data for specified
phrase or location.
"""
from datetime import datetime
import gzip
import json
from argparse import ArgumentParser
import os, re
from unidecode import unidecode
from functools import reduce
from langid import langid
import cld2
from multiprocessing import Pool
import logging
import pandas as pd

def contains_geo(geo, location_box):
    """
    Determine if location box contains geo point.
    
    Parameters:
    -----------
    geo : [float, float]
    Latitude, longitude.
    location_box : [[float, float], [float, float]]
    Latitude 1 (W), latitude 2 (E), longitude 1 (S), longitude 2 (N).
    """
    lat, lon = geo
    lat1, lat2 = location_box[0]
    lon1, lon2 = location_box[1]
    contains = (lat >= lat1 and lat <= lat2 and lon >= lon1 and lon <= lon2)
    return contains

def mine_tweets(archive_file, out_file, phrases=None, location_box=None, location_info=None, match_hashtags=False, user_loc_phrase=None, lang=None, lang_detect=None, lang_detect_dir=None):
    """
    Extract tweets from archive according to matching
    phrases in the main text or location, and write to file.
    
    Parameters:
    -----------
    archive_file : str
    file_output : str
    Writeable output file name.
    phrases : [str]
    location_box : [[float]]
    Latitude and longitude values for bounding box.
    match_hashtags : bool
    Treat phrases as hashtags (different value in tweet JSON).
    user_loc_phrase : str
    User location matching phrase.
    lang : tweet language
    lang_detect : detected tweet language (via CLD)
    lang_detect_dir : directory for previously detected languages
    """
    if(phrases is not None):
        # logging.debug('phrases %s'%(','.join(phrases)))
        if(match_hashtags):
            phrase_tags = set([unidecode(x.replace('#','').lower()) for x in phrases])
            logging.debug('phrase tags %s'%(phrase_tags))
        else:
#             phrase_matcher = re.compile('|'.join(phrases).lower())
            # remove spaces when needed
            phrases_fixed = phrases + [x.replace(' ', '') for x in phrases if ' ' in x]
            phrases_fixed_combined = '|'.join(phrases_fixed).lower()
            # match tokenized words
            # this might be easier if we just tokenized and rejoined everything with whitespace...awk
            phrases_fixed_str = '(?<=[,\.! #])(%s)(?=[,\.! #])|(?<=[,\.! #])(%s)$|^(%s)(?=[,\.! #])'%(phrases_fixed_combined, phrases_fixed_combined, phrases_fixed_combined)
#             phrase_matcher = re.compile('|'.join(phrases_fixed).lower())
            phrase_matcher = re.compile(phrases_fixed_str)
#             logging.debug('phrase match pattern %s'%(phrase_matcher.pattern))
    else:
        phrase_matcher = None
    if(user_loc_phrase is not None):
        user_loc_phrase_matcher = re.compile(user_loc_phrase.lower())
    if(location_info is not None):
        logging.debug('location info %s'%(location_info))
    if(lang_detect):
        langid.set_languages(['en', 'es', 'fr'])
    match_ctr = 0
    error_ctr = 0
    non_delete_ctr = 0
    
    with open(out_file, 'a') as file_output:
        with gzip.open(archive_file, 'r') as archive:
            if(lang_detect_dir is not None):
                lang_detect_data_file = os.path.join(lang_detect_dir, os.path.basename(archive_file).replace('.gz', '_lang_id.tsv.gz'))
                lang_detect_data = pd.read_csv(lang_detect_data_file, sep='\t', index_col=False, header=None, compression='gzip', names=['id', 'lang', 'score'])
                # convert ID to int
                lang_detect_data = lang_detect_data.assign(**{'id' : lang_detect_data.loc[:, 'id'].apply(lambda x: int(x))})
                lang_detect_data.drop_duplicates('id', inplace=True)
                # make dictionary for fast lookup
                lang_detect_lookup = dict(zip(lang_detect_data.loc[:, 'id'].values, lang_detect_data.loc[:, 'lang'].values))
                # get set of all valid IDs
                lang_detect_valid_ids = set(lang_detect_data[lang_detect_data.loc[:, 'lang']==lang_detect].loc[:, 'id'].unique())
                logging.debug('%d valid posts with lang %s'%(len(lang_detect_valid_ids), lang_detect))
            for l in archive:
                try:
                    if(type(l) is bytes):
                        l = l.decode('utf-8').strip()
                    j = json.loads(l.strip())
                    if('delete' not in j and 'status_withheld' not in j):
                        non_delete_ctr += 1
                        # logging.debug(sorted(j.keys()))
                        # phrase matching for hashtag/text
                        txt_match = True
                        phrase_match_j = []
                        if(phrases is not None):
                            if(match_hashtags):
                                if(j.get('entities') is not None and j['entities'].get('hashtags') is not None):
                                    j_tags = set([unidecode(x['text'].lower()) for x in j['entities']['hashtags']])
                                    phrase_match_j = j_tags
                                    txt_match = (len(phrase_tags & j_tags) > 0)
                            else:
                                j_text = unidecode(j['text'].lower())
                                phrase_match_j = phrase_matcher.findall(j_text)
                                if(len(phrase_match_j) > 0):
                                    phrase_match_j = list(reduce(lambda a,b: a+b, [[y for y in x if y!=''] for x in phrase_match_j]))
                                txt_match = (len(phrase_match_j) > 0)
                        j_coord = j.get('coordinates')
                        j_place = j.get('place')
                        # location matching
                        loc_match = True
                        if(location_box is not None):
                            if(j_coord is not None and contains_geo(j_coord, location_box)):
                                loc_match = True
                            else:
                                loc_match = False
                        if(location_info is not None and len(location_info) > 0):
                            if(j_place is not None):
    #                             if(j_place['country_info'] == 'FR'):
    #                                 logging.debug('should catch place = %s'%(str(j_place)))
                                loc_match = all([j_place[k] == v for k,v in location_info.items()])
                            else:
                                loc_match = False
                        # user bio location matching
                        user_loc_match = True
                        if(user_loc_phrase is not None):
                            if(j['user'].get('location') is not None):
                                j_user_loc = unidecode(j['user']['location'].lower())
                                user_loc_match = user_loc_phrase_matcher.search(j_user_loc) is not None
                            else:
                                user_loc_match = False
                        # language matching
                        # use tweet-provided lang, use pre-detected lang, or detect lang on the fly
                        lang_match = True
                        if(lang is not None):
                            lang_match = j['lang'] == lang
                        if(lang_detect_dir is not None):
                            lang_match = j['id'] in lang_detect_valid_ids
#                             if(lang_match):
#                                 logging.debug('ID %d matches lang %s'%(j['id'], lang_detect))
#                             if(not lang_match):
#                                 logging.debug('ID %d not found in lang_detect'%(j['id']))
#                             else:
#                                 j_lang_detect = lang_detect_lookup[int(j['id'])]
#                                 lang_match = j_lang_detect == lang_detect
                        elif(lang_detect is not None):
#                             j_lang_detect = langid.classify(j['text'])[0]
                            j_lang_detect = cld2.detect(j['text']).details[0].language_code
                            lang_match = j_lang_detect == lang_detect
                        # if(phrase_matcher.search(j_text) is not None):
    #                     logging.debug('txt match %s, loc match %s, user loc match %s'%(txt_match, loc_match, user_loc_match))
                        if(txt_match and loc_match and user_loc_match and lang_match):
                            match_ctr += 1
    #                         logging.debug(j_text)
                            # add info on matching phrase/s!
                            j['phrase_match'] = phrase_match_j
                            j_dump = json.dumps(j).replace('\n','')
                            try:
                                file_output.write('%s\n'%(j_dump))
                            except Exception as e:
                                logging.debug('write exception %s'%(e))
                        # tmp debugging
#                         elif(lang_match): 
#                             logging.debug('lang match True: txt_match %s loc_match %s user_loc_match %s'%(txt_match, loc_match, user_loc_match))
                        if(non_delete_ctr % 100000 == 0):
                            logging.debug('matched %d/%d non-delete tweets'%(match_ctr, non_delete_ctr))
                except Exception as e:
                    logging.debug(e)
                    # handling broken tweets
                    # logging.debug(e)
                    # logging.debug(type(l))
                    # logging.debug(l)
                    error_ctr += 1
                    pass
                # tmp debugging
    #             if(non_delete_ctr > 100000):
    #                 break
    logging.debug('non-delete %d tweets'%(non_delete_ctr))
    logging.debug('matched %d tweets'%(match_ctr))
    logging.debug('errored %d tweets'%(error_ctr))

def build_out_file(out_dir, phrases, phrase_file, location_box, location_info, user_loc_phrase, lang, lang_detect, add_dates_from_files_to_out_file, archive_files):
    """
    Build out file to which to write tweets.
    """
    out_file_str = ''
    if(phrase_file is not None):
        # too strict
#         phrase_file_name_matcher = re.compile('(\w+)_phrases.txt')
#         phrase_str = phrase_file_name_matcher.search(os.path.basename(phrase_file)).group(1)
        phrase_str = os.path.basename(phrase_file).split('.')[0]
        phrase_str = '_PHRASES=%s'%(phrase_str)
        out_file_str += phrase_str
    elif(phrases is not None):
        phrase_str = '_PHRASES=%s'%(','.join(phrases))
        out_file_str += phrase_str
    if(location_box is not None):
        location_str = '_LOCBOX=%s'%(','.join(map(lambda x: '%.3f'%(x), [location_box[0][0], location_box[1][0], location_box[0][1], location_box[1][1]])))
        # location_str = '_%s'%(','.join(map(lambda x: '%.3f'%(x), location_box)))
        out_file_str += location_str
    if(location_info is not None):
        for k, v in sorted(list(location_info.items()), key=lambda x: x[0]):
            out_file_str += '_%s=%s'%(k, v)
    if(user_loc_phrase is not None):
        out_file_str += "USERLOCPHRASE=_%s"%(user_loc_phrase)
    if(add_dates_from_files_to_out_file):
        archive_files = sorted(archive_files)
        # hack: find first date in file name
        # MONTH-day-year
        date_matcher = re.compile('\w{3}-\d{2}-\d{2}')
        date_str_1 = date_matcher.search(os.path.basename(archive_files[0])).group(0)
        date_str_2 = date_matcher.search(os.path.basename(archive_files[-1])).group(0)        
        out_file_str += '_DATERANGE=%s-%s'%(date_str_1, date_str_2)
    if(lang is not None):
        out_file_str += '_LANG=%s'%(lang)
    if(lang_detect is not None):
        out_file_str += '_LANGDETECT=%s'%(lang_detect)
    out_file = os.path.join(out_dir, 'archive%s.gz'%(out_file_str))
    return out_file
    
def main():
    parser = ArgumentParser()
    parser.add_argument('archive_files', nargs='+')
    parser.add_argument('--phrases', default="")
    parser.add_argument('--phrase_file', default=None)
    parser.add_argument('--match_hashtags', default=False)
    parser.add_argument('--location_box', nargs='+', default=None)
    parser.add_argument('--location_country', default=None)
    parser.add_argument('--user_loc_phrase', default=None)
    parser.add_argument('--lang', default=None)
    parser.add_argument('--lang_detect', default=None)
    parser.add_argument('--lang_detect_dir', default=None)
    parser.add_argument('--out_dir', default='../../data/mined_tweets/')
    parser.add_argument('--add_dates_from_files_to_out_file', type=bool, default=True)
    args = vars(parser.parse_args())

    # extract phrases
    phrases = args['phrases'].split(',')
    if(len(phrases) == 1 and phrases[0] == ''):
        phrases = None
    if(args.get('phrase_file') is not None and os.path.exists(args['phrase_file'])):
        phrases = sorted(set([l.strip().lower() for l in open(args['phrase_file'], 'r')]) - set(['']))
        phrase_file = args['phrase_file']
    else:
        phrases = None
        phrase_file = None
    # load locations if they exist
    if(args.get('location_box') is not None and len(args['location_box']) > 0 and args['location_box'] != ''):
        location_box = list(map(float, args['location_box']))
        location_box = [location_box[:2], location_box[2:]]
    else:
        location_box = None
    location_info = {}
    if(args.get('location_country') is not None):
        location_info['country_code'] = args['location_country']
#     logging.debug('user loc phrase "%s"'%(user_loc_phrase))
    if(args['user_loc_phrase'] == ''):
        user_loc_phrase = None
    archive_files = args.get('archive_files')
    lang = args.get('lang')
    lang_detect = args.get('lang_detect')
    lang_detect_dir = args.get('lang_detect_dir')
    add_dates_from_files_to_out_file = args.get('add_dates_from_files_to_out_file')
    out_dir = args.get('out_dir')
    user_loc_phrase = args.get('user_loc_phrase')
    match_hashtags = args.get('match_hashtags')
        
    # sort archive files by date (month-day-year)
    date_fmt = '%b-%d-%y'
    matcher = re.compile('[A-Z][a-z]+-[0-3][0-9]-1[0-9]')
#     print('archive files %s'%(str(archive_files)))
    archive_file_dates = map(lambda x: datetime.strptime(matcher.findall(os.path.basename(x))[0], date_fmt), archive_files)
    archive_files, archive_file_dates = zip(*sorted(zip(archive_files, archive_file_dates), key=lambda x: x[1]))
    start_date = datetime.strftime(archive_file_dates[0], date_fmt)
    end_date = datetime.strftime(archive_file_dates[-1], date_fmt)
    # out file
    out_file = build_out_file(out_dir, phrases, phrase_file, location_box, location_info, user_loc_phrase, lang, lang_detect, add_dates_from_files_to_out_file, archive_files)
    # logging file
    logging_dir = '../../output'
    logging_file = build_out_file(logging_dir, phrases, phrase_file, location_box, location_info, user_loc_phrase, lang, lang_detect, add_dates_from_files_to_out_file, archive_files).replace('.gz', '.txt')
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.DEBUG)
    logging.debug('going to write to out file %s'%(out_file))
    
    # find tweets with matching phrase and/or location
    # and write to file
    # repeatedly append to end of txt file
    # then compress after mining
    txt_out_file = out_file.replace('.gz', '.txt')
    if(os.path.exists(txt_out_file)):
        os.remove(txt_out_file)
    for archive_file in archive_files:
        logging.debug('mining archive file %s'%(archive_file))
        # limit processes for lang detection
        # if not, langid will use multiple cores => TODO: is this actually the problem?
        if(lang_detect):
            with Pool(processes=1) as pool:
                pool.starmap(mine_tweets, [[archive_file, txt_out_file, phrases, location_box, location_info, match_hashtags, user_loc_phrase, lang, lang_detect, lang_detect_dir]])
                pool.close()
        else:
            mine_tweets(archive_file, txt_out_file, phrases=phrases, location_box=location_box, location_info=location_info, match_hashtags=match_hashtags, user_loc_phrase=user_loc_phrase, lang=lang, lang_detect=lang_detect, lang_detect_dir=lang_detect_dir)        
    # compress gzip file after writing
    with gzip.open(out_file, 'wt') as file_output:
        file_output.write(''.join(open(txt_out_file).readlines()))
    os.remove(txt_out_file)
#     with gzip.open(out_file, 'wt') as file_output:
#         for archive_file in archive_files:
#             logging.debug('mining archive file %s'%(archive_file))
#             # limit processes for lang detection
#             # if not, langid will use multiple cores
#             if(lang_detect):
#                 with Pool(processes=1) as pool:
#                     pool.starmap(mine_tweets, [archive_file, file_output, phrases, location_box, location_info, match_hashtags, user_loc_phrase, lang, lang_detect])
#             else:
#                 mine_tweets(archive_file, file_output, phrases=phrases, location_box=location_box, location_info=location_info, match_hashtags=match_hashtags, user_loc_phrase=user_loc_phrase, lang=lang, lang_detect=lang_detect)

if __name__ == '__main__':
    main()