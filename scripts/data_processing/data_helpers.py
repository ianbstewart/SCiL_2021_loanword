"""
Helper functions for data processing.
"""
import re
from nltk.parse.corenlp import CoreNLPParser
from nltk.tokenize import word_tokenize
from nltk.tokenize.toktok import ToktokTokenizer
import logging
from multiprocessing import Pool
import os
import gzip
from bz2 import BZ2File
import lzma
from zstd import ZSTD_uncompress
# old
# from zstd import ZstdDecompressor
import subprocess
from math import ceil, floor
from functools import reduce
from itertools import product
import scipy
import scipy.sparse
from scipy.stats import wilcoxon
from pattern.es import conjugate, INFINITIVE, PRESENT, PRETERITE, FUTURE, SINGULAR, PLURAL, FIRST, SECOND, THIRD
from emoji import UNICODE_EMOJI
from elasticsearch import Elasticsearch
from time import sleep
import numpy as np
import pandas as pd
import twitter
from unidecode import unidecode
import requests
from sklearn.preprocessing import StandardScaler
from scipy.spatial.distance import cdist
from ast import literal_eval
from datetime import datetime, timedelta
import calendar
from pandarallel import pandarallel
import langid

class StanfordTaggerWrapper:
    """
    Wrapper class for Stanford NER tagger.
    """
    def __init__(self, parser):
        self.parser = parser
        
    def tag(self, tokens):
        token_tags = self.parser.tag(tokens)
        return token_tags

def load_tagger(port=9003, tagtype='ner'):
    """
    Load POS/NER tagger.
    Default to Stanford tagger because we can't
    find Twitter pre-trained tagger. FML
    Assumes that server is already running as follows:
    cd /hg190/corpora/StanfordCoreNLP/tmp/stanford-corenlp-full-2018-02-27/
    java -Xmx4g -cp "*" edu.stanford.nlp.pipeline.StanfordCoreNLPServer -serverProperties StanfordCoreNLP-spanish.properties -preload pos,ner -status_port 9003 -port 9003 -timeout 15000 # Spanish
    java -Xmx4g -cp "*" edu.stanford.nlp.pipeline.StanfordCoreNLPServer -serverProperties StanfordCoreNLP-french.properties -preload pos,ner -status_port 9004 -port 9004 -timeout 15000 # French
    
    :returns tagger:: tagger model
    """
    parser = CoreNLPParser('http://localhost:%d'%(port), tagtype=tagtype)
    tagger = StanfordTaggerWrapper(parser)
    return tagger

class BasicTokenizer:
    """
    Wrapper class for nltk.tokenize.word_tokenize.
    """
    def __init__(self, lang):
        self.lang = lang
        
    def tokenize(self, txt):
        return word_tokenize(txt, self.lang)

## text cleaning
# emoji_pattern = re.compile("["
#         u"\U0001F600-\U0001F64F"  # emoticons
#         u"\U0001F300-\U0001F5FF"  # symbols & pictographs
#         u"\U0001F680-\U0001F6FF"  # transport & map symbols
#         u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
#                            "]+", flags=re.UNICODE)
HASH_MATCHER = re.compile('#[^\s#]+')
USER_MATCHER = re.compile('@\s?\w+')
URL_MATCHER = re.compile('https?://[^\s]+|t\.co[^\s]+|pic.twitter.com[^\s]+')
NUM_MATCHER = re.compile('\d+')
RETURN_MATCHER = re.compile('[\n\r]')
MARKUP_MATCHER = re.compile('\*\*')
MATCHER_PAIRS = [[HASH_MATCHER, '#HASH'], [USER_MATCHER, '@USER'], [URL_MATCHER, '<URL>'], [NUM_MATCHER, '<NUM>'], [RETURN_MATCHER, ' ']]
def clean_tweet_txt(txt):
    txt = txt.lower()
    for matcher, replacement in MATCHER_PAIRS:
        txt = matcher.sub(replacement, txt)
    return txt

def clean_txt_for_matching(txt, TKNZR=None):
    """
    Clean text for word matching.
    Tokenize, re-combine with spaces, add start/end buffer space.
    """
    txt = RETURN_MATCHER.sub(' ', txt)
    txt = MARKUP_MATCHER.sub(' ', txt)
    if(TKNZR is None):
        TKNZR = ToktokTokenizer()
    txt = ' %s '%(' '.join(TKNZR.tokenize(txt)))
    txt = txt.lower()
    return txt

def clean_txt_simple(txt):
    txt = txt.lower()
    txt = unidecode(txt)
    return txt

# not comprehensive ;_;
# emoji_pattern = re.compile("["
#         u"\U0001F600-\U0001F64F"  # emoticons
#         u"\U0001F300-\U0001F5FF"  # symbols & pictographs
#         u"\U0001F680-\U0001F6FF"  # transport & map symbols
#         u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
#                            "]+", flags=re.UNICODE)
EMOJI_MATCHER = re.compile(r"(?:{})+".format("|".join(map(re.escape,UNICODE_EMOJI.keys()))))
def clean_txt_emojis(txt):
    txt = EMOJI_MATCHER.sub(' ', txt)
    return txt

class BasicCounter:
    def __init__(self):
        self.counter = 0
    def increment(self):
        self.counter += 1

def tag_sents(sents, lang='french', port=9004, tag_type='serial', verbose=False):
    """
    Add POS tags to sentences. Supports French and Spanish 
    through StanfordCoreNLP.
    
    :param sents: raw sentences to tag
    :param lang: language to tag
    :param tag_type: serial or parallel tag
    """
    pos_tagger = load_tagger(port=port, tagtype='pos')
    # build tokenizer once, use repeatedly => faster than calling word_tokenize separately
    # TODO: find better solution for language-specific tokenization
#     tokenizer = lambda x: word_tokenize(x, language=lang)
    tokenizer = BasicTokenizer(lang=lang)
    # clean sents
#     clean_sents = [clean_txt(sent) for sent in sents]
    clean_sents = list(map(clean_tweet_txt, sents))
    tokenized_sents = [tokenizer.tokenize(sent) for sent in clean_sents]
    if(verbose):
        print('tokenized sents sample %s'%(str(tokenized_sents[:5])))
#     logging.debug('about to tag %d tokenized sentences'%(len(tokenized_sents)))
#     logging.debug('sent examples: %s'%(str(tokenized_sents[:5])))
    # parallelize tags
    # WARNING: if you parallelize upstream then don't use parallelize
    # otherwise you will wreck your cores
    if(tag_type == 'parallel'):
        # dumb pool method with data copying
#         num_process = 5
#         pool = Pool(num_process)
#         tagged_sents = pool.map(pos_tagger.tag, tokenized_sents)
#         pool.close()
        # less dumb build-in parallel apply
        num_process = 5
        tokenized_sents = pd.Series(tokenized_sents)
        pandarallel.initialize(nb_workers=num_process)
        tagged_sents = tokenized_sents.parallel_apply(pos_tagger.tag)
        # recombine
#         tagged_sents = list(reduce(lambda x,y: x+y, tagged_sents))
    # serial tags
    elif(tag_type == 'serial'):
#         tagged_sents = pos_tagger.tag_sents(tokenized_sents)
        tagged_sents = []
        for tokenized_sent in tokenized_sents:
            try:
                tagged_sent = pos_tagger.tag(tokenized_sent)
#                 if(verbose):
#                     print('generated sent tags %s'%(str(tagged_sent)))
                tagged_sents.append(tagged_sent)
                if(len(tagged_sents) % 1000 == 0):
                    logging.debug('tagged %d/%d sents'%(len(tagged_sents), len(tokenized_sents)))
            except Exception as e:
                logging.debug('could not tag sent "%s" because error %s'%(str(tokenized_sent), e))
#         tagged_sents = [pos_tagger.tag(tokenized_sent) for tokenized_sent in tokenized_sents]
    logging.debug('tagged %d sents'%(len(tagged_sents)))
    return tagged_sents

def tag_sents_tweet_NLP_raw(sents, tag_file_suffix=None):
    """
    Tag raw sents. 
    For parallel processing, add tag file suffix to temporary sent file.
    """
    tag_dir = 'ark-tweet-nlp-0.3.2/'
    if(tag_file_suffix is not None):
        tag_file_base = 'sents_%s'%(tag_file_suffix)
    else:
        tag_file_base = 'sents'
    sent_out_file_name = os.path.join(tag_dir, '%s.txt'%(tag_file_base))
    with open(sent_out_file_name, 'w') as sent_out_file:
        sent_out_file.write('\n'.join(sents))
    os.chdir(tag_dir)
    tag_command = './runTagger.sh %s'%((os.path.basename(sent_out_file_name)))
    tag_command_args = tag_command.split(' ')
#     with open(tag_out_file_name, 'w') as tag_out_file:
#         proc = subprocess.Popen(tag_command_args, stdout=tag_out_file)
#         print(proc.stdout)
    proc = subprocess.Popen(tag_command_args, stdout=subprocess.PIPE)
#     print()
    os.chdir('..')
#     tagged_sents = [l.strip() for l in open(tag_out_file_name, 'r')]
    raw_output = proc.communicate()[0].decode('utf-8')
    tagged_sents = [l.split('\t') for l in raw_output.split('\n')[:-1]]
    # combine words/tags
#     tagged_sents = [['%s/%s'%(pair[0], pair[1]) for pair in list(zip(s[0].split(' '), s[1].split(' ')))] for s in tagged_sents]
    tagged_sents = [[(pair[0], pair[1]) for pair in list(zip(s[0].split(' '), s[1].split(' ')))] for s in tagged_sents]
    # remove tmp file
    os.remove(sent_out_file_name)
    return tagged_sents

def tag_sents_tweet_NLP(sents, tag_type='serial', tag_file_suffix=''):
    """
    Tag English sentences using Twitter NLP: https://www.cs.cmu.edu/~ark/TweetNLP/#pos
    Download 0.3.2 model from here: https://code.google.com/archive/p/ark-tweet-nlp/downloads
    MUST BE RUN FROM scripts/data_processing/ until I figure out how to import things
    :param sents: raw sentences (not tokenized)
    :param tag_type: serial or parallel
    :param tag_file_suffix: tag file suffix (tmp writing raw text for tagging)
    :return tagged_sents: tagged raw sentences
    """
    # clean sentences => avoid line breaks!!
    sents = [clean_txt(x) for x in sents]
    if(tag_type == 'parallel'):
        num_process = 5
        num_process = min(len(sents), num_process)
        pool = Pool(num_process)
        # split sents into chunks
        sent_chunk_size = int(ceil(len(sents) / num_process))
        sent_chunks = [sents[(i*sent_chunk_size):((i+1)*sent_chunk_size)] for i in range(num_process)]
        # need one unique ID per chunk to write sentences to unique files
        if(tag_file_suffix != ''):
            sent_chunk_ids = map(lambda x: '%d_%s'%(x, tag_file_suffix), range(num_process))
        else:
            sent_chunk_ids = list(range(num_process))
        tagged_sents = pool.starmap(tag_sents_tweet_NLP_raw, list(zip(sent_chunks, sent_chunk_ids)))
        # collapse
        tagged_sents = list(reduce(lambda x,y: x+y, tagged_sents))
    else:
        # write raw sents to tmp file 
        tagged_sents = tag_sents_tweet_NLP_raw(sents)
    return tagged_sents

class ZstdWrapper(object):
    
    def __init__(self, f_name):
        # TODO: update to ZSTD_uncompress
        self.decomp = ZstdDecompressor()
        self.f_iter = self.decomp.stream_reader(open(f_name, 'rb'))
        self.prev_line = ""
        self.CHUNK_SIZE = 65536 # may have to increase to 2**24 to avoid messing up giant comments
    
    def __iter__(self):
        return self
    
    def __next__(self):
        chunk = self.f_iter.read(self.CHUNK_SIZE)
        if(not chunk):
            raise StopIteration
        else:
            str_data = chunk.decode('utf-8')
            lines = str_data.split('\n')
            lines_clean = [self.prev_line + lines[0]] + lines[1:-1]
            self.prev_line = lines[-1]
            return lines_clean
        
def get_file_iter(file_name):
    """
    Get file iterator for different types: gz, bz2, xz, zst.
    """
    file_name_ext = os.path.splitext(file_name)[-1][1:]
    if(file_name_ext == 'gz'):
        file_input = gzip.open(file_name)
    elif(file_name_ext == 'bz2'):
        file_input = BZ2File(file_name)
    elif(file_name_ext == 'xz'):
        file_input = lzma.open(file_name)
    elif(file_name_ext == 'zst'):
        file_input = ZstdWrapper(file_name) # WARNING returns list of lines to process
    return file_input

## sparse matrix
def save_sparse_matrix_rows_cols(data, out_file_name, row_idx_lookup, col_idx_lookup):
    """
    Save sparse matrix, row names, col names.
    """
    scipy.sparse.save_npz(out_file_name, data)
    # save rows, cols
    rows = sorted(row_idx_lookup.keys(), key=row_idx_lookup.get)
    cols = sorted(col_idx_lookup.keys(), key=col_idx_lookup.get)
    row_out_file_name = out_file_name.replace('.npz', '_rows.txt')
    col_out_file_name = out_file_name.replace('.npz', '_cols.txt')
    with open(row_out_file_name, 'w') as row_out_file:
        row_out_file.write('\n'.join(rows))
    with open(col_out_file_name, 'w') as col_out_file:
        col_out_file.write('\n'.join(cols))
        
## conjugate verbs
VERB_TENSES=[PRETERITE, PRESENT, FUTURE]
VERB_NUMBERS=[SINGULAR, PLURAL]
VERB_PERSONS=[FIRST, SECOND, THIRD]
VERB_COMBOS=list(product(VERB_TENSES, VERB_PERSONS, VERB_NUMBERS))
# we have to "warm-start" the conjugator to make it work? this is weird
try:
    conjugate('dar', PRETERITE, FIRST, SINGULAR)
except Exception as e:
    conjugate('dar', PRETERITE, FIRST, SINGULAR)
def conjugate_verb(verb):
    """
    Generate all common verb conjugations:
    {PAST/PRESENT/FUTURE/INFINITIVE} x {SINGULAR/PLURAL} x {1st/2nd/3rd}
    
    :param verb: verb (infinitive form)
    :returns conjugated_verbs: verb conjugations
    """
    conjugated_verbs = []
    for (VERB_TENSE, VERB_PERSON, VERB_NUMBER) in VERB_COMBOS:
        conjugated_verb = conjugate(verb, VERB_TENSE, VERB_PERSON, VERB_NUMBER)
        conjugated_verbs.append(conjugated_verb)
        logging.debug('conjugating %s x tense=%s person=%s number=%s; conjugated=%s'%(verb, VERB_TENSE, VERB_PERSON, VERB_NUMBER, conjugated_verb))
    # also add infinitive lol
    conjugated_verbs.append(verb)
    return conjugated_verbs

def conjugate_light_verb(verb, add_inf=False):
    """
    Conjugate light verb and add corresponding noun phrase.
    """
    verb_split = verb.split(' ')
    light_verbs = verb_split[0].split('|')
    # optional: generate light verbs in infinitive form
    # easier querying for Google N-grams!!
    if(add_inf):
        conjugated_light_verbs = list(map(lambda x: f'{x}_INF', light_verbs))
    else:
        conjugated_light_verbs = list(map(conjugate_verb, light_verbs))
        conjugated_light_verbs = list(reduce(lambda x,y: x+y, conjugated_light_verbs))
    # optional: parentheses
    # e.g. hacer (un) post
    non_verb = ' '.join(verb_split[1:])
    paren_phrase_matcher = re.compile('\((.+)\) ')
    paren_matcher = re.compile('[\(\)]')
    if(paren_matcher.search(non_verb) is not None):
        non_verb_list = [
            paren_phrase_matcher.sub(' ', non_verb), 
            paren_phrase_matcher.sub(r' \1 ', non_verb),
        ]
    else:
        non_verb_list = [non_verb]
    # fix duplicate nouns
    # e.g. hacer box|boxing
    clean_non_verb_list = []
    for non_verb_i in non_verb_list:
        dup_matcher = re.compile('\w+\|\w+')
        non_verb_dups = dup_matcher.search(non_verb)
        if(non_verb_dups is not None):
            for non_verb_dup in non_verb_dups.group(0).split('|'):
                non_verb_j = dup_matcher.sub(non_verb_dup, non_verb_i)
                clean_non_verb_list.append(non_verb_j)
        else:
            clean_non_verb_list.append(non_verb_i)
    # join verb + non-verb
    verb_combos = list(product(conjugated_light_verbs, non_verb_list))
    verb_combos = list(map(lambda x: ' '.join(x), verb_combos))
    # clean extra spaces
    space_matcher = re.compile('\s{2,}')
    verb_combos = list(map(lambda x: space_matcher.sub(' ', x), verb_combos))
    return verb_combos

def extract_all_light_verb_phrases(txt):
    """
    Extract all light verb phrases from text.
    """
    verb_str = txt.split(' ')[0]
    non_verb_str = ' '.join(txt.split(' ')[1:])
    verbs = verb_str.split('|')
    verb_phrases = []
    for verb in verbs:
        verb_phrases.append(' '.join([verb, non_verb_str]))
    return verb_phrases

def remove_ambiguous_verb_forms(verb_forms):
    """
    Remove verb forms that may be nouns, 
    e.g. "acceso" (N/V).
    """
    ambiguous_integrated_verbs = ['accesar', 'auditar', 'boxear', 'chequear', 'formear', 'frizar']
    for ambiguous_integrated_verb in ambiguous_integrated_verbs:
        false_positive_verb_matcher = re.compile('^(%s)$'%('|'.join([ambiguous_integrated_verb.replace('ar', 'o'), ambiguous_integrated_verb.replace('ar', 'a')])))
        verb_forms = list(filter(lambda x: false_positive_verb_matcher.search(x) is None, verb_forms))
    return verb_forms

# def conjugate_light_verb(txt):
#     verb_str = txt.split(' ')[0]
#     non_verb_str = ' '.join(txt.split(' ')[1:])
#     verb_forms = conjugate_verb(verb_str)
#     light_verb_forms = list(map(lambda x: ' '.join([x, non_verb_str]), verb_forms))
#     return light_verb_forms
  
def start_es_instance(es_year, es_start_month, es_end_month, es_cluster_name, es_base_dir='/hg190/elastic_search/'):
    """
    Start running ES instance in the background.
    Note: process must be terminated if program breaks!
    
    :param es_year: ES year
    :param es_start_month: ES start month
    :param es_end_month: ES end month
    :param es_cluster_name: ES cluster name ("reddit_comments" or "twitter_posts")
    :param es_base_dir: ES base directory 
    """
    months = ['jan', 'feb', 'mar', 'apr', 'may', 'june', 'july', 'aug', 'sep', 'oct', 'nov', 'dec']
    ES_INITIALIZE_TIMEOUT = 300
    es_dir = os.path.join(es_base_dir, 'es_instances_for_%s'%(es_cluster_name))
    es_instance_name = '%s_%d_m_%d_%d'%(es_cluster_name, es_year, es_start_month, es_end_month)
    es_start_month_str = months[es_start_month-1]
    es_end_month_str = months[es_end_month-1]
    es_dir_full = os.path.join(es_dir, str(es_year), '%s-%s-%d'%(es_start_month_str, es_end_month_str, es_year), 'elasticsearch-2.1.1/bin')
    es_command = './elasticsearch --cluster.name %s --node.name %s'%(es_cluster_name, es_instance_name)
    process = subprocess.Popen(es_command.split(' '), cwd=es_dir_full)
    try:
        sleep(ES_INITIALIZE_TIMEOUT)
    except Exception as e:
        pass
    return process

def collect_scroll_results(es, es_instance_name, es_query, MAX_SCROLL_SIZE=1000, keys_to_include=[]):
    """
    Collect full query results using Elasticsearch scroll.
    """
    res = es.search(index=es_instance_name, body=es_query, size=MAX_SCROLL_SIZE, scroll='2m')
    scroll_id = res['_scroll_id']
    scroll_size = len(res['hits']['hits'])
    full_results = []
    filter_keys = len(keys_to_include) > 0
    while(scroll_size > 0):
#         for i, res_i in enumerate(res['hits']['hits']):
#             full_results.append(res_i)
        res_hits = res['hits']['hits']
        if(filter_keys):
            res_hits_filtered = list(map(lambda x: {'_source' : {y : x['_source'][y] for y in keys_to_include}}, res_hits))
#             res_hits_filtered = {x : res_hits['_source'][x] for x in keys_to_include}
            res_hits = res_hits_filtered
        full_results += res_hits
        res = es.scroll(scroll_id=scroll_id, scroll='2m')
        scroll_id = res['_scroll_id']
        scroll_size = len(res_hits)
        # TODO: memory error??
#         break
    return full_results

def execute_queries_all_instances(es_queries, keys_to_include=[], 
                                  es_year_month_pairs=[(2018, 7, 9), (2018, 10, 12), (2019, 1, 3), (2019, 4, 6)], 
                                  es_cluster_name='reddit_comments', es_base_dir='/hg190/elastic_search/', 
                                  es_port=9200, verbose=True):
    """
    Execute ES queries on all available ES instances
    and combine results.
    
    :param es_queries: ES queries
    :returns combined_results:: combined query results (hits)
    """
    months = ['jan', 'feb', 'mar', 'apr', 'may', 'june', 'july', 'aug', 'sep', 'oct', 'nov', 'dec']
    es_dir = os.path.join(es_base_dir, 'es_instances_for_%s'%(es_cluster_name))
    ES_INITIALIZE_TIMEOUT = 150
    MAX_SIZE = 1000
    ## collect all results
    combined_results = []
    for es_year, es_start_month, es_end_month in es_year_month_pairs:
        es_year_month_results = []
        es_instance_name = '%s_%d_m_%d_%d'%(es_cluster_name, es_year, es_start_month, es_end_month)
        es_start_month_str = months[es_start_month-1]
        es_end_month_str = months[es_end_month-1]
        es_dir_full = os.path.join(es_dir, str(es_year), '%s-%s-%d'%(es_start_month_str, es_end_month_str, es_year), 'elasticsearch-2.1.1/bin')
        if(verbose):
            print('starting index %s'%(es_instance_name))
        # start up ES
#         es_command = './elasticsearch --cluster.name %s --node.name %s'%(es_cluster_name, es_instance_name)
        # specify custom port
        es_command = f'./elasticsearch -Des.http.port={es_port} --cluster.name {es_cluster_name} --node.name {es_instance_name}'
        process = subprocess.Popen(es_command.split(' '), cwd=es_dir_full)
        try:
            try:
                if(verbose):
                    print('sleeping for %d sec to wait for ES to initialize'%(ES_INITIALIZE_TIMEOUT))
                sleep(ES_INITIALIZE_TIMEOUT)
            except Exception as e:
                pass
            # TODO: attempt to open index??
            # may need to do this to have multiple indices open at once
            es_open_command_url = f"http://localhost:{es_port}/{es_instance_name}/_open"
            try:
                es_open_request_result = requests.post(es_open_command_url)
                if(verbose):
                    es_open_request_result_txt = es_open_request_result.text
                    print(f'opened ES instance {es_instance_name} with result\n{es_open_request_result_txt}')
#                 es_open_process = subprocess.Popen(es_open_command.split(' '), cwd='.')
                sleep(ES_INITIALIZE_TIMEOUT)
            except Exception as e:
                if(verbose):
                    print(f'error in opening ES = {e}')
            es_params = [{'host' : 'localhost', 'port' : es_port}]
            es = Elasticsearch(es_params, timeout=100)
            # execute queries
            for es_query in es_queries:
                if(verbose):
                    print(f'processing query {es_query}')
                results = []
                res = collect_scroll_results(es, es_instance_name, es_query, keys_to_include=keys_to_include)
                for i, res_i in enumerate(res):
                    results.append(res_i['_source'])
                if(verbose):
                    print('collected %d results'%(len(results)))
                es_year_month_results.append(results)
        except Exception as e:
            if(verbose):
                print('process ending early because error %s'%(e))
        process.terminate()
        combined_results.append(es_year_month_results)
    return combined_results

## API auth methods

def load_twitter_API(auth_file):
    """
    Load Twitter API with auth.
    """
    auth_data = pd.read_csv(auth_file, sep=',', header=None, names=['key_type', 'key_val'])
    auth_data_lookup = dict(zip(auth_data.loc[:, 'key_type'], auth_data.loc[:, 'key_val']))
    api = twitter.Api(consumer_key=auth_data_lookup['consumer_key'], 
                      consumer_secret=auth_data_lookup['consumer_secret'], 
                      access_token_key=auth_data_lookup['access_token'], 
                      access_token_secret=auth_data_lookup['access_secret'])
    return api

## geolocation
def match_location_country(location, us_state_matcher, latin_america_country_matcher, 
                           unambig_city_matcher, unambig_city_country_lookup, 
                           latin_america_country_codes, us_country_code):
    """
    Match the country/region of a specified location to 
    either US, Latin America, Europe or other.
    
    :param location: location string
    :param us_state_matcher: US state matcher
    :param latin_america_country_matcher: Latin America country matcher
    :param unambig_city_matcher: unambiguous city matcher
    :param unambig_city_country_lookup: unambiguous city-country lookup
    :param latin_america_country_codes: Latin America country codes
    :param us_country_code: US country code
    :return location_country: location country estimate
    """
    clean_location = clean_txt_simple(location)
    us_state_match = us_state_matcher.search(location)
    latin_america_country_match = latin_america_country_matcher.search(location)
    location_country = ''
    if(us_state_match is not None):
        location_country = us_country_code
    elif(latin_america_country_match is not None):
        location_country = 'Latin_America'
    else:
        unambig_city_match = unambig_city_matcher.search(location)
        if(unambig_city_match is not None):
            unambig_city = unambig_city_match.group(0)
            if(unambig_city in unambig_city_country_lookup):
                unambig_city_country = unambig_city_country_lookup[unambig_city]
                if(unambig_city_country in latin_america_country_codes):
                    unambig_city_country = 'Latin_America'
                location_country = unambig_city_country
    return location_country

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

def match_location_region(location_txt, us_state_matcher, country_matcher, country_name_code_lookup, unambig_city_matcher, unambig_city_country_lookup, country_codes):
    # clean text
    clean_location_txt = list(map(clean_txt_simple, location_txt))
    locations = list(map(lambda x: estimate_location_from_txt(x, us_state_matcher, country_matcher, country_name_code_lookup, unambig_city_matcher, unambig_city_country_lookup), clean_location_txt))
    
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
    location_regions = list(map(lambda x: country_region_lookup.get(x) if x in country_region_lookup else 'UNK', locations))
    return location_regions

# chunk query

def generate_chunk_queries(terms, extra_query_params={}, MAX_QUERY_CHUNK_SIZE=50, search_var='body'):
    """
    Generate match queries in chunks, e.g. with 50 terms at once.
    """
    term_chunk_count = int(ceil(len(terms) / MAX_QUERY_CHUNK_SIZE))
    search_term_chunks = list(map(lambda i: terms[(i*MAX_QUERY_CHUNK_SIZE):((i+1)*MAX_QUERY_CHUNK_SIZE)], range(term_chunk_count)))
    es_queries = []
    for search_term_chunk in search_term_chunks:
        es_query = {
            'query' : {
                'match' : {
                    search_var : '|'.join(search_term_chunk)
                }
            }
        }
        # extra params (e.g. lang) => different structure
        if(len(extra_query_params) > 0):
            es_query = {
                'query' : {
                    'bool' : {
                        'must' : [
                            {
                                'match' : {
                                    search_var : '|'.join(search_term_chunk)
                                }
                            },
                        ]
                    }
                }
            }
            for k, v in extra_query_params.items():
                es_query['query']['bool']['must'].append({'match' : {k : v}})
        es_queries.append(es_query)
    return es_queries

# load data from directories

def load_data_from_dirs(data_dirs, file_matcher=None, compression='gzip', use_cols=None, sample_size=None, verbose=False):
    """
    Load data from data directories.
    
    :param data_dirs: data directories
    :param file_matcher: file matcher
    :param compression: file compression type {"gzip", "zip"}
    :param use_cols: columns to include from data
    :returns combined_data: data frame
    """
    combined_data = []
    use_col_matcher = lambda x: x in use_cols
    # default file matcher = tweets
    if(file_matcher is None):
        file_matcher = re.compile('.*tweets\.gz')
    for data_dir in data_dirs:
        data_files = os.listdir(data_dir)
        if(file_matcher != ''):
            data_files = list(filter(lambda x: file_matcher.search(x) is not None, data_files))
        data_files = list(map(lambda x: os.path.join(data_dir, x), data_files))
        # remove directories
        data_files = list(filter(lambda x: not os.path.isdir(x), data_files))
        # optional sample
        if(sample_size is not None):
            np.random.seed(123)
            data_files = np.random.choice(data_files, sample_size, replace=False)
        data = []
        for data_file_i in data_files:
            try:
                if(use_cols is None):
                    data_i = pd.read_csv(data_file_i, sep='\t', index_col=False, compression=compression)
                else:
                    data_i = pd.read_csv(data_file_i, sep='\t', index_col=False, compression=compression, usecols=use_col_matcher)
                data.append(data_i)
            except Exception as e:
                if(verbose):
                    print(f'skipped file {data_file_i} because error {e}')
                else:
                    logging.info(f'skipped file {data_file_i} because error {e}')
        
#         data = list(map(lambda x: pd.read_csv(x, sep='\t', index_col=False, compression=compression), data_files))
        data = pd.concat(data, axis=0)
        # add file directory for later filtering
        data = data.assign(**{
            'file_dir' : os.path.basename(os.path.normpath(data_dir))
        })
        combined_data.append(data)
    combined_data = pd.concat(combined_data, axis=0)
    # fix duplicate index
    combined_data.reset_index(drop=True, inplace=True)
    return combined_data

# URL matches

# def extract_URL_matches(data, URL_matcher):
#     """
#     Extract all matching URLs from data.
    
#     :param data: post data
#     :param URL_matcher: URL regex matcher
#     """
#     URL_matches = []
#     post_URLs = data[data.loc[:, 'urls'] != ''].loc[:, 'urls'].values
#     combined_post_URLs = set()
#     for post_URL_i in post_URLs:
#         # extract list of URLs if needed
#         try:
#             post_URL_i = literal_eval(post_URL_i)
#         # otherwise treat single URL as list
#         except Exception as e:
#             post_URL_i = [post_URL_i]
#             pass
#         post_URLs_i = set(post_URL_i)
#         combined_post_URLs.update(post_URLs_i)
#     URL_matches = list(filter(lambda x: URL_matcher.search(x) is not None, combined_post_URLs))
#     return URL_matches

def extract_URL_matches(post_data, URL_matcher):
    """
    Extract all matching URLs from file.
    """
    URL_matches = []
    post_data.fillna('', inplace=True)
    post_URLs = post_data.loc[:, 'urls'].values
    combined_post_URLs = []
    for post_URL_i in post_URLs:
        if(post_URL_i == ''):
            post_URL_i = [post_URL_i]
        # extract list of URLs if needed
        else:
            try:
                post_URL_i = literal_eval(post_URL_i)
            # otherwise treat single URL as list
            except Exception as e:
                post_URL_i = [post_URL_i]
                pass
        combined_post_URLs.append(post_URL_i)
    URL_matches = list(map(lambda x: list(filter(lambda y: URL_matcher.search(y) is not None, x)), combined_post_URLs))
    return URL_matches

# ugly data loading

def load_data_manual(data_file, sep='\t', max_cols=None, verbose=False, pad_val=None):
    """
    Manually load data from csv format file.
    """
    data = []
    for i, l in enumerate(open(data_file, 'r')):
        if(i == 0):
            data_cols = l.strip().split(sep)
            if(max_cols is not None):
                data_cols = data_cols[:max_cols]
            else:
                max_cols = len(data_cols)
        else:
            data_i = l.strip().split(sep)
            data_i = data_i[:max_cols]
            if(len(data_i) == len(data_cols)):
                data.append(data_i)
            elif(len(data_i) < len(data_cols)):
                if(pad_val is not None):
                    missing_count = len(data_cols) - len(data_i)
                    data_i = data_i + [pad_val,]*missing_count
                    data.append(data_i)
                elif(verbose):
                    print('count=%d/%d, bad line data %s'%(len(data_i), len(data_cols), str(data_i)))
    data = pd.DataFrame(data, columns=data_cols)
    return data

## handle access tokens

class AccessTokenHandler:
    """
    Hold info about access tokens.
    Less clumsy procedure to handle rate limiting
    among access tokens.
    """
    
    def __init__(self, access_tokens, user_IDs):
        self.access_tokens = access_tokens
        self.user_IDs = user_IDs
        self.ctr = 0
        self.access_token_curr = self.access_tokens[self.ctr]
        self.user_ID_curr = self.user_IDs[self.ctr]
        # keeping track of which access tokens are rate limited
        self.access_token_rate_limited = {k : False for k in self.access_tokens}
    
    def next_token(self):
        """
        Increment token to the next one in the list. 
        If index==len(tokens), set index=0 (i.e. start over).
        """
        self.ctr = (self.ctr + 1) % len(self.access_tokens)
        self.access_token_curr = self.access_tokens[self.ctr]
        self.user_ID_curr = self.user_IDs[self.ctr]
        
    def get_access_token_curr(self):
        return self.access_token_curr
    
    def get_user_ID_curr(self):
        return self.user_ID_curr
    
    def get_curr_token_rate_limited(self):
        return self.access_token_rate_limited[self.access_token_curr]
    
    def set_curr_token_rate_limited(self, rate_limited):
        self.access_token_rate_limited[self.access_token_curr] = rate_limited
        
    def get_all_token_rate_limited(self):
        all_token_rate_limited = all(self.access_token_rate_limited.values())
        return all_token_rate_limited
    
def try_literal_eval(x):
    x_list = []
    try:
        x_list = literal_eval(x)
    except Exception as e:
        pass
    return x_list

## handling artist names
def load_artist_names(artist_category='latin_american', data_dir='../../data/culture_metadata/'):
    """
    Load all valid artist names from file.
    """
    if(artist_category == 'latin_american'):
        artist_files = ['../../data/culture_metadata/latin_american_musician_subcategory_dbpedia_data.tsv', '../../data/culture_metadata/latin_american_pop_musicians_en_wiki_data.tsv', '../../data/culture_metadata/latin_american_pop_musicians_es_wiki_data.tsv']
    elif(artist_category == 'us_american'):
        artist_files = ['../../data/culture_metadata/us_american_musician_subcategory_dbpedia_data.tsv', '../../data/culture_metadata/us_american_pop_musicians_en_wiki_data.tsv']
    artist_data = pd.concat(list(map(lambda x: pd.read_csv(x, sep='\t'), artist_files)), axis=0)
    artist_names = list(artist_data.loc[:, 'name'].unique())
    return artist_names

## matching by nearest neighbors for e.g. audience age distributions
def get_matching_pairs(treated_df, non_treated_df, scaler=True, dist_metric='mahalanobis'):
    treated_x = treated_df.values
    non_treated_x = non_treated_df.values
    if scaler == True:
        scaler = StandardScaler()
    if scaler:
        scaler.fit(treated_x)
        treated_x = scaler.transform(treated_x)
        non_treated_x = scaler.transform(non_treated_x)
    
    # how to prevent duplicate matching?
    # greedy matching: randomly select treatment var
    # cosine sim => not good
#     treat_non_treat_dist = treated_df.dot(non_treated_df.T)/(linalg.norm(treated_df)*linalg.norm(non_treated_df))
    # mahalanobis distance => much better
    treat_non_treat_dist = cdist(treated_df.values, non_treated_df.values, dist_metric)
#     treat_non_treat_dist = treat_non_treat_dist.values
    # iterate through treated vals randomly
    np.random.seed(123)
    N_treated = len(treated_df.index)
    treated_df_idx = np.random.choice(list(range(N_treated)), N_treated, replace=False)
    matched_idx = []
    for i in treated_df_idx:
        nearest_neighbors_i = np.argsort(treat_non_treat_dist[i, :])
        nearest_neighbor_idx_i = nearest_neighbors_i[0]
        # get next-nearest neighbor until we get a valid index
        nearest_neighbor_ctr = 1
        while(nearest_neighbor_idx_i in matched_idx):
            nearest_neighbor_idx_i = nearest_neighbors_i[nearest_neighbor_ctr]
            nearest_neighbor_ctr += 1
        matched_idx.append(nearest_neighbor_idx_i)
    matched = non_treated_df.iloc[matched_idx, :]
    # nearest neighbors with replacement...DUMB!!
#     nbrs = NearestNeighbors(n_neighbors=1, algorithm='ball_tree').fit(non_treated_x)
#     distances, indices = nbrs.kneighbors(treated_x)
#     indices = indices.reshape(indices.shape[0])
#     matched = non_treated_df.iloc[indices, :]
    return matched

def forced_matching(treated_df, control_df, cat_vals, scaler=True, dist_metric='mahalanobis'):
    """
    Force treatment and control to match on categorical variable.
    E.g. if we're using LOCATION, when matching we will only
    consider all control units that have identical LOCATION to treatment.
    """
    # get valid idx for each cat val
    treat_control_dist = cdist(treated_df.values, control_df.values, dist_metric)
    treat_control_cat_val_dist = cdist(treated_df.loc[:, cat_vals].values, control_df.loc[:, cat_vals].values, dist_metric)
    np.random.seed(123)
    N_treated = len(treated_df.index)
    treated_df_idx = np.random.choice(list(range(N_treated)), N_treated, replace=False)
    matched_control_idx = []
    matched_treat_idx = []
    for treated_idx_i in treated_df_idx:
        # limit allowed neighbors to matching cat vals
        valid_neighbors = set(np.where(treat_control_cat_val_dist[treated_idx_i, :] == 0.)[0]) - set(matched_control_idx)
        print('%d valid neighbors'%(len(valid_neighbors)))
        if(len(valid_neighbors) > 0):
            nearest_neighbors_i = np.argsort(treat_control_dist[treated_idx_i, :])
            valid_nearest_neighbors_i = list(filter(lambda x: x in valid_neighbors, nearest_neighbors_i))
#             print('%d valid neighbors'%(len(valid_nearest_neighbors_i)))
            nearest_neighbor_idx_i = valid_nearest_neighbors_i[0]
            # get next-nearest neighbor until we get a valid index
            nearest_neighbor_ctr = 1
            while(nearest_neighbor_idx_i in matched_control_idx):
                nearest_neighbor_idx_i = valid_nearest_neighbors_i[nearest_neighbor_ctr]
                nearest_neighbor_ctr += 1
            matched_control_idx.append(nearest_neighbor_idx_i)
            matched_treat_idx.append(treated_idx_i)
        print('%d units assigned'%(len(matched_treat_idx)))
    matched_treat = treated_df.iloc[matched_treat_idx, :]
    matched_control = control_df.iloc[matched_control_idx, :]
    return matched_treat, matched_control

## audience data wrangling

def assign_audience_pct(interests, audience_pct_lookup, audience_pct_count=4):
    """
    Assign audience distribution to list of interests.
    Compute mean over all audience distributions in list.
    """
    interest_pcts = list(map(audience_pct_lookup.get, interests))
    interest_pcts = list(filter(lambda x: x is not None, interest_pcts))
    mean_audience_pct = np.zeros(audience_pct_count)
    # compute mean over all interests
    if(len(interest_pcts) > 0):
        interest_pcts = list(map(lambda x: x.reshape(-1,1), interest_pcts))
        audience_pcts = np.concatenate(interest_pcts, axis=1).T
        mean_audience_pct = np.mean(audience_pcts, axis=0)
    return mean_audience_pct

def parse_date_all_formats(date_str, date_fmts):
    """
    Try all possible formats to parse date.
    """
    date_parsed = ''
    for date_fmt in date_fmts:
        try:
            date_parsed = datetime.strptime(date_str, date_fmt)
        except Exception as e:
#             print(e)
            pass
    if(date_parsed == ''):
        print(f'bad date=<{date_str}>')
    return date_parsed

def clean_date_values(data):
    """
    Convert date variables to usable format.
    """
    # convert date var
    date_fmt_1 = '%Y-%m-%d %H:%M:%S'
    date_fmt_2 = '%a %b %d %H:%M:%S %z %Y'
    date_fmt_3 = '%Y-%m-%d %H:%M:%S%z'
    date_fmts = [date_fmt_1, date_fmt_2, date_fmt_3]
    # clean dates
    date_var = 'created_at'
    alternate_date_var = 'date'
    if(alternate_date_var in data.columns):
        data = data.assign(**{
            date_var : data.apply(lambda x: x.loc[date_var] if x.loc[date_var]!='' else x.loc[alternate_date_var], axis=1)
        })
    clean_date_var = 'clean_date'
    # TODO: why are some of the date formats failing to parse?
    mid_date_matcher = re.compile('(?<=[0-9])T(?=[0-9])') # remove weird T in middle of text
    final_punct_matcher = re.compile('(?<=[\+-][0-9]{2}):(?=00$)') # remove punct in final timezone text
    data = data.assign(**{
        clean_date_var : data.loc[:, date_var].apply(lambda x: mid_date_matcher.sub(' ', final_punct_matcher.sub('', x)))
    })
    data = data.assign(**{
        clean_date_var : data.loc[:, clean_date_var].apply(lambda x: parse_date_all_formats(x, date_fmts))
    })
    return data

def bin_data_var(data, bin_ranges, bin_names, bin_var='es'):
    """
    Bin data variable.
    """
    data_clean = data.loc[:, bin_var]
    data_clean = data_clean.replace('', np.nan)
    data_clean = data_clean.astype(float)
    bin_vals = pd.Series(map(bin_names.get, np.digitize(data_clean, bin_ranges)))
    bin_vals[data_clean.apply(lambda x: np.isnan(x)).values] = ''
    data = data.assign(**{
        f'{bin_var}_bin' : bin_vals.values
    })
    return data

## load integrated verb count data

def load_verb_count_data(data_file):
    """
    Load verb count data collected from raw Twitter data.
    """
    data = pd.read_csv(data_file, sep=',', header=None)
    data.columns= ['loanword', 'integrated_verb', 'light_verb', 'file_name']
    data.dropna(axis=0, inplace=True)
    # get date
    date_fmt = '%b-%d-%y'
    data = data.assign(**{
        'date' : data.loc[:, 'file_name'].apply(lambda x: datetime.strptime('-'.join(x.split('-')[1:4]), date_fmt))
    })
    # get year for more coarse-grained analysis
    data = data.assign(**{
        'date_year' : data.loc[:, 'date'].apply(lambda x: x.year)
    })
    return data

## compute integrated verb rate

def compute_integrated_rate(data):
    """
    Compute integrated verb rate for given verb.
    """
    integrated_rate = data.loc[:, 'integrated_verb'].sum() / data.loc[:, ['integrated_verb', 'light_verb']].sum(axis=1).sum()
    return integrated_rate

def compute_integration_rate_all_words(data):
    """
    Compute integration rate for multiple words.
    """
    integrated_verb_data = data[data.loc[:, 'verb_type']=='integrated_verb']
    light_verb_data = data[data.loc[:, 'verb_type']=='light_verb']
    word_var = 'word'
    count_var = 'count'
    integrated_verb_counts = integrated_verb_data.groupby(word_var).apply(lambda x: x.loc[:, count_var].sum())
    light_verb_counts = light_verb_data.groupby(word_var).apply(lambda x: x.loc[:, count_var].sum())
    integrated_verb_rate = integrated_verb_counts / (integrated_verb_counts + light_verb_counts).fillna(0, inplace=False)
    # remove invalid values
    integrated_verb_rate = integrated_verb_rate[~(np.isinf(integrated_verb_rate) | np.isnan(integrated_verb_rate))]
    return integrated_verb_rate

## change time variables
def add_months(sourcedate, months):
    """
    Add X months to date.
    """
    month = sourcedate.month - 1 + months
    year = sourcedate.year + month // 12
    month = month % 12 + 1
    day = min(sourcedate.day, calendar.monthrange(year,month)[1])
    new_date = datetime(year=year, month=month, day=day)
    return new_date

def bin_by_month_period(date_val, month_bin_window=3):
    """
    Bin date into nearest X-month period.
    We assume that month window divides the year evenly:
    X={2, 3, 4, 6}
    """
    months_per_year = 12
    date_year = date_val.year
    start_date = datetime(year=date_year, day=1, month=1)
    month_bin_count = int(months_per_year / month_bin_window)
    month_bins = list(map(lambda x: add_months(start_date, month_bin_window*x), range(month_bin_count)))
    month_bin_timestamps = list(map(lambda x: x.timestamp(), month_bins))
    date_val_timestamp = date_val.timestamp()
    date_val_bin = np.digitize([date_val_timestamp], month_bin_timestamps)[0]
    date_val_month_bin = month_bins[date_val_bin-1]
    return date_val_month_bin

## style metrics
def compute_word_length(text_tokens, use_log=False):
    token_len = np.array(list(map(len, text_tokens)))
    if(use_log):
        token_len = np.log(token_len)
    mean_token_len = np.mean(token_len)
    return mean_token_len
def compute_sentence_length(text_sent_tokens, use_log=False):
    # tokenize sentences
    sent_len = np.array(list(map(len, text_sent_tokens)))
    if(use_log):
        sent_len = np.log(sent_len)
    mean_sent_len = np.mean(sent_len)
    return mean_sent_len
def compute_type_token_ratio(text_tokens):
    type_token_ratio = len(text_tokens) / len(set(text_tokens))
    return type_token_ratio
def compute_tag_distribution(tagged_sents):
    words, tags = list(zip(*(map(lambda x: list(zip(*x)), tagged_sents))))
    combined_tags = list(reduce(lambda x,y: x+y, tags))
    tag_counts = pd.Series(combined_tags).value_counts()
    tag_dist = tag_counts / tag_counts.sum()
    return tag_dist

# clean tokenization
def word_tokenize_clean(text, tokenizer, filter_word_matcher):
    text_clean = filter_word_matcher.sub('', text)
    text_tokens = tokenizer.tokenize(text_clean)
    return text_tokens
def sent_tokenize_clean(text, word_tokenizer, sent_tokenizer, filter_word_matcher):
    text_clean = filter_word_matcher.sub('', text)
    text_sents = list(filter(lambda x: len(x) > 0, sent_tokenizer.tokenize(text_clean)))
    sent_tokens = list(map(lambda x: word_tokenizer.tokenize(x), text_sents))
    return sent_tokens

## lang ID

def load_lang_id_model():
    """
    Load lang ID model with normalized probabilities.
    """
    lang_id_model = langid.langid.LanguageIdentifier.from_modelstring(langid.langid.model, norm_probs=True)
    return lang_id_model

## significance testing
def binom_test(p_1, p_2, n_1, n_2):
    """
    Compute difference between two binomial samples.
    """
    p_pool = (n_1*p_1+n_2*p_2)/(n_1+n_2)
    z_score = (p_1 - p_2) / (p_pool*(1-p_pool)*(1/n_1 + 1/n_2))**.5
    p_val = 1-scipy.stats.norm.cdf(abs(z_score))
    return z_score, p_val

def test_difference_by_category(data, category_var, category_1, category_2):
    word_var = 'word'
    dep_var = 'integrated_rate'
    data_1 = data[data.loc[:, category_var]==category_1].loc[:, [word_var, dep_var]]
    data_2 = data[data.loc[:, category_var]==category_2].loc[:, [word_var, dep_var]]
    data_combined = pd.merge(data_1, data_2, on=word_var)
    # drop nan vals
    data_combined.dropna(axis=1, how='any', inplace=True)
    data_vals_1 = data_combined.loc[:, f'{dep_var}_x']
    data_vals_2 = data_combined.loc[:, f'{dep_var}_y']
    median_diff = np.median(data_vals_1 - data_vals_2)
    test_stat, p_val = wilcoxon(data_vals_1, data_vals_2)
#     test_stat, p_val = ttest_rel(data_vals_1, data_vals_2)
    return median_diff, test_stat, p_val

def convert_to_day(txt_date, date_matchers, date_formats):
    """
    Convert text date to day (rounding down) based on matching date.
    
    :param txt_date: date in text form
    :param date_matchers: all possible date matching expressions
    :param date_formats: all possible date formats
    :returns date_day:: day of date
    """
    date_day = None
    for date_matcher, date_format in zip(date_matchers, date_formats):
        try:
            txt_day = date_matcher.search(txt_date.strip()).group(0)
            date_day = datetime.strptime(txt_day, date_format)
        except Exception as e:
            pass
    if(date_day is None):
        print(txt_date)
        raise Exception('bad')
    # round down to nearest day
    if(date_day is not None):
        date_day = datetime(year=date_day.year, month=date_day.month, day=date_day.day)
    return date_day