"""
Mine reddit comments for specified subreddit, language, etc.
"""
from argparse import ArgumentParser
import logging
import os
import gzip
import json
import pandas as pd
import re
from bz2 import BZ2File

def mine_comments(data_file, out_file_name, lang=None, lang_id_dir='', subreddit=None, subreddits=None, phrases=None, users=None):
    """
    Mine comments for specified metadata.
    """
    # load lang ID data
    if(lang is not None):
        data_file_format = '.'.join(os.path.basename(data_file).split('.')[1:])
        lang_id_file = os.path.join(lang_id_dir, os.path.basename(data_file).replace('.%s'%(data_file_format), '_lang_id.tsv.gz'))
        lang_id_data = pd.read_csv(lang_id_file, sep='\t', index_col=False, compression='gzip', header=None, names=['id', 'lang', 'score'])
        valid_lang_ids = set(lang_id_data[lang_id_data.loc[:, 'lang']==lang].loc[:, 'id'].unique())
        logging.debug('%d valid IDs with lang=%s'%(len(valid_lang_ids), lang))
    if(phrases is not None):
        # we want phrases to match tokenized words
        punct = '[ ,\.?!;:]'
        punct_phrase_template = '|'.join(['^%s'+punct, punct+'%s$', punct+'%s'+punct])
        phrases_clean = [punct_phrase_template%(phrase,)*3 for phrase in phrases]
        phrase_matcher = re.compile('|'.join(phrases_clean))
    comment_match_ctr = 0
    comment_ctr = 0
    id_var = 'id'
    subreddit_var = 'subreddit'
    txt_var = 'body'
    user_var = 'author'
    with gzip.open(out_file_name, 'wt') as out_file:
        with BZ2File(data_file, 'r') as data_input:
            for l in data_input:
                try:
                    l_data = json.loads(l.strip())
                    comment_ctr += 1
                    valid_lang = True
                    if(lang is not None):
                        l_id = l_data[id_var]
                        valid_lang = l_id in valid_lang_ids
                    valid_subreddit = True
                    if(subreddits is not None):
                        l_subreddit = l_data[subreddit_var]
                        valid_subreddit = l_subreddit in subreddits
                    valid_phrase = True
                    phrase_matches = []
                    if(phrases is not None):
                        l_txt = l_data[txt_var].lower()
                        phrase_matches = phrase_matcher.findall(l_txt)
                        valid_phrase = len(phrase_matches) > 0
                    valid_user = True
                    if(users is not None):
                        l_user = l_data[user_var]
                        valid_user = l_user in users
                    valid_post = (valid_lang and valid_subreddit and valid_phrase and valid_user)
                    if(valid_post):
                        comment_match_ctr += 1
                        if(len(phrase_matches) > 0):
                            l_data['phrase_matches'] = phrase_matches
                        out_file.write('%s\n'%(json.dumps(l_data)))
                        if(comment_match_ctr % 1000 == 0):
                            logging.debug('%d/%d comments mined'%(comment_match_ctr, comment_ctr))
                except Exception as e:
                    print(e)
                    pass

def main():
    parser = ArgumentParser()
    parser.add_argument('data_file')
    parser.add_argument('--out_dir', default='../../data/mined_reddit_comments/')
    parser.add_argument('--lang_id_dir', default='lang_id/')
    parser.add_argument('--lang', default=None)
    parser.add_argument('--phrase_file', default=None)
    ## TODO: subreddit -> valid subreddits in file
    parser.add_argument('--subreddit_file', default=None)
    parser.add_argument('--subreddit', default=None)
    parser.add_argument('--user_file', default=None)
    args = vars(parser.parse_args())
    out_file_str = os.path.basename(args['data_file']).replace('.bz2', '')
    if('lang' in args and args['lang'] is not None):
        lang = args['lang']
        lang_id_dir = os.path.join(os.path.dirname(args['data_file']), args['lang_id_dir'])
        out_file_str += '_LANG=%s'%(lang)
    else:
        lang = None
        lang_id_dir = None
    if('subreddit' in args and args.get('subreddit') is not None):
        subreddit = args['subreddit']
        out_file_str += '_SUBREDDIT=%s'%(subreddit)
    else:
        subreddit = None
    if(args.get('subreddit_file') is not None):
        subreddit_file_base = os.path.basename(args['subreddit_file']).split('.')[0]
        subreddits = set([l.strip() for l in open(args['subreddit_file'])])
        out_file_str += '_SUBREDDITS=%s'%(subreddit_file_base)
    else:
        subreddits = None
    if(args.get('phrase_file') is not None):
        phrase_file = args['phrase_file']
        phrase_file_base = os.path.basename(phrase_file).split('.')[0]
        phrases = [l.strip() for l in open(phrase_file, 'r')]
        out_file_str += '_PHRASES=%s'%(phrase_file_base)
    else:
        phrases = None
    if(args.get('user_file') is not None):
        user_file = args['user_file']
        user_file_base = os.path.basename(user_file).split('.')[0]
        users = [l.strip() for l in open(user_file, 'r')]
        out_file_str += '_USERS=%s'%(user_file_base)
    else:
        users = None
    logging_file = '../../output/mine_reddit_comments_%s.txt'%(out_file_str)
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.DEBUG)
    
    ## mine comments
    out_dir = args['out_dir']
    if(not os.path.exists(out_dir)):
        os.mkdir(out_dir)
    out_file_name = os.path.join(out_dir, '%s.gz'%(out_file_str))
    mine_comments(args['data_file'], out_file_name, lang=lang, lang_id_dir=lang_id_dir, subreddits=subreddits, phrases=phrases, users=users)
    
if __name__ == '__main__':
    main()