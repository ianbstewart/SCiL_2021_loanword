"""
Collect subreddit text and metadata into
single .tsv for easy preliminary analysis
e.g. loanword integration across subreddits.
Include all languages to compute rate of EN use.
id | text | POS tags | date | subreddit | user | lang | lang score
"""
from argparse import ArgumentParser
import logging
import os
import pandas as pd
from data_helpers import get_file_iter, clean_txt
import json

def main():
    parser = ArgumentParser()
    parser.add_argument('post_file')
    parser.add_argument('--lang', default='es')
    parser.add_argument('--lang_id_dir', default='lang_id')
    parser.add_argument('--POS_tag_dir', default='POS_tag')
    parser.add_argument('--filtered_subreddit_file', default='../../data/mined_reddit_comments/subreddit_counts/filtered_subreddit_list_LANG=es_MINACTIVEPOSTS=26_MINACTIVETIMEPERIODS=0.txt')
    parser.add_argument('--out_dir', default='../../data/mined_reddit_comments/filtered_subreddit_data/')
    args = vars(parser.parse_args())
    # get post file data
    post_file = args['post_file']
    post_file_format = post_file.split('.')[-1]
    post_file_base = os.path.basename(post_file).replace('.%s'%(post_file_format), '')
    post_file_dir = os.path.dirname(post_file)
    logging_file = '../../output/collect_subreddit_text_meta_data_%s_LANG=%s.txt'%(post_file_base, args['lang'])
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.DEBUG)
    
    ## load data
    # allowed subreddits
    filtered_subreddits = set([l.strip() for l in open(args['filtered_subreddit_file'])])
    # raw text and metadata first
    post_data = []
    subreddit_var = 'subreddit'
    txt_var = 'body'
    post_data_vars = ['id', txt_var, subreddit_var, 'author', 'created_utc']
    # tmp debugging
#     post_ctr_cutoff = 1000
    try:
        post_iter = get_file_iter(args['post_file'])
        post_ctr = 0
        for lines in post_iter:
            if(type(lines) is not list):
                lines = [lines]
            for l in lines:
                try:
                    l_data = json.loads(l)
                    if(l_data[subreddit_var] in filtered_subreddits):
                        # clean text (no return chars!!)
                        l_data[txt_var] = clean_txt(l_data[txt_var])
                        post_data_l = pd.Series({post_data_var : l_data[post_data_var] for post_data_var in post_data_vars})
                        post_data.append(post_data_l)
                    post_ctr += 1
                    if(post_ctr % 1000000 == 0):
                        logging.debug('%d/%d valid posts'%(len(post_data), post_ctr))
                except Exception as e:
                    logging.debug('could not load line because %s'%(e))
                    pass
#             if(len(post_data) > post_ctr_cutoff):
#                 break
    except Exception as e:
        logging.debug('closing file %s'%(args['post_file']))
    post_data = pd.concat(post_data, axis=1).transpose()
    logging.debug('post data = %d'%(post_data.shape[0]))
    
    ## add lang ID data
    lang_id_dir = os.path.join(post_file_dir, args['lang_id_dir'])
    lang_id_file = os.path.join(lang_id_dir, '%s_lang_id.tsv.gz'%(post_file_base))
    lang_id_data = pd.read_csv(lang_id_file, sep='\t', compression='gzip', header=None, index_col=False, names=['id', 'lang', 'score'])
    post_data = pd.merge(post_data, lang_id_data, on='id', how='inner')
    logging.debug('post data + lang ID data = %d'%(post_data.shape[0]))
    
    ## add POS data
    POS_tag_dir = os.path.join(post_file_dir, args['POS_tag_dir'])
    POS_tag_file = os.path.join(POS_tag_dir, '%s_LANG=%s_tagged.tsv.gz'%(post_file_base, args['lang']))
    POS_tag_data = pd.read_csv(POS_tag_file, sep='\t', compression='gzip', index_col=False)
    POS_tag_data.columns = ['id', 'POS']
    post_data = pd.merge(post_data, POS_tag_data, on='id', how='left')
    logging.debug('post data + POS data = %d'%(post_data.shape[0]))
    
    ## save to file
    if(not os.path.exists(args['out_dir'])):
        os.mkdir(args['out_dir'])
    out_file_name = os.path.join(args['out_dir'], '%s_LANG=%s_lang_POS_tag_data.gz'%(post_file_base, args['lang']))
    post_data.to_csv(out_file_name, sep='\t', index=False, compression='gzip')
    
if __name__ == '__main__':
    main()