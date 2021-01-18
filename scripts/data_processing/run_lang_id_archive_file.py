"""
Run langid on all post archive files, and store label, score of highest-scoring language.
Store in a separate file for easy access later.
"""
from argparse import ArgumentParser
import logging
import os
import gzip
from bz2 import BZ2File
from data_helpers import ZstdWrapper
# import langid
import cld2 # 10x faster than langid: https://github.com/GregBowyer/cld2-cffi
import json
import lzma

def main():
    parser = ArgumentParser()
    parser.add_argument('post_file')
#     parser.add_argument('--out_dir', default='/hg190/corpora/twitter-crawl/new-archive/lang_id/')
    parser.add_argument('--out_dir', default=None)
    parser.add_argument('--text_var', default='text')
    parser.add_argument('--id_var', default='id')
    # old argument for langid to speed up processing
#     parser.add_argument('--allowed_langs', nargs='+', default=['en', 'es', 'fr', 'ms', 'ja', 'ar', 'ru', 'pt', 'tr', 'ko']) # based on most popular languages in 2013 https://mashable.com/2013/12/17/twitter-popular-languages/
    args = vars(parser.parse_args())
    post_file = args['post_file']
    logging_file = '../../output/run_lang_id_archive_file_%s.txt'%(os.path.basename(post_file))
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.DEBUG, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    ## manage output directory
    ## default: same as post file
    if('out_dir' not in args or args['out_dir'] is None):
        out_dir = os.path.join(os.path.dirname(post_file), 'lang_id')
        if(not os.path.exists(out_dir)):
            os.mkdir(out_dir)
    else:
        out_dir = args['out_dir']
    
    ## set up langid
    ## we restrict languages for faster runtime (~ 10x, from 238 tweets/sec to 1282 tweets/sec)
#     if('allowed_langs' in args and len(args['allowed_langs']) > 0):
#         logging.debug('allowed langs = %s'%(','.join(args['allowed_langs'])))
#         langid.set_languages(args['allowed_langs'])
    
    ## iterate over all data and write langid output to file
    post_file_format = post_file.split('.')[-1]
    out_file_name = os.path.join(out_dir, os.path.basename(post_file.replace('.%s'%(post_file_format), '_lang_id.tsv.gz')))
    logging.debug('writing to file %s'%(out_file_name))
    line_ctr = 0
    delete_val = '[deleted]'
    if(not os.path.exists(out_file_name)):
        with gzip.open(out_file_name, 'wt') as out_file:
            try:
                if(post_file_format == 'gz'):
                    post_file_input = gzip.open(post_file)
                elif(post_file_format == 'bz2'):
                    post_file_input = BZ2File(post_file)
                elif(post_file_format == 'xz'):
                    post_file_input = lzma.open(post_file)
                elif(post_file_format == 'zst'):
                    post_file_input = ZstdWrapper(post_file) # WARNING returns list of lines to process
#                     decomp = ZstdDecompressor()
#                     post_file_input = decomp.read_to_iter(open(post_file, 'r'))
                for lines in post_file_input:
                    # for consistency: convert each line to list of lines
                    if(type(lines) is str or type(lines) is bytes):
                        lines = [lines]
                    for l in lines:
                        try:
                            l_data = json.loads(l.strip())
                            if('delete' not in l_data.keys() and l_data[args['text_var']] != delete_val):
                                status_id = l_data[args['id_var']]
                                txt = l_data[args['text_var']]
        #                         txt_lang, txt_lang_conf_score = langid.classify(txt)
                                # use most likely language and score
                                txt_lang_results = cld2.detect(txt).details
                                txt_lang = txt_lang_results[0].language_code
                                txt_lang_conf_score = txt_lang_results[0].percent
                                # writing lang + ID to file
                                out_file.write('%s\n'%('\t'.join([str(status_id), txt_lang, '%.3f'%(txt_lang_conf_score)])))
                                # writing full data to file
                #                 l_data['lang_id'] = {'lang' : txt_lang[0], 'score' : txt_lang[1]}
                #                 out_file.write('%s\n'%(json.dumps(l_data)))
                                line_ctr += 1
                                if(line_ctr % 1000000 == 0):
                                    logging.debug('processed %d lines'%(line_ctr))
                        except Exception as e:
                            logging.debug('error = %s'%(e))
                            pass
                post_file_input.close()
            except Exception as e:
                logging.debug('closing input file')
                post_file_input.close()

if __name__ == '__main__':
    main()