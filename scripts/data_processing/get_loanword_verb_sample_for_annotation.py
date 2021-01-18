"""
Get sample of loanwords from collected
data for annotation:
does replacing the integrated verb
with the light verb change the semantics?
e.g. "yo tuiteo" vs. "yo envio un tweet"
"""
from argparse import ArgumentParser
import logging
import os
import pandas as pd
from collections import defaultdict
import gzip
from nltk.tokenize import sent_tokenize
import json
import re
from data_helpers import clean_txt_emojis

def main():
    parser = ArgumentParser()
    parser.add_argument('data_files', nargs='+')
    parser.add_argument('--loanword_integrated_verb_file', default='../../data/loanword_resources/tweets_loanwords_clean_integrated_verbs.tsv')
    parser.add_argument('--max_samples_per_verb', type=int, default=5)
    parser.add_argument('--max_verbs', type=int, default=10)
    args = vars(parser.parse_args())
    logging_file = '../../output/get_loanword_verb_sample_for_annotation.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.DEBUG)

    ## load verb forms
    integrated_verb_data = pd.read_csv(args['loanword_integrated_verb_file'], sep='\t', index_col=False)
    verb_to_loanword_map = dict(zip(integrated_verb_data.loc[:, 'loanword_verb'].values, integrated_verb_data.loc[:, 'loanword'].values))
    loanword_verb_matcher = re.compile('|'.join(integrated_verb_data.loc[:, 'loanword_verb'].values))
    logging.debug(integrated_verb_data.head())
    
    ## collect samples
    # get the sentence containing the phrase
    sample_data = []
    verb_sample_counts = defaultdict(int)
    max_verbs = args['max_verbs']
    max_samples_per_verb = args['max_samples_per_verb']
    lang = 'spanish'
    txt_var = 'text'
    sample_complete = False
    for f in args['data_files']:
        logging.debug('processing %s'%(f))
        post_ctr = 0
        txt_data_combined = []
        for l in gzip.open(f, 'r'):
            try:
                l_data = json.loads(l.strip())
                l_txt = l_data[txt_var]
                txt_data_combined.append(l_txt)
            except Exception as e:
                print('exception %s'%(e))
                pass
        # deduplicate, clean data
        txt_data_combined = list(set(txt_data_combined))
        txt_data_combined = list(map(lambda x: x.replace('\n', ''), txt_data_combined))
        txt_data_combined = list(map(lambda x: clean_txt_emojis(x), txt_data_combined))
        for l_txt in txt_data_combined:
            l_sents = sent_tokenize(l_txt, language=lang)
            l_phrases = loanword_verb_matcher.findall(l_txt)
            for l_phrase in l_phrases:
                l_loanword = verb_to_loanword_map[l_phrase]
                phrase_sents = list(filter(lambda x: l_phrase in x, l_sents))
                for phrase_sent in phrase_sents:
                    if(verb_sample_counts[l_loanword] < max_samples_per_verb):
                        logging.debug('found verb "%s" for sent "%s"'%(l_phrase, phrase_sent))
                        sample_data.append([l_phrase, phrase_sent, l_loanword])
                        verb_sample_counts[l_loanword] += 1
            post_ctr += 1
            if(post_ctr % 1000 == 0):
                logging.debug('processed %d posts'%(post_ctr))
            # check for completion: 10 verbs, 5 samples per verb
            if(len(list(filter(lambda x: x >= max_samples_per_verb, verb_sample_counts.values()))) >= max_verbs):
                logging.debug('breaking with verb counts\n%s'%(str(verb_sample_counts)))
                sample_complete = True
                break
        if(sample_complete):
            break
    sample_data_cols = ['loanword_verb', 'text', 'loanword']
    sample_data = pd.DataFrame(sample_data, columns=sample_data_cols)
    # add column with light verb equivalent
    ## TODO: add multiple light verbs? would let us customize the light verb phrases
    light_verb = 'hacer'
    sample_data = sample_data.assign(**{
        'loanword_phrase' : sample_data.loc[:, 'loanword'].apply(lambda x: '%s (un) %s'%(light_verb, x))
    })
    # add label col
    sample_data = sample_data.assign(**{
        'substitution_OK' : ''
    })
    # if we have more than allowed count, filter 
    # all loanwords with < max sample count
    if(sample_data.shape[0] > max_samples_per_verb * max_verbs):
        valid_loanwords = sample_data.groupby('loanword').apply(lambda x: x.shape[0] == max_samples_per_verb)
        valid_loanwords = valid_loanwords[valid_loanwords].index
        sample_data = sample_data[sample_data.loc[:, 'loanword'].isin(valid_loanwords)]
    
    ## save to file
    out_dir = os.path.dirname(args['loanword_integrated_verb_file'])
    out_file = os.path.join(out_dir, 'loanword_verb_context_sample.tsv')
    sample_data.to_csv(out_file, sep='\t', index=False)
    
if __name__ == '__main__':
    main()