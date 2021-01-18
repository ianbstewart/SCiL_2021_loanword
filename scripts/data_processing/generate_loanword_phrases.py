"""
Generate loanword phrases for data mining using integrated verbs and light verbs.

Ex. "tweetear" => "tweetear", "tweeteo", etc.
"""
from argparse import ArgumentParser
import logging
import os
from data_helpers import conjugate_verb
from itertools import product
from functools import reduce
import re
import pandas as pd

def main():
    parser = ArgumentParser()
    parser.add_argument('loanword_file') # integrated verbs: data/loanword_resources/../../data/loanword_resources/wiktionary_twitter_reddit_loanword_verbs_integrated_verbs.tsv
    # light verbs: ../../data/loanword_resources/wiktionary_twitter_reddit_loanword_verbs_light_verbs.tsv
#     parser.add_argument('--light_verb_file', default=None)
    parser.add_argument('--out_dir', default='../../data/loanword_resources/')
    args = vars(parser.parse_args())
    loanword_file = args['loanword_file']
    loanword_file_base = os.path.basename(loanword_file).split('.')[0]
    logging_file = '../../output/generate_loanword_phrases_%s.txt'%(loanword_file_base)
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.DEBUG)

    ## generate query phrases from (1) integrated (2) light verbs
    ## load raw data
    phrase_data = pd.read_csv(loanword_file, sep='\t', index_col=False)
    # conjugate all integrated verbs
    if('integrated verb' in phrase_data.columns):
        # some integrated verbs are ambiguous in 3rd person form
        # e.g. 'accesar' -> 'acceso' can be a noun
        ambiguous_integrated_verbs = ['accesar', 'auditar', 'boxear', 'chequear', 'formear', 'frizar']
        conjugated_verb_phrase_data = []
        for loanword, integrated_verb in zip(phrase_data.loc[:, 'loanword'].values, phrase_data.loc[:, 'integrated verb'].values):
            integrated_verb_conjugations = conjugate_verb(integrated_verb)
            # remove likely false positives
            if(integrated_verb in ambiguous_integrated_verbs):
                false_positive_verb_matcher = re.compile('^(%s)$'%('|'.join([integrated_verb.replace('ar', 'o'), integrated_verb.replace('ar', 'a')])))
                integrated_verb_conjugations = list(filter(lambda x: false_positive_verb_matcher.search(x) is None, integrated_verb_conjugations))
            integrated_verb_conjugations_str = '(%s)'%('|'.join(integrated_verb_conjugations))
            conjugated_verb_phrase_data.append([loanword, integrated_verb_conjugations_str])
#             conjugated_verb_phrase_data += list(map(lambda x: (loanword, x), integrated_verb_conjugations))
        conjugated_verb_phrase_data = pd.DataFrame(conjugated_verb_phrase_data, columns=['loanword', 'verb'])
#         print(conjugated_verb_phrase_data.head())
    elif('light verb' in phrase_data.columns):
        conjugated_verb_phrase_data = []
        light_verb_matcher = re.compile('^[a-z\|]+') # assume that light verb comes first
        paren_matcher = re.compile('\((.+)\)')
#         bar_matcher = re.compile('^([a-z]+)\|([a-z]+)|(?<=[\) \(])([a-z]+)\|([a-z]+)')
        noun_matcher = re.compile('[a-z\-\|]+$')
        for loanword, light_verb_phrase in zip(phrase_data.loc[:, 'loanword'].values, phrase_data.loc[:, 'light verb'].values):
            # get verbs from light verb phrase
            light_verbs = light_verb_matcher.search(light_verb_phrase).group(0).split('|')
            # get associated noun phrase
            noun_phrase = ' '.join(light_verb_phrase.split(' ')[1:])
            # paren = optional (e.g. determiner)
            noun_phrase = paren_matcher.sub(r'(\1)?', noun_phrase)
            noun_phrase = noun_phrase.replace(')? ', ' )?')
#             print('clean noun phrase "%s"'%(noun_phrase))
            # isolate noun phrase
            noun_phrase_noun_str = noun_matcher.search(noun_phrase).group(0)
            # handle multiple nouns
            noun_phrase_nouns = noun_phrase_noun_str.split('|')
            if(len(noun_phrase_nouns) > 1):
                noun_phrase = noun_phrase.replace(noun_phrase_noun_str, '(%s)'%('|'.join(noun_phrase_nouns)))
#             print('post-transform noun phrase "%s"'%(noun_phrase))
            # catch OR pairs
#             noun_phrase = bar_matcher.sub(r'(\1|\2)', noun_phrase)
            # fix paren boundaries for mid-phrase parens
            # ex. "hacer (un?) tweet" => "hacer (un ?)tweet"
#             print('post-transform noun phrase "%s"'%(noun_phrase))
            combined_conjugated_light_verb_phrases = []
            for light_verb in light_verbs:
                light_verb_conjugations = conjugate_verb(light_verb)
#                 conjugated_light_verb_phrases = "(%s) %s"%('|'.join(light_verb_conjugations), noun_phrase)
                combined_conjugated_light_verb_phrases += light_verb_conjugations
            conjugated_light_verb_phrase_str = "(%s) %s"%('|'.join(combined_conjugated_light_verb_phrases), noun_phrase)
#                 light_verb_phrases_conjugated = list(map(lambda x: '%s %s'%(x, noun_phrase), light_verb_conjugations))
#                 light_verb_phrases_combined += light_verb_phrases_conjugated
#             conjugated_verb_phrase_data += list(map(lambda x: (loanword, x), light_verb_phrases_combined))
            conjugated_verb_phrase_data.append([loanword, conjugated_light_verb_phrase_str])
#             print('conjugated verb phrases\n%s'%(str(conjugated_verb_phrase_data)))
        conjugated_verb_phrase_data = pd.DataFrame(conjugated_verb_phrase_data, columns=['loanword', 'verb'])
    logging.debug('conjugated verb phrase data\n%s'%(conjugated_verb_phrase_data.head()))
    
    ## write to file
    out_file = os.path.join(args['out_dir'], '%s_query_phrases.tsv'%(loanword_file_base))
    conjugated_verb_phrase_data.to_csv(out_file, sep='\t', index=False)
    
if __name__ == '__main__':
    main()