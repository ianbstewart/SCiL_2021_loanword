"""
Clean sampled integrated verb data for annotation.
(sampled in sample_post_txt_by_phrase.py)

E.g. for "tweet / te voy a tuitear" we want to provide
the corresponding light verb as an alternative
e.g. "tweet / te voy a tuitear / te voy a hacer un tweet"
"""
from argparse import ArgumentParser
import logging
import os
from pattern.es import tenses, conjugate
import pandas as pd
import re

def generate_light_verb_phrase(light_verb, word_match, noun, internal_str=''):
    word_match_tense = tenses(word_match)[0]
    conjugated_light_verb = conjugate(light_verb, word_match_tense)
#     if(determiner):
#         light_verb_phrase = '%s un %s'%(conjugated_light_verb, noun)
#     else:
#         light_verb_phrase = '%s %s'%(conjugated_light_verb, noun)
    if(internal_str != ''):
        light_verb_phrase = f'{conjugated_light_verb} {internal_str} {noun}'
    else:
        light_verb_phrase = f'{conjugated_light_verb} {noun}'
    return light_verb_phrase

def main():
    parser = ArgumentParser()
    parser.add_argument('sample_data')
    parser.add_argument('--light_verb_data', default='../../data/loanword_resources/wiktionary_twitter_reddit_loanword_verbs_light_verbs.tsv')
    args = vars(parser.parse_args())
    sample_data_file = args['sample_data']
    sample_data_file_base = os.path.basename(sample_data_file).split('.')[0]
    logging_file = '../../output/clean_sample_integrated_verb_data_for_annotation_%s.txt'%(sample_data_file_base)
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    ## make light verb match the integrated verb tense/person
    sample_data = pd.read_csv(sample_data_file, sep='\t', index_col=False)
    sample_data.rename(columns={'word_match' : 'integrated_verb'}, inplace=True)
    light_verb_data = pd.read_csv(args['light_verb_data'], sep='\t', index_col=False)
    # split light verbs
    verb_matcher = re.compile('^[a-z\|]+')
    noun_matcher = re.compile('[a-z\-\|]+$')
    internal_matcher = re.compile('(?<=[a-z\-\|] )+([\(\)a-z]+)(?= [a-z\-\|])+')
    light_verb_data = light_verb_data.assign(**{
        'verb_list' : light_verb_data.loc[:, 'light verb'].apply(lambda x: verb_matcher.search(x).group(0).split('|')),
        'noun_list' : light_verb_data.loc[:, 'light verb'].apply(lambda x: noun_matcher.search(x).group(0).split('|')),
        'internal' : light_verb_data.loc[:, 'light verb'].apply(lambda x: internal_matcher.search(x).group(0) if internal_matcher.search(x) is not None else '')
    })
    # catch parentheticals
    # e.g. "hacer (un) boicot" => ["un", ""]
    paren_matcher = re.compile('(?<=\))([a-z\-\|]+)(?=\))')
    light_verb_data = light_verb_data.assign(**{
        'internal_phrase_list' : light_verb_data.loc[:, 'internal'].apply(lambda x: paren_matcher.search(x).group(0).split('|') + [''] if paren_matcher.search(x) is not None else x.split('|'))
    })
    # flatten
    flat_light_verb_data = []
    for _, row in light_verb_data.iterrows():
        for verb in row.loc['verb_list']:
            for noun in row.loc['noun_list']:
                for internal_phrase in row.loc['internal_phrase_list']:
                    flat_light_verb_data.append([row.loc['loanword'], verb, internal_phrase, noun])
    flat_light_verb_data = pd.DataFrame(flat_light_verb_data, columns=['word', 'light_verb', 'light_verb_internal', 'light_verb_noun'])
    sample_data = pd.merge(sample_data, flat_light_verb_data.loc[:, ['word', 'light_verb', 'light_verb_internal', 'light_verb_noun']], on='word')
#     print(sample_data.head(20))
    # get light verb with/without determiner
    sample_light_verb_phrase = sample_data.apply(lambda x: generate_light_verb_phrase(x.loc['light_verb'], x.loc['integrated_verb'], x.loc['light_verb_noun'], x.loc['light_verb_internal']), axis=1)
#     sample_light_verb_phrase_no_det = sample_data.apply(lambda x: generate_light_verb_phrase(x.loc['light_verb'], x.loc['integrated_verb'], x.loc['light_verb_noun'], determiner=False), axis=1)
#     sample_light_verb_phrase = pd.concat([sample_light_verb_phrase_with_det, sample_light_verb_phrase_no_det])
    sample_data = sample_data.assign(**{
        'light_verb_phrase' : sample_light_verb_phrase.values
    })
    sample_data.sort_values(['word', 'integrated_verb'], inplace=True, ascending=True)
    logging.warning(sample_data.head(20))
#     default_light_verb = 'hacer'
#     sample_data = sample_data.assign(**{
#         'light_verb' : sample_data.apply(lambda x: generate_light_verb_phrase(default_light_verb, x.loc['word_match'], x.loc['word']), axis=1)
#     })
    # generate equivalent sentence
    sample_data = sample_data.assign(**{
        'text_alternative' : sample_data.apply(lambda x: x.loc['text'].replace(x.loc['integrated_verb'], x.loc['light_verb_phrase']), axis=1)
    })
    # add columns for label, alternative
    sample_data = sample_data.assign(**{
        'equivalent_label' : -1,
        'alternative_light_verb' : -1,
    })
    print('%d samples total'%(sample_data.shape[0]))
    
    ## save file
    sample_out_file = sample_data_file.replace('.tsv', '_light_verb_annotated.csv')
    sample_data.to_csv(sample_out_file, sep=',', index=False) # save as .csv for AMT
    
    ## optional: subset data for specific verb for pilot test
    ## e.g. restrict to "googlear" and add fake light verb for true negatives
    pilot_loanword = 'google'
    pilot_true_sample_data = sample_data[sample_data.loc[:, 'word'] == pilot_loanword]
    pilot_true_light_verb = 'buscar'
    # replace light verbs in false data
    pilot_false_sample_data = pilot_true_sample_data.copy()
    pilot_false_light_verb = 'dar'
    pilot_false_internal_str = ''
    pilot_false_sample_data = pilot_false_sample_data.assign(**{
        'light_verb' : pilot_false_light_verb,
        'light_verb_internal' : pilot_false_internal_str,
    })
    pilot_false_sample_data = pilot_false_sample_data.assign(**{
        'light_verb_phrase' : pilot_false_sample_data.apply(lambda x: generate_light_verb_phrase(x.loc['light_verb'], x.loc['integrated_verb'], x.loc['word'], x.loc['light_verb_internal']), axis=1)
    })
    pilot_false_sample_data = pilot_false_sample_data.assign(**{
        'text_alternative' : pilot_false_sample_data.apply(lambda x: x.loc['text'].replace(x.loc['integrated_verb'], x.loc['light_verb_phrase']), axis=1)
    })
    pilot_true_out_file = sample_out_file.replace('.csv', f'_{pilot_loanword}_true_VERB={pilot_true_light_verb}.csv')
    pilot_false_out_file = sample_out_file.replace('.csv', f'_{pilot_loanword}_false_VERB={pilot_false_light_verb}.csv')
    pilot_true_sample_data.to_csv(pilot_true_out_file, sep=',', index=False)
    pilot_false_sample_data.to_csv(pilot_false_out_file, sep=',', index=False)
    
if __name__ == '__main__':
    main()