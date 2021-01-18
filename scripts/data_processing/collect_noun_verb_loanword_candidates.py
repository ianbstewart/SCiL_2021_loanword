"""
Collect noun/verb loanword candidates based on
high N/V tag rates and/or presence of verb inflection
for N words.
"""
from argparse import ArgumentParser
import logging
import os
import numpy as np
import pandas as pd
import scipy.sparse
from sklearn.preprocessing import normalize
import re

def main():
    parser = ArgumentParser()
    parser.add_argument('tag_pct_file')
#     parser.add_argument('--loanword_file', default='')
    parser.add_argument('--noun_pct_cutoff', type=float, default=0.25)
    parser.add_argument('--verb_pct_cutoff', type=float, default=0.25)
    parser.add_argument('--word_count_file', default='../../data/mined_tweets/freq_data/tweets_combined_word_count.npz')
    parser.add_argument('--standard_lang_word_file', default='../../data/loanword_resources/ES_words.txt')
    args = vars(parser.parse_args())
    logging_file = '../../output/collect_noun_verb_loanword_candidates.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.DEBUG)

    ## load tag data
    tag_pct_file = args['tag_pct_file']
    tag_pct_data_fmt = os.path.splitext(os.path.basename(tag_pct_file))[-1][1:]
    tag_file_base = os.path.basename(tag_pct_file).replace('.%s'%(tag_pct_data_fmt), '')
    if(tag_pct_data_fmt == 'tsv'):
        tag_pct_data = pd.read_csv(tag_pct_file, sep='\t', index_col=0)
    elif(tag_pct_data_fmt == 'npz'):
        tag_pct_data = scipy.sparse.load_npz(tag_pct_file)
        tag_pct_row_file = tag_pct_file.replace('.npz', '_rows.txt')
        tag_pct_col_file = tag_pct_file.replace('.npz', '_cols.txt')
        tag_pct_rows = [l.strip() for l in open(tag_pct_row_file, 'r')]
        tag_pct_cols = [l.strip() for l in open(tag_pct_col_file, 'r')]
        # normalize per-row (tags sum to 1)
        tag_pct_data = normalize(tag_pct_data, norm='l1', axis=1)
        tag_pct_data = pd.DataFrame(tag_pct_data.todense(), index=tag_pct_rows, columns=tag_pct_cols)
    logging.debug('tag pct data\n%s'%(tag_pct_data.head()))
    # normalize!!
#     tag_pct_data = tag_pct_data / tag_pct_data.sum(axis=1)
    
    ## identify candidates consistently tagged as noun/verb
    noun_col = 'N'
#     noun_candidate_tag_pcts = tag_pct_data[tag_pct_data.loc[:, noun_col] >= args['noun_pct_cutoff']].loc[:, noun_col].sort_values(inplace=False, ascending=False)
    verb_col = 'V'
    noun_verb_candidate_tag_pcts = tag_pct_data[(tag_pct_data.loc[:, noun_col] >= args['noun_pct_cutoff']) & (tag_pct_data.loc[:, verb_col] >= args['verb_pct_cutoff'])].loc[:, noun_col].sort_values(inplace=False, ascending=False)
    logging.debug('%d valid noun/verb candidates'%(len(noun_verb_candidate_tag_pcts)))
    logging.debug(noun_verb_candidate_tag_pcts.tail(50)) # look at the tail for possible outliers (e.g. is "swimming" really a noun?)
    
    ## identify verb candidates for noun in data by inflection
    verb_inflections = ['ed', 'ing']
    # handle words with word-final <e>
    noun_ending_matcher_e = re.compile('e$')
    ## TODO: word-final <y> => -ied
    noun_ending_matcher_i = re.compile('y$')
    noun_ending_matchers = [[noun_ending_matcher_e, ''], [noun_ending_matcher_i, 'i']]
    noun_ending_matcher_combined = re.compile('|'.join([x[0].pattern for x in noun_ending_matchers]))
    # TODO: handle short-vowel words? "bat" => "batting"
    noun_candidates = noun_verb_candidate_tag_pcts.index
    vocab = tag_pct_data.index.tolist()
    noun_verb_candidates = []
    noun_verb_candidates = []
    for noun_candidate in noun_candidates:
        # handle word-final patterns
        if(noun_ending_matcher_combined.search(noun_candidate) is not None):
            for noun_ending_matcher, noun_ending_sub in noun_ending_matchers:
                if(noun_ending_matcher.search(noun_candidate) is not None):
                    noun_candidate_stem = noun_ending_matcher.sub('', noun_candidate)
                    generated_verbs = ['%s%s'%(noun_candidate_stem, verb_inflection) for verb_inflection in verb_inflections]
                    break
        else:
            generated_verbs = ['%s%s'%(noun_candidate, verb_inflection) for verb_inflection in verb_inflections]
        valid_generated_verbs = list(filter(lambda x: x in vocab, generated_verbs))
        if(len(valid_generated_verbs) > 0):
            noun_verb_candidate_pair = [noun_candidate, valid_generated_verbs]
            noun_verb_candidates.append(noun_verb_candidate_pair)
    logging.debug('%d noun verb candidates'%(len(noun_verb_candidates)))
    
    ## save! noun form of noun verb candidates
    out_dir = os.path.dirname(args['tag_pct_file'])
    candidate_out_file_name = os.path.join(out_dir, '%s_noun_verb_candidates_NOUN=%.2f_VERB=%.2f.txt'%(tag_file_base, args['noun_pct_cutoff'], args['verb_pct_cutoff']))
    valid_noun_candidates, valid_verb_candidates = zip(*noun_verb_candidates)
    with open(candidate_out_file_name, 'w') as out_file:
        out_file.write('\n'.join(valid_noun_candidates))
        
    ## filter for loanwords using ES vocabulary
    ## TODO: remove standard ES words? from dict
    word_counts = scipy.sparse.load_npz(args['word_count_file'])
    word_count_rows = [l.strip() for l in open(args['word_count_file'].replace('.npz', '_rows.txt'))]
    word_count_totals = pd.Series(np.asarray(word_counts.sum(axis=1))[:, 0], index=word_count_rows).sort_values(inplace=False, ascending=False)
    # filter by cutoff
    word_count_pct_cutoff = 25
#     word_count_totals_cutoff = word_count_totals.iloc[:word_count_rank_cutoff]
    word_count_totals_cutoff_pct = np.percentile(word_count_totals, word_count_pct_cutoff)
    word_count_totals_cutoff = word_count_totals[word_count_totals >= word_count_totals_cutoff_pct]
    logging.debug('filtering for %d words'%(word_count_totals_cutoff.shape[0]))
    word_count_vocab = set(word_count_totals_cutoff.index)
    # noun filter: check for nouns (bare word)
    filtered_valid_noun_loanwords = list(filter(lambda x: x in word_count_vocab, valid_noun_candidates))
    logging.debug('%d loanword noun candidates'%(len(filtered_valid_noun_loanwords)))
    # verb filter: check for verbs (infinitive + past + present)
    es_verb_suffixes = ['e?ar', 'e?ado', 'e?o', 'e?as', 'e?amos', 'e?áis', 'e?an', 'e?ás']
#     es_verb_suffix_matcher = re.compile('|'.join(es_verb_suffixes))
    filtered_valid_noun_verb_loanwords = []
    for noun_loanword in filtered_valid_noun_loanwords:
        es_verb_suffix_matcher = re.compile('|'.join(['^%s%s$'%(noun_loanword, es_verb_suffix) for es_verb_suffix in es_verb_suffixes]))
        verb_loanword_matches = list(filter(lambda x: es_verb_suffix_matcher.search(x) is not None, word_count_vocab))
        if(len(verb_loanword_matches) > 0):
            logging.debug('found matches for noun loanword %s:\n%s'%(noun_loanword, ','.join(verb_loanword_matches)))
            filtered_valid_noun_verb_loanwords.append(noun_loanword)
    logging.debug('%d loanword noun/verb candidates'%(len(filtered_valid_noun_verb_loanwords)))
    # convert to str
#     filtered_valid_noun_verb_loanwords = list(map(lambda x: x.decode('utf-8'), filtered_valid_noun_verb_loanwords))
    
    # remove standard words from list (false friends like "fin")
    standard_lang_words = []
    # doing this with for-loop because of encoding errors??
    for l in open(args['standard_lang_word_file'], 'rb'):
        try:
            l = l.strip()
            standard_lang_word = l.decode('utf-8')
            standard_lang_words.append(standard_lang_word)
        except Exception as e:
            pass
    standard_lang_words = set(standard_lang_words)
#     print('standard words sample %s'%(str(list(standard_lang_words)[:20])))
    filtered_valid_noun_verb_loanwords = list(set(filtered_valid_noun_verb_loanwords) - standard_lang_words)
    # tmp debugging 
#     test_standard_word = 'fin'
#     print('%s in standard words %s'%(test_standard_word, test_standard_word in standard_lang_words))
#     print('%s in loanwords %s'%(test_standard_word, test_standard_word in filtered_valid_noun_verb_loanwords))
    
    ## write to file
    loanword_out_file_name = candidate_out_file_name.replace('.txt', '_loanwords.txt')
    with open(loanword_out_file_name, 'w') as out_file:
        out_file.write('\n'.join(filtered_valid_noun_verb_loanwords))
    
if __name__ == '__main__':
    main()