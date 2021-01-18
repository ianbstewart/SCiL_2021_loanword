"""
Extract prior language use for authors.
I.e. total count of EN, ES etc.
"""
from argparse import ArgumentParser
import logging
import os
import re
from data_helpers import clean_tweet_txt
from langid import langid
from langid.langid import LanguageIdentifier, model
import numpy as np
import pandas as pd
from scipy.sparse import dok_matrix, save_npz
from multiprocessing import Pool

def extract_lang_counts(data, score_cutoff, author_var='screen_name', txt_var='text'):
    """
    Extract lang counts from data, filter for high-confidence langs, convert to percents.
    """
    identifier = LanguageIdentifier.from_modelstring(model, norm_probs=True)
    data = data.assign(**{
            txt_var : data.loc[:, txt_var].apply(clean_tweet_txt)
    })
    lang = data.loc[:, txt_var].apply(lambda x: identifier.classify(x))
    # split into lang/score
    lang_val = lang.apply(lambda x: x[0])
    lang_score = lang.apply(lambda x: x[1])
    lang_data = pd.concat([lang_val, lang_score], axis=1)
    lang_data.columns = ['lang', 'lang_score']
    # limit to high-score lang posts
    lang_data = lang_data[lang_data.loc[:, 'lang_score'] >= score_cutoff]
    lang_counts = lang_data.loc[:, 'lang'].value_counts().reset_index().rename(columns={'index' : 'lang', 'lang' : 'lang_count'})
    # convert to percent
    lang_counts = lang_counts.assign(**{
        'lang_count' : lang_counts.loc[:, 'lang_count'] / lang_counts.loc[:, 'lang_count'].sum()
    })
    name = data.loc[:, author_var].iloc[0]
    lang_counts = lang_counts.assign(**{
        'screen_name' : name
    })
    return lang_counts

def extract_lang_counts_star(params):
    data, identifier = params
    return extract_lang_counts(data, identifier)

def main():
    parser = ArgumentParser()
    parser.add_argument('data_dirs', nargs='+')
    parser.add_argument('--lang_id_data', default='lang_id.gz')
    parser.add_argument('--out_dir', default='../../data/mined_tweets/')
    args = vars(parser.parse_args())
    logging_file = '../../output/extract_lang_use_for_authors.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    ## load data
    data_dirs = args['data_dirs']
    file_matcher = re.compile('.+tweets\.gz')
    combined_data = []
    author_var = 'screen_name'
    txt_var = 'text'
#     url_var = 'permalink'
    id_var = 'id'
    name_url_matcher = re.compile('(?<=twitter\.com/)[a-zA-Z0-9_]+')
    use_cols = [txt_var, id_var, author_var]
    for data_dir in data_dirs:
        data_files = list(filter(lambda x: file_matcher.search(x) is not None, os.listdir(data_dir)))
        data_files = list(map(lambda x: os.path.join(data_dir, x), data_files))
        data = list(map(lambda x: pd.read_csv(x, sep='\t', index_col=False, compression='gzip', usecols=use_cols), data_files))
        data = pd.concat(data, axis=0)
        data.fillna('', inplace=True)
        # remove null text
        data = data[data.loc[:, 'text'] != '']
        # get lang ID data
        lang_id_data_file = os.path.join(data_dir, args['lang_id_data'])
        lang_id_data = pd.read_csv(lang_id_data_file, sep='\t', index_col=False, compression='gzip')
        data = pd.merge(lang_id_data, data, on='id')
        combined_data.append(data)
    combined_data = pd.concat(combined_data, axis=0)
    # fix author var?? some authors are int values
    combined_data = combined_data.assign(**{
        author_var : combined_data.loc[:, author_var].apply(lambda x: str(x))
    })
#     print(combined_data.head())
    
    ## extract language and score
    # store as sparse matrix
    N_author = combined_data.loc[:, author_var].nunique()
    author_lang_counts = []
    score_cutoff = 0.9
    lang_var = 'lang'
    lang_score_var = 'lang_score'
    for author_i, data_i in combined_data.groupby(author_var):
        # restrict to high score
        data_i = data_i[data_i.loc[:, lang_score_var] >= score_cutoff]
        if(data_i.shape[0] > 0):
            lang_counts_i = data_i.loc[:, lang_var].value_counts().reset_index().rename(columns={'index' : lang_var, lang_var : 'lang_count'}, inplace=False)
            lang_counts_i = lang_counts_i.assign(**{
                author_var : author_i
            })
            author_lang_counts.append(lang_counts_i)
    author_lang_counts = pd.concat(author_lang_counts, axis=0)
            
    # old code: classify lang on the fly
#     # parallel
#     NUM_PROC = 10
#     # tmp debugging
# #     combined_data = combined_data.head(2500)
#     with Pool(processes=NUM_PROC) as pool:
#         data_iter = map(lambda x: x[1], combined_data.groupby(author_var))
#         pool_arg_iter = zip(data_iter, (score_cutoff,)*N_author)
#         author_lang_counts = pool.starmap(extract_lang_counts, pool_arg_iter)
#         logging.info('after pool map, we have %d output'%(len(author_lang_counts)))
#         pool.terminate()
    # convert to sparse matrix
    N_author = author_lang_counts.loc[:, author_var].nunique()
    N_lang = author_lang_counts.loc[:, 'lang'].nunique()
    author_idx_lookup = dict(zip(author_lang_counts.loc[:, author_var].unique(), range(N_author)))
    lang_idx_lookup = dict(zip(author_lang_counts.loc[:, 'lang'].unique(), range(N_lang)))
    author_lang_mat = dok_matrix((N_author, N_lang), dtype=float)
    for author_i, data_i in author_lang_counts.groupby(author_var):
        idx_i = author_idx_lookup[author_i] 
        for lang_j, count_j in data_i.loc[:, ['lang', 'lang_count']].values:
            idx_j = lang_idx_lookup[lang_j]
            author_lang_mat[idx_i, idx_j] = count_j
    # compute ES percent 
    major_lang = 'es'
    major_lang_idx = lang_idx_lookup[major_lang]
    major_lang_pcts = pd.DataFrame(
        [np.array(author_lang_mat[:, major_lang_idx] / author_lang_mat.sum(axis=1))[:, 0], 
         list(sorted(author_idx_lookup.keys(), key=author_idx_lookup.get))],
        index=[f'{major_lang}', author_var]
    ).transpose()
    
    ## write to file
    # indices and data
    out_dir = args['out_dir']
    author_idx_file = os.path.join(out_dir, 'author_lang_counts.npz.rows')
    lang_idx_file = os.path.join(out_dir, 'author_lang_counts.npz.cols')
    lang_out_file = os.path.join(out_dir, 'author_lang_counts.npz')
    author_lang_mat = author_lang_mat.tocsr()
    save_npz(lang_out_file, author_lang_mat)
    with open(author_idx_file, 'w') as author_idx_out:
        author_idx_out.write('\n'.join(author_idx_lookup.keys()))
    with open(lang_idx_file, 'w') as lang_idx_out:
        lang_idx_out.write('\n'.join(lang_idx_lookup.keys()))
    # also write ES percent for later data combining
    data_dir_base = os.path.basename(os.path.normpath(data_dirs[0]))
    major_lang_out_file = os.path.join(out_dir, f'{data_dir_base}_LANG={major_lang}_pct.tsv')
    major_lang_pcts.to_csv(major_lang_out_file, sep='\t', index=False)
    
if __name__ == '__main__':
    main()