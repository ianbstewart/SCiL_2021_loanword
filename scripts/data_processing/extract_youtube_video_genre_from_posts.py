"""
Extract author counts for video genre, i.e.
did this video contain music by a Latin
American or US American artist?
"""
from argparse import ArgumentParser
from data_helpers import load_data_from_dirs, load_data_manual, clean_txt_simple, clean_date_values
from ast import literal_eval
import logging
import os
import re
import pandas as pd

def assign_category(x, categories, category_sets):
    for category, category_set in zip(categories, category_sets):
        if(x in category_set):
            return category
    return ''

def try_literal_eval(x):
    x_lit = []
    try:
        x_lit = literal_eval(x)
    except Exception as e:
        pass
    return x_lit

def main():
    parser = ArgumentParser()
    parser.add_argument('data_dir')
    parser.add_argument('--youtube_data', default='../../data/culture_metadata/youtube_video_data.tsv')
    parser.add_argument('--latin_american_artist_data', nargs='+', default=['../../data/culture_metadata/latin_american_musician_subcategory_dbpedia_data.tsv', '../../data/culture_metadata/latin_american_pop_musicians_en_wiki_data.tsv', '../../data/culture_metadata/latin_american_pop_musicians_es_wiki_data.tsv'])
    parser.add_argument('--us_american_artist_data', nargs='+', default=['../../data/culture_metadata/us_american_musician_subcategory_dbpedia_data.tsv', '../../data/culture_metadata/us_american_pop_musicians_en_wiki_data.tsv'])
    parser.add_argument('--out_dir', default='../../data/mined_tweets/')
    args = vars(parser.parse_args())
    logging_file = '../../output/extract_youtube_video_genre_from_posts.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    ## load data
    # prior post data
    data_dir = args['data_dir']
    file_matcher = re.compile('.*tweets\.gz')
    loanword_author_data = load_data_from_dirs([data_dir], file_matcher=file_matcher)
    loanword_author_data.fillna('', inplace=True)
    # fix names
    author_var = 'screen_name'
    loanword_author_data = loanword_author_data.assign(**{
        author_var : loanword_author_data.loc[:, author_var].apply(lambda x: str(x).lower())
    })
    # fix dates
    loanword_author_data = clean_date_values(loanword_author_data)
    print('clean dates:\n %s'%(loanword_author_data.loc[:, 'clean_date'].iloc[:10]))
    
    ## extract media data from posts
    url_var = 'urls'
    media_loanword_author_data = loanword_author_data[loanword_author_data.loc[:, url_var] != '']
    # extract YouTube ID
    youtube_media_matcher = re.compile('(?<=youtube\.com/watch\?v=)[a-zA-Z0-9_\-]+|(?<=youtu\.be/)[a-zA-Z0-9_\-]+')
    media_loanword_author_data = media_loanword_author_data.assign(**{
        'youtube_id' : media_loanword_author_data.loc[:, 'urls'].apply(lambda x: youtube_media_matcher.search(x).group(0) if youtube_media_matcher.search(x) is not None else '')
    })
    logging.info('%d authors with at least one YouTube link'%(media_loanword_author_data[media_loanword_author_data.loc[:, 'youtube_id'] != ''].loc[:, author_var].nunique()))
    # youtube video data
    youtube_data_file = args['youtube_data']
    # why does this drop so many columns??
    # because the file was badly formatted RIP
#     youtube_video_data = load_data_manual(youtube_data_file)
    youtube_video_data = load_data_manual(youtube_data_file, max_cols=11, verbose=False, pad_val='')
    logging.info('loaded %d YouTube video data'%(youtube_video_data.shape[0]))
    # clean bad data
    youtube_video_data.fillna('', inplace=True)
    # convert list vars
    list_vars = ['topicCategories', 'tags']
    youtube_video_data = youtube_video_data.assign(**{
        v : youtube_video_data.loc[:, v].apply(lambda x: try_literal_eval(x))
        for v in list_vars
    })
    youtube_video_data = youtube_video_data.assign(**{
        'clean_topic_categories' : youtube_video_data.loc[:, 'topicCategories'].apply(lambda x: list(map(lambda y: y.split('/')[-1].lower(), x)))
    })
    # clean tags
    youtube_video_data = youtube_video_data.assign(**{
        'clean_tags' : youtube_video_data.loc[:, 'tags'].apply(lambda x: list(map(clean_txt_simple, x)))
    })
    # artist data
    latin_american_artist_data_files = args['latin_american_artist_data']
    us_american_artist_data_files = args['us_american_artist_data']
    latin_american_artist_data = pd.concat(list(map(lambda x: pd.read_csv(x, sep='\t'), latin_american_artist_data_files)), axis=0)
    us_american_artist_data = pd.concat(list(map(lambda x: pd.read_csv(x, sep='\t'), us_american_artist_data_files)), axis=0)
    latin_american_artist_names = set(latin_american_artist_data.loc[:, 'name'].unique())
    us_american_artist_names = set(us_american_artist_data.loc[:, 'name'].unique())
    # remove intersection
    latin_american_us_american_artists = latin_american_artist_names & us_american_artist_names
    latin_american_artist_names = latin_american_artist_names - latin_american_us_american_artists
    us_american_artist_names = us_american_artist_names - latin_american_us_american_artists
    # fix names
    latin_american_artist_names = set(map(clean_txt_simple, latin_american_artist_names))
    us_american_artist_names = set(map(clean_txt_simple, us_american_artist_names))
    # filter by name size: two words or more
    # too many false positives from e.g. "evolucion" "love" "television" "mario"
#     name_min_char_len = 4
    clean_latin_american_artist_names = set(filter(lambda x: len(x.split(' ')) > 1, latin_american_artist_names))
    clean_us_american_artist_names = set(filter(lambda x: len(x.split(' ')) > 1, us_american_artist_names))
    print('%d clean Latin American artist names: %s'%(len(clean_latin_american_artist_names), str(list(clean_latin_american_artist_names)[:10])))
    
    ## label video data as "Latin American" or "US American" based on:
    # 1. title contains known artist name
    # 2. content tags contain known artist name
    # 3. content tags contain genre associated with group of artists (e.g. "music_of_latin_america")
    # title extraction
    artist_title_matcher = re.compile('^[a-z0-9\s]+(?= \- )|[a-z0-9\s]+(?= | )')
    youtube_video_data = youtube_video_data.assign(**{
        'title_artist' : youtube_video_data.loc[:, 'title'].apply(clean_txt_simple).apply(lambda x: artist_title_matcher.search(x).group(0) if artist_title_matcher.search(x) is not None else '')
    })
    title_artist_var = 'title_artist'
    youtube_video_data = youtube_video_data.assign(**{
        'clean_title' : youtube_video_data.loc[:, 'title'].apply(clean_txt_simple)
    })
    youtube_video_data = youtube_video_data.assign(**{
        'has_latin_american_title_artist' : youtube_video_data.loc[:, 'title_artist'].apply(lambda x: x in latin_american_artist_names),
        'has_us_american_title_artist' : youtube_video_data.loc[:, 'title_artist'].apply(lambda x: x in us_american_artist_names),
    })
    # artist tag
    youtube_video_data = youtube_video_data.assign(**{
        'latin_american_artist_tags' : youtube_video_data.loc[:, 'clean_tags'].apply(lambda x: set(x) & clean_latin_american_artist_names),
        'us_american_artist_tags' : youtube_video_data.loc[:, 'clean_tags'].apply(lambda x: set(x) & clean_us_american_artist_names),
    })
    youtube_video_data = youtube_video_data.assign(**{
        'has_latin_american_artist_tag' : youtube_video_data.loc[:, 'latin_american_artist_tags'].apply(lambda x: len(x) > 0),
        'has_us_american_artist_tag' : youtube_video_data.loc[:, 'us_american_artist_tags'].apply(lambda x: len(x) > 0),
    })
    # music genre
    latin_american_music_tag_categories = set(['music_of_latin_america'])
    us_american_music_tag_categories = set(['rock_music', 'electronic_music', 'soul_music', 'country_music', 'christian_music'])
    youtube_video_data = youtube_video_data.assign(**{
        'has_latin_american_music_genre_tag' : youtube_video_data.loc[:, 'clean_topic_categories'].apply(lambda x: len(set(x) & latin_american_music_tag_categories) > 0),
        'has_us_american_music_genre_tag' : youtube_video_data.loc[:, 'clean_topic_categories'].apply(lambda x: len(set(x) & us_american_music_tag_categories) > 0),
    })
    # identify matching IDs
    latin_american_music_video_data = youtube_video_data[(youtube_video_data.loc[:, 'has_latin_american_title_artist']) | 
                                                    (youtube_video_data.loc[:, 'has_latin_american_artist_tag']) | 
                                                    (youtube_video_data.loc[:, 'has_latin_american_music_genre_tag'])]
    us_american_music_video_data = youtube_video_data[(youtube_video_data.loc[:, 'has_us_american_title_artist']) | 
                                                     (youtube_video_data.loc[:, 'has_us_american_artist_tag']) | 
                                                     (youtube_video_data.loc[:, 'has_us_american_music_genre_tag'])]
    # remove overlap
    id_var = 'id'
    latin_american_us_american_video_overlap = set(latin_american_music_video_data.loc[:, id_var].unique()) & set(us_american_music_video_data.loc[:, id_var].unique())
    latin_american_music_video_data = latin_american_music_video_data[~latin_american_music_video_data.loc[:, id_var].isin(latin_american_us_american_video_overlap)]
    us_american_music_video_data = us_american_music_video_data[~us_american_music_video_data.loc[:, id_var].isin(latin_american_us_american_video_overlap)]
    latin_american_music_video_ids = latin_american_music_video_data.loc[:, id_var].unique()
    us_american_music_video_ids = us_american_music_video_data.loc[:, id_var].unique()
    # save full data in case of need later
    latin_american_music_video_data = latin_american_music_video_data.assign(**{
        'video_genre' : 'latin_american_music'
    })
    us_american_music_video_data = us_american_music_video_data.assign(**{
        'video_genre' : 'us_american_music'
    })
    genre_youtube_video_data = pd.concat([latin_american_music_video_data, us_american_music_video_data], axis=0)
    genre_youtube_video_data.rename(columns={'id' : 'youtube_id'}, inplace=True)
    artist_title_vars = ['has_latin_american_title_artist', 'has_us_american_title_artist']
    tag_category_vars = ['latin_american_artist_tags', 'us_american_artist_tags', 'has_latin_american_artist_tag', 'has_us_american_artist_tag', 'has_latin_american_music_genre_tag', 'has_us_american_music_genre_tag']
    genre_video_vars = ['youtube_id', 'title_artist', 'clean_title', 'clean_tags', 'clean_topic_categories', 'video_genre'] + artist_title_vars + tag_category_vars
    genre_video_data = genre_youtube_video_data.loc[:, genre_video_vars]
    genre_out_dir = os.path.dirname(youtube_data_file)
    genre_out_file = os.path.join(genre_out_dir, 'youtube_video_music_genre_data.tsv')
    genre_video_data.to_csv(genre_out_file, sep='\t', index=False)
    
    ## save data
    # post ID | video ID | category
    youtube_media_loanword_author_data = media_loanword_author_data[media_loanword_author_data.loc[:, 'youtube_id'] != '']
    video_category_names = ['latin_american_artist', 'us_american_artist']
    video_category_sets = [latin_american_music_video_ids, us_american_music_video_ids]
    video_category_var = 'youtube_id_category'
    video_id_var = 'youtube_id'
    youtube_media_loanword_author_data = youtube_media_loanword_author_data.assign(**{
        video_category_var : youtube_media_loanword_author_data.loc[:, video_id_var].apply(lambda x: assign_category(x, video_category_names, video_category_sets))
    })
    post_id_var = 'id'
    date_var = 'clean_date'
    print('clean dates:\n%s'%(youtube_media_loanword_author_data.head()))
    youtube_media_loanword_author_data = youtube_media_loanword_author_data.loc[:, [post_id_var, video_id_var, date_var, video_category_var, author_var]]
    clean_youtube_media_data = youtube_media_loanword_author_data[youtube_media_loanword_author_data.loc[:, video_category_var] != '']
    out_dir = args['out_dir']
    data_dir_base = os.path.basename(os.path.normpath(data_dir))
    youtube_media_out_file = os.path.join(out_dir, f'{data_dir_base}_youtube_video_data_full.tsv')
    clean_youtube_media_out_file = os.path.join(out_dir, f'{data_dir_base}_youtube_video_categories.tsv')
    youtube_media_loanword_author_data.to_csv(youtube_media_out_file, sep='\t', index=False)
    clean_youtube_media_data.to_csv(clean_youtube_media_out_file, sep='\t', index=False)
    # author | category %
    default_video_category = 'latin_american_artist'
    per_author_media_data = youtube_media_loanword_author_data.groupby(author_var).apply(lambda x: x[x.loc[:, video_category_var]==default_video_category].shape[0] / x.shape[0]).reset_index().rename(columns={0 : f'{default_video_category}_video_pct'})
    per_author_media_data_file = os.path.join(out_dir, f'{data_dir_base}_youtube_video_category_per_author_pct.tsv')
    per_author_media_data.to_csv(per_author_media_data_file, sep='\t', index=False)
    
if __name__ == '__main__':
    main()