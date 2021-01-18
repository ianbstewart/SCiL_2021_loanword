"""
Extract media sharing among authors.

Ex. how often did author A share a Spotify link? Which artist IDs did they share? What is the genre of these artists?

We have already extracted extra data about Spotify tracks/artists in extract_spotify_musicians_from_posts.py.
"""
from argparse import ArgumentParser
import logging
from data_helpers import load_data_from_dirs, load_artist_names, clean_date_values, try_literal_eval, extract_URL_matches
import os
import re
from functools import reduce
import pandas as pd
from ast import literal_eval

def get_match(txt, matcher):
    match = matcher.search(txt)
    if(match is not None):
        match_txt = match.group(0)
    else:
        match_txt = ''
    return match_txt

def main():
    parser = ArgumentParser()
    parser.add_argument('data_dirs', nargs='+')
    parser.add_argument('--out_dir', default='../../data/mined_tweets/')
    parser.add_argument('--media_id_data', default='../../data/culture_metadata/spotify_track_data.tsv')
    parser.add_argument('--media_artist_data', default='../../data/culture_metadata/spotify_musician_data.tsv')
    args = vars(parser.parse_args())
    logging_file = '../../output/extract_music_sharing_for_authors.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    ## load author data
    data_dirs = args['data_dirs']
    file_matcher = re.compile('.+tweets\.gz')
    use_cols = ['urls', 'screen_name', 'created_at', 'date']
    combined_data = load_data_from_dirs(data_dirs, file_matcher=file_matcher, compression='gzip', use_cols=use_cols)
    combined_data.fillna('', inplace=True)
    # fix date variable
    combined_data = clean_date_values(combined_data)
    logging.info(combined_data.head())
    
    ## extract media links
    combined_data = combined_data[combined_data.loc[:, 'urls'] != '']
    media_matcher = re.compile(r'(?<=spotify\.com/track/)[a-zA-Z0-9]+')
    combined_data = combined_data.assign(**{
        'media_link_ids' : extract_URL_matches(combined_data, media_matcher)
    })
    
    # get links per author
    author_var = 'screen_name'
    media_link_var = 'media_link_ids'
    date_var = 'clean_date'
    combined_data = combined_data[combined_data.loc[:, author_var] != '']
    author_links = combined_data.groupby(author_var).apply(lambda x: list(reduce(lambda y,z: y+z, x.loc[:, media_link_var].values)))    
    valid_author_links = author_links[author_links.apply(len) > 0]
    logging.info('%d/%d authors with at least one media link'%(len(valid_author_links), len(author_links)))
    logging.info(valid_author_links.head())
    # flatten
    media_id_var = 'track_id'
    flat_author_link_data = []
    valid_author_combined_data = combined_data[combined_data.loc[:, author_var].isin(valid_author_links.index)]
    for idx, data_i in valid_author_combined_data.iterrows():
        author_i = data_i.loc[author_var]
        date_i = data_i.loc[date_var]
        links_i = data_i.loc[media_link_var]
        for link_j in links_i:
            flat_author_link_data.append([author_i, link_j, date_i])
    valid_author_link_data = pd.DataFrame(flat_author_link_data, columns=[author_var, media_id_var, date_var])
    # extract IDs from links
    valid_author_link_data = valid_author_link_data.assign(**{
        media_id_var : valid_author_link_data.loc[:, media_id_var].apply(lambda x: media_matcher.search(x).group(0))
    })
    logging.info('flat author data')
    logging.info(valid_author_link_data.head())
    
    ## match media ID => artist ID
    media_id_data_file = args['media_id_data']
    media_artist_data_file = args['media_artist_data']
    media_id_data = pd.read_csv(media_id_data_file, sep='\t', index_col=False, converters={'artist_name' : try_literal_eval})
    media_artist_data = pd.read_csv(media_artist_data_file, sep='\t', index_col=False, converters={'genres' : literal_eval})
    media_id_data.fillna('', inplace=True)
    # fix artist names
    media_artist_var = 'artist_id'
    media_id_data = media_id_data[media_id_data.loc[:, media_artist_var] != '']
    media_id_data = media_id_data.assign(**{
        media_artist_var : media_id_data.loc[:, media_artist_var].apply(literal_eval)
    })
    logging.info('loaded media data')
    logging.info(media_id_data.head())
    # add duplicate rows for all musicians on each track
    flat_media_id_data = []
    dup_var = 'artist_id'
    for idx_i, row_i in media_id_data.iterrows():
        for dup_val in row_i.loc[dup_var]:
            row_i.loc[dup_var] = dup_val
            flat_media_id_data.append(row_i)
    flat_media_id_data = pd.concat(flat_media_id_data, axis=1).transpose()
    flat_media_id_data.rename(columns={'id' : media_id_var}, inplace=True)
    logging.info('flat media data has cols %s'%(list(sorted(flat_media_id_data.columns))))
    logging.info(flat_media_id_data.head())
    media_data = pd.merge(flat_media_id_data.loc[:, ['artist_name', media_id_var, media_artist_var]], media_artist_data, on=media_artist_var)
    logging.info('merged data')
    logging.info(media_data.head())
    
    ## assign artists to category based on known artist names
    latin_american_artist_names = load_artist_names(artist_category='latin_american')
    us_american_artist_names = load_artist_names(artist_category='us_american')
    # remove overlap
    artist_name_overlap = set(latin_american_artist_names) & set(us_american_artist_names)
    latin_american_artist_names = set(latin_american_artist_names) - set(artist_name_overlap)
    us_american_artist_names = set(us_american_artist_names) - set(artist_name_overlap)
    logging.info('US American artist N=%d; sample %s'%(len(us_american_artist_names), str(list(us_american_artist_names)[:10])))
    logging.info('Latin American artist N=%d; sample %s'%(len(latin_american_artist_names), str(list(latin_american_artist_names)[:10])))
    # we don't need to filter names like we did with YouTube
    # for min word len or char len
    # because we know that the Spotify artist names refer only to musicians
    artist_category_names = ['us_american', 'latin_american']
    artist_category_name_lists = [us_american_artist_names, latin_american_artist_names]
    for artist_category_name, artist_category_name_list in zip(artist_category_names, artist_category_name_lists):
        media_data = media_data.assign(**{
            f'{artist_category_name}_artist' : media_data.loc[:, 'artist_name'].apply(lambda x: len(set(x) & artist_category_name_list) > 0)
        })
    logging.info('%d Latin American artist music'%(media_data.loc[:, 'latin_american_artist'].sum()))
    logging.info('%d US American artist music'%(media_data.loc[:, 'us_american_artist'].sum()))
    
    ## assign artists to category based on genre
    # based on analysis of known US and Latin American artists here: http://localhost:8894/notebooks/scripts/data_processing/test_musician_sharing_in_loanword_authors.ipynb
    us_american_genres = set(['acid rock', 'acoustic pop', 'adult standards', 'afrofuturism', 'album rock', 'alternative country', 'alternative hip hop', 'alternative metal', 'alternative r&b', 'alternative rock', 'anti-folk', 'art pop', 'atl hip hop', 'atl trap', 'bachata', 'banda', 'baton rouge rap', 'bebop', 'blues', 'blues rock', 'boy band', 'bronx hip hop', 'canadian pop', 'canadian singer-songwriter', 'chamber pop', 'chamber psych', 'chicago drill', 'chicago rap', 'chillwave', 'classic rock', 'classic soul', 'classic uk pop', 'comedy rap', 'comic', 'conscious hip hop', 'contemporary vocal jazz', 'cool jazz', 'country rock', 'crunk', 'dance pop', 'deep pop r&b', 'deep southern trap', 'detroit hip hop', 'dfw rap', 'dirty south rap', 'disco', 'dmv rap', 'dream pop', 'drill', 'east coast hip hop', 'easy listening', 'edm', 'electric blues', 'electropop', 'escape room', 'estonian hip hop', 'etherpop', 'europop', 'experimental hip hop', 'experimental pop', 'florida death metal', 'florida rap', 'folk', 'folk rock', 'freak folk', 'funk', 'funk rock', 'g funk', 'gangster rap', 'garage rock', 'girl group', 'glam metal', 'glam rock', 'glitch', 'glitch hop', 'grunge', 'hard rock', 'hardcore hip hop', 'harlem hip hop', 'hawaiian hip hop', 'heartland rock', 'hip hop', 'hip pop', 'houston rap', 'hyperpop', 'indie folk', 'indie pop', 'indie poptimism', 'indie rock', 'indie soul', 'indietronica', 'industrial', 'industrial metal', 'industrial rock', 'instrumental rock', 'intelligent dance music', 'jazz', 'jazz funk', 'jazz guitar', 'jazz trumpet', 'jazztronica', 'la pop', 'lgbtq+ hip hop', 'lilith', 'lounge', 'melancholia', 'mellow gold', 'melodic rap', 'memphis hip hop', 'memphis soul', 'miami hip hop', 'minneapolis sound', 'minnesota hip hop', 'modern blues', 'modern rock', 'motown', 'nc hip hop', 'neo mellow', 'neo r&b', 'neo soul', 'new americana', 'new jack swing', 'new jersey indie', 'new orleans rap', 'new wave pop', 'norteno', 'nu metal', 'nyc pop', 'nyc rap', 'omaha indie', 'outlaw country', 'permanent wave', 'philly rap', 'pittsburgh rap', 'pop', 'pop rap', 'pop rock', 'post-grunge', 'post-teen pop', 'protopunk', 'psychedelic rock', 'queens hip hop', 'quiet storm', 'r&b', 'rap', 'rap latina', 'rap rock', 'reggaeton flow', 'regional mexican', 'rock', 'roots rock', 'shimmer pop', 'shiver pop', 'singer-songwriter', 'smooth jazz', 'soft rock', 'soul', 'soul blues', 'soul flow', 'soul jazz', 'southern hip hop', 'southern soul', 'stomp and holler', 'teen pop', 'tejano', 'texas blues', 'texas country', 'traditional folk', 'trap', 'trap soul', 'tropical house', 'uk pop', 'underground hip hop', 'underground rap', 'urban contemporary', 'vapor trap', 'viral pop', 'vocal jazz', 'west coast rap', 'west coast trap', 'wonky', 'wrestling', 'yacht rock'])
    latin_american_genres = set(['art rock', 'baile pop', 'bolero', 'bossa nova', 'brazilian indie', 'brazilian rock', 'brega', 'cancion melodica', 'cantautor', 'champeta', 'chilean indie', 'chilean rock', 'colombian indie', 'colombian pop', 'cumbia', 'dance rock', 'dancehall', 'ecuadorian pop', 'electronica', 'forro', 'funk carioca', 'funk das antigas', 'grupera', 'jovem guarda', 'latin', 'latin afrobeat', 'latin alternative', 'latin arena pop', 'latin christian', 'latin jazz', 'latin pop', 'latin rock', 'latin viral pop', 'latin worship', 'mariachi', 'mariachi cristiano', 'metal', 'mexican pop', 'mexican rock', 'modern reggae', 'mpb', 'new romantic', 'new wave', 'nueva cancion', 'nueva trova chilena', 'pagode', 'peruvian rock', 'pop chileno', 'pop peruano', 'puerto rican pop', 'ranchera', 'reggae', 'reggae en espanol', 'reggae fusion', 'reggaeton', 'reggaeton chileno', 'rock cristiano', 'rock en espanol', 'roots reggae', 'salsa', 'samba', 'spanish pop', 'tipico', 'trap chileno', 'tropical', 'trova', 'vallenato', 'velha guarda', 'veracruz indie'])
    # TODO: classify songs based on musicians' Wikipedia categories
    genre_list_var = 'genres'
    genre_list = [us_american_genres, latin_american_genres]
    genre_category_names = ['us_american', 'latin_american']
    # only count a song as US American, Latin American, etc. if it excludes other genres
    for i, (genre_category_name_i, genres_i) in enumerate(zip(genre_category_names, genre_list)):
        non_genres_i = set(reduce(lambda x,y: x|y, genre_list[:i] + genre_list[i+1:]))
        media_data = media_data.assign(**{
            f'{genre_category_name_i}_genre' : media_data.loc[:, genre_list_var].apply(lambda x: len(set(x) & genres_i) > 0 and len(set(x) & non_genres_i) == 0)
        })
#     genre_cutoff = 0.5
#     media_data = media_data.assign(**{
#         'us_american_genre' : media_data.loc[:, genre_list_var].apply(lambda x: len((set(x) - latin_american_genres) & us_american_genres) / len((set(x) - latin_american_genres)) > genre_cutoff),
#         'latin_american_genre' : media_data.loc[:, genre_list_var].apply(lambda x: len((set(x) - us_american_genres) & set(latin_american_genres)) / len(set(x) - us_american_genres) > genre_cutoff),
#     })

    ## combine artist + genre => genre
    for category_name in genre_category_names:
        media_data = media_data.assign(**{
            f'{category_name}_genre' : media_data.loc[:, [f'{category_name}_genre', f'{category_name}_artist']].max(axis=1)
        })
    
    logging.info('assigned music genres')
    logging.info('%d/%d Latin American artists'%(media_data.loc[:, 'latin_american_genre'].sum(), media_data.shape[0]))
    logging.info('%d/%d US American artists'%(media_data.loc[:, 'us_american_genre'].sum(), media_data.shape[0]))
    genre_category_vars = ['latin_american_genre', 'us_american_genre']
    logging.info(media_data.loc[:, genre_category_vars + [media_id_var, media_artist_var]].head())
    
    ## get genre counts per author
    genre_category_vars = ['latin_american_genre', 'us_american_genre']
    author_media_data = pd.merge(media_data.loc[:, genre_category_vars + [media_id_var, media_artist_var]], valid_author_link_data, on=media_id_var)
    logging.info('merged media genre data, author link data')
    logging.info(author_media_data.head())
    # reshape data: author | us american count | latin american count | total count
    author_media_counts = []
    for genre_category_var in genre_category_vars:
        genre_counts = author_media_data.groupby(author_var).apply(lambda x: x.loc[:, genre_category_var].sum()).reset_index().rename(columns={0 : f'{genre_category_var}_count'})
        if(len(author_media_counts) == 0):
            author_media_counts = genre_counts.copy()
        else:
            author_media_counts = pd.merge(author_media_counts, genre_counts, on=author_var)
    # add total counts
    author_total_media_counts = author_media_data.loc[:, author_var].value_counts().reset_index().rename(columns={'index' : author_var, author_var : 'total_link_count'})
    author_media_counts = pd.merge(author_total_media_counts, author_media_counts, on=author_var, how='right')
    logging.info('collected aggregate music counts')
    logging.info(author_media_counts.head())
    
    ## save full data and artist count data
    out_dir = args['out_dir']
    data_dir_base = os.path.basename(os.path.normpath(data_dirs[0]))
    author_media_out_file = os.path.join(out_dir, f'{data_dir_base}_author_spotify_link_data.tsv')
    author_media_counts_out_file = os.path.join(out_dir, f'{data_dir_base}_author_spotify_counts.tsv')
    author_media_data.to_csv(author_media_out_file, sep='\t', index=False)
    author_media_counts.to_csv(author_media_counts_out_file, sep='\t', index=False)
    
if __name__ == '__main__':
    main()