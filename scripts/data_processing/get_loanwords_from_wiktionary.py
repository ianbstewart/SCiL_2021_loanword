#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Get additional loanwords from Wiktionary,
using "derived from English" as a marker for loanword.
"""
from argparse import ArgumentParser
import logging
import os
from bs4 import BeautifulSoup
from urllib import request
import re

def has_word(tag):
    return tag.name == 'a' and tag.has_attr('title') and tag['title'] == tag.string

def collect_words(base_url, next_page_str, category_url):
    next_page_url = base_url + category_url
    es_verb_matcher = re.compile('ar$') # ear => higher precision
    next_page = True
    words = []
    while(next_page):
        logging.debug('about to load next page: %s'%(next_page_url))
        next_page_html = request.urlopen(next_page_url).read()
        wiki_soup = BeautifulSoup(next_page_html, features="lxml")
        word_tag_list = wiki_soup.find_all(has_word)
        # filter for verbs
        verb_tag_list = list(filter(lambda x: es_verb_matcher.search(x.string) is not None, word_tag_list))
        verb_list = list(map(lambda x: x.string, verb_tag_list))
        words += verb_list
        next_page_tags = wiki_soup.find_all("a", string=next_page_str)
        next_page = len(next_page_tags) > 0
        if(next_page):
            next_page_tag = next_page_tags[0]
            next_page_url_args = next_page_tag['href']
            next_page_url = base_url + next_page_url_args
    # TODO: filter for actual verbs!! based on dict definition
    
    return words

def check_for_verb_es(word_url):
    # TODO: check that word is verb, on ES Wiki
    pass

def main():
    parser = ArgumentParser()
    parser.add_argument('--out_dir', default='../../data/loanword_resources/')
    args = vars(parser.parse_args())
    logging_file = '../../output/get_loanwords_from_wiktionary.txt'
    if(os.path.exists(logging_file)):
        os.remove(logging_file)
    logging.basicConfig(filename=logging_file, level=logging.DEBUG)

    ## collect words
    # ES Wiki
    es_wiki_base_url = 'https://es.wiktionary.org'
    es_next_page_str = 'página siguiente'
    es_wiki_category_url = '/w/index.php?title=Categor%C3%ADa:ES:Palabras_de_origen_ingl%C3%A9s'
    es_verb_list = collect_words(es_wiki_base_url, es_next_page_str, es_wiki_category_url)
    print('ES verbs %s'%(','.join(es_verb_list)))
    # EN Wiki
    en_wiki_base_url = 'https://en.wiktionary.org'
    en_next_page_str = 'next page'
    en_wiki_category_url = '/w/index.php?title=Category:Spanish_terms_derived_from_English'
    en_verb_list = collect_words(en_wiki_base_url, en_next_page_str, en_wiki_category_url)
    print('EN verbs %s'%(','.join(en_verb_list)))
    # combined list
    es_verbs_combined = sorted(set(es_verb_list) | set(en_verb_list))

    ## write to file
    out_file_name = os.path.join(args['out_dir'], 'es_wiktionary_loanword_verb_candidates.txt')
    with open(out_file_name, 'w') as out_file:
        out_file.write('\n'.join(es_verbs_combined))
    
if __name__ == '__main__':
    main()