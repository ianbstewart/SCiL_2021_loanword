# Data collection

This project requires data about:

1. Loanword use 
2. Music artists (for dissertation; not for SCiL study)

I will explain each component here.

# Loanword use

To collect loanword author data, we do the following:

1. Identify a data source (Twitter, Reddit, etc.).
    1. Assuming that data has already been downloaded, convert data to Elastic Search for easier querying.
        - `index_ES_posts.sh`
    2. Validate data: sample data by loanword phrases for native language annotators.
        - `sample_post_txt_by_phrase.sh`
        - `clean_sample_integrated_verb_data_for_annotation.sh`
2. Match all authors who use a loanword at least once from this data.
    1. Run a query on the Elastic Search instance.
        - `collect_loanword_authors.sh`
3. Extract prior posts from all authors.
    1. For Twitter: collect the prior ~1000 posts from historical timeline data via API.
        - `collect_tweets_from_loanword_authors.sh`
    2. Collect all prior posts available in Elastic Search instances.
        - `collect_tweets_from_loanword_authors_in_elasticsearch.sh`
4. Extract social background data for all authors.
    0. Extract relevant descriptive information.
        - `combine_descriptive_info_for_loanword_authors.sh`
    1. If location field available in original data: extract location as country using string matching.
        - `extract_location_for_authors.sh`
    2. Tag all prior author posts for language, then compute language proportion (e.g. 10% Spanish).
        - `tag_language_all_author_posts.sh`
        - `extract_lang_use_for_authors.sh`
    3. Compute activity rate as posts per day, based on prior posts.
        - `extract_author_activity_data.sh` 
    4. Compute sharing rates as URLs, shares per post, based on prior posts.
        - `extract_author_activity_data.sh` 
    5. Extract all media links, based on prior posts.
        - `extract_spotify_musicians_from_posts.sh`
        - `extract_youtube_video_data_from_posts.sh`
        - `extract_youtube_video_genre_from_posts.sh`
        - `extract_music_sharing_for_authors.sh`
        - `combine_balance_author_media_sharing_data.sh` 
5. Organize data into regression format, i.e. predicting light verb vs. integrated verb from social factors.
    - `combine_post_author_data_for_regression.sh`
6. For newspaper comparison:
    1. Organize loanword and native verbs for query. Then send queries to query-writer.
        - `organize_verb_forms_for_corpus_query.sh` 

# Music artists

To collect data for musical artists, we do the following:

1. Define categories of musicians of interest via structured database like DBPedia.
2. Collect musicians that fit into these categories.
    - `collect_musicians_from_dbpedia.sh`
    - `collect_pages_from_wikipedia_by_category.sh`
3. (Optional) Expand categories with suggested musicians from Spotify.
    - `collect_similar_artists_from_spotify.sh`
4. Collect media links from prior author posts.
    - See "Extract all media links" above.
5. Extract metadata from media links, using Spotify/YouTube APIs.
    - `extract_spotify_musicians_from_posts.sh`
    - `extract_youtube_video_data_from_posts.sh`
6. Match each link to musician category, based on artist name, tag, or genre provided by media metadata.
    - `extract_music_sharing_for_authors.sh`
7. Extract age distribution for musicians based on FB interest.
    1. Extract FB interest IDs for musicians.
        - `get_facebook_interest_ids.sh`
    2. Get age distributions for interests.
        - `mine_FB_age_distribution_for_interests.sh`

# Exploratory analysis

- Expand light verbs to be used with loanwords.
    - `expand_loanword_light_verbs.ipynb`
- Get long-term loanword counts from Google N-grams.
    - `get_counts_from_google_ngrams.ipynb`
    - `plot_loanword_integration_over_time.ipynb`
- Get summary statistics for loanword data.
    - `get_summary_statistics.ipynb`
- Explore distributions of social variables among loanword authors.
    - `explore_demographics_in_loanword_authors.ipynb`
- Compare social media vs. newspaper integration.
    - `compare_loanword_integration_with_standard_corpus.ipynb`
- Test methods for balancing age distribution among musicians.
    - `balance_musicians_age_distribution.ipynb`
- Test collinearity among author variables (e.g. language x location).
    - `test_social_variable_collinearity.ipynb`
- Compare behavior across different author sub-groups.
    - `explore_high_integration_authors.ipynb`
- Test alternate measures of formality.
    - `test_formality_among_integrated_verbs.ipynb`

# Helpers

- Data processing helpers:
    - `data_helpers.py`
