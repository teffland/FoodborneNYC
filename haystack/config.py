""" Global Haystack Configurations """
config = {
    'logging_dir':'/Users/thomaseffland/haystack_data/log' # directory to save log files to
}

yelp_download_config = {
    # the dir to download the yelp data from S3 to
    'data_dir': 'haystack/sources/yelpfiles/', 
    # the dir to save bulk import csvs to
    # NOTE that this MUST be the import dir for neo4j
    'neo_data_dir': '/Users/thomaseffland/neo4j_data/import', 
    'feed_file':  'yelp_businesses.json.gz',
    'bucket_name': 'yelp-syndication',
    'bucket_dir':  'nychealth'
}