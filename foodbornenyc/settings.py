""
Global Settings
"""

database_config = {
    'user': 'johnnybananas',
    'password': 'placeholder_password',
    'dbhost': 'toy.db',
    'dbbackend':'sqlite'
}

yelp_download_config = {
    'rawdata_dir': 'foodbornenyc/sources/yelpfiles/', # the dir to download the yelp data from S3 to
    'local_file':  'yelp_businesses.json.gz',
    'bucket_name': 'yelp-syndication',
    'bucket_dir':  'nychealth'
}

geocode_config = {
    'wait_time': 4, # wait 4 sec until timeout
    'run_time': 240 # run for max 4 hours
}

yelp_classify_config = {
    'model_file': 'foodbornenyc/pipelines/models/yelp_sick_logreg.pkl',
    'days_back': 7,
    'verbosity': 1
}
