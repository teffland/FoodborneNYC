""" Yelp Downloader """
import botocore.session
from datetime import datetime, date, timedelta
import traceback
import requests
import time
import gzip
from json import loads
from os import path
from io import open
import csv
from collections import OrderedDict

from py2neo import Node, Relationship, Subgraph

from geopy.geocoders import Nominatim #open street map -- free :)

from haystack.data_models.sources import YelpDownloadHistory
from haystack.data_models.sources import Location, location_identifier
from haystack.data_models.sources import YelpReview, YelpBusiness, YelpCategory, YelpUser
from haystack.data_models.base import db
from haystack.config import yelp_download_config as yelp_config

from haystack.util.logger import get_logger
from haystack.util.conversion import xstr
from haystack.util.timing import sec_to_hms
logger = get_logger(__name__, level='INFO')

def update_yelp_from_feed():
    """ Entrypoint to sync the Yelp feed with the DB """
    # check for a download history
    download_history = list(YelpDownloadHistory.select(db).order_by('-_._created').limit(1))
    download_history = download_history[0] if download_history else None

    # maybe download the feed
    downloaded_fname, download_history  = download_latest_yelp_data(download_history)
    db.push(download_history)

    # maybe unzip the feed
    unzipped_fname, download_history = unzip_file(downloaded_fname, download_history)
    db.push(download_history)

    # sync the database
    # TODO (teffland): add last_download_date
    # if new we use an optimized bulk insert ETL pipeline for speed
    if True:#yelp_db_is_empty():
        yelp_feed_to_graph_csv(unzipped_fname)
        download_history = cypher_bulk_insert(download_history)
    # else we update slowly, since there shouldn't be too many updates needed
    # and we need more complicated merge behavior for upserts
    else:
        download_history = upsert_yelpfile_to_db(unzipped_fname, download_history)
    db.push(download_history)

class BadResponseError(Exception):
    pass

def yelp_db_is_empty():
    query = """
    MATCH (n)
    WHERE n:YelpReview 
       OR n:YelpBusiness
       OR n:YelpUser
       OR n:YelpCategory
    RETURN count(n)
    """
    node_count = list(db.run(query))[0][0]
    if node_count > 0:
        return True
    else:
        return False

def download_url_to_file(url, data_dir, filename):
    """Download a url to local file.

        Write to file as stream. This keeps a low memory footprint

        Args:
            url (str): the url to download from

            data_dir (str): the local directory to write the file

            filename (str): the name of the file to write

        Returns:
            None
            
    """
    response = requests.get(url, stream=True)

    # throw exception if response has issues
    if not response.ok:
        # TODO: This is poor handling, make it more explicit
        logger.warning("BAD RESPONSE")
        raise BadResponseError

    total_size = response.headers.get('content-length')
    logger.info("Yelp feed total size: %i MB", int(total_size)/(1024*1024))

    out_file = data_dir + filename
    start_time = time.time()
    # you may think: we should unzip as we download. Don't. It's really slow
    with open(out_file, 'wb') as handle:
        # response ok, do the download and give info during
        block_size = 1024*4
        count = 1
        for block in response.iter_content(block_size):
            # bookkeeping display
            percent = int(count*block_size)/float(total_size)*100
            duration = time.time() - start_time
            progress_size = int(count*block_size)/(1024*1024)
            if count % 100 == 0:
                # the only place not logging is ok: when doing carriage returns (logging can't)
                print "\r Downloading Yelp Data...%.2f%%  %i MB downloaded, %d seconds so far" %\
                      (percent, progress_size, duration),

            # write it out
            handle.write(block)
            handle.flush()
            count +=1
    print # gets rid of final carriage return
    
def download_latest_yelp_data(ydh):
    """Attempt to download the latest gzip file from the Yelp Syndication.

        Args:
            None
        
        Returns:
            local_file: the name of where the yelp feed was downloaded to

        Notes: 
            Yelp doesn't let us look at the bucket, 
            so we just try exact filenames with presigned urls for the past month
    """
    #get where to save the feed from config
    local_file = yelp_config['data_dir'] + yelp_config['feed_file']    

    # if it doesn't exist or it's old, create the new one
    if not ydh or ydh.created.date() != date.today():
        ydh = YelpDownloadHistory()
        logger.info("Creating new download history for today")

    # if we already downloaded, log and return
    if ydh.downloaded:
        logger.info("Already downloaded the Yelp feed for today...")
        return local_file, ydh

    # set up botocore client
    session = botocore.session.get_session()
    client = session.create_client('s3')

    # try to download most recent data (go up to one month back)
    for day_delta in range(31):
        # generate the correct filename for a day
        dateformat = '%Y%m%d'
        day = date.today() - timedelta(day_delta)
        day_str = day.strftime(dateformat) # eg '20151008'
        ext = '_businesses.json.gz'
        filename =  day_str + ext
        logger.info("Attempting to get Yelp Reviews from %s.....", day.strftime("%m/%d/%Y"))

        # generate a presigned url for the file, 
        # since yelp doesn't give us bucket access
        url = client.generate_presigned_url(
                'get_object',
                Params={'Bucket': yelp_config['bucket_name'],
                'Key':yelp_config['bucket_dir'] +'/'+ filename },
                ExpiresIn=3600 # 1 hour in seconds
                )
        # do the downloading
        logger.info("Feed URL: %s", url)
        try:
            download_url_to_file(url, yelp_config['data_dir'], yelp_config['feed_file'])
            # if we succeed, move on
            break

        except BadResponseError:
            if day_delta == 30: 
                logger.warning("NO YELP DATA AVAILABLE FOR THE PAST MONTH!")
                return
            else:
                logger.warning("no data for date: %s\n\
                     Trying the day before." % day.strftime("%m/%d/%Y"))

    logger.info("Latest Yelp Data successfully downloaded from feed.")
    # save the success to download history
    ydh.downloaded = True
    return local_file, ydh

def unzip_file(filename, ydh):
    """
    Take in a .gz file and unzip it, saving it with the same file name
    """
    # if it doesn't exist, error out because we need to download it
    if not ydh.downloaded:
        logger.critical("Cannot unzip file for today because it wasn't downloaded")
        return

    if ydh.unzipped:
        logger.info("Today's feed already unzipped, skipping the unzip")
        rawfile = filename.strip('.gz')
        return rawfile, ydh

    logger.info("Extracting file: %s" % filename)
    with gzip.open(filename, 'rb') as infile:
        rawfile = filename.strip('.gz')
        with open(rawfile, 'wb') as outfile:
            i=1
            for line in infile:
                outfile.write(line)
                if i % 1000 == 0 :
                    print "\r Extracted %i businesses so far" % i
                i += 1
    logger.info("Done extracting file: %s" % rawfile)
    ydh.unzipped = True
    return rawfile, ydh

def yelp_feed_to_graph_csv(filename):
    """ Converts a yelp feed json into csvs for bulk insertion.

    This is highly optimized and makes the bulk yelp insert
    about as fast as possible.

    Last benchmark clocked this pipeline at about <UNKNOWN> hours
    for about 35k business and 250k reviews.
    """ 
    num_businesses = sum(1 for line in open(filename, 'rb'))
    logger.info('Writing out all yelp data to csv for ETL')
    logger.info('{} total businesses... this usually takes about 10 minutes'.format(num_businesses))
    start = time.time()
    
    # open csv file handles
    prefix = yelp_config['neo_data_dir']
    locations_file = open(path.join(prefix, 'locations.csv'), 'wb')
    businesses_file = open(path.join(prefix, 'businesses.csv'), 'wb')
    categories_file = open(path.join(prefix, 'categories.csv'), 'wb')
    categories_businesses_file = open(path.join(prefix, 'categories_businesses.csv'), 'wb')
    reviews_file = open(path.join(prefix, 'reviews.csv'), 'wb')
    users_file = open(path.join(prefix, 'users.csv'), 'wb')
    
    # create the csv writers
    locations_writer = csv.writer(locations_file, quoting=csv.QUOTE_MINIMAL, doublequote=True)
    businesses_writer = csv.writer(businesses_file, quoting=csv.QUOTE_MINIMAL, doublequote=True)
    categories_writer = csv.writer(categories_file, quoting=csv.QUOTE_MINIMAL, doublequote=True)
    categories_businesses_writer = csv.writer(categories_businesses_file, quoting=csv.QUOTE_MINIMAL, doublequote=True)
    reviews_writer = csv.writer(reviews_file, quoting=csv.QUOTE_MINIMAL, doublequote=True)
    users_writer = csv.writer(users_file, quoting=csv.QUOTE_MINIMAL, doublequote=True)
    
    # iterate over the data and write it to file online 
    with open(filename, 'rb') as datafile:
        # these must be cached as they can have duplicates
        locations = {}
        categories ={}
        num_users = 0
        
        for i, line in enumerate(datafile):
            try:
                og_business_dict = loads(line)
            except ValueError:
                logger.warning("Broken JSON Element. Skipping...")
                continue
            
            created = int(datetime.now().strftime('%s'))
            #logger.info("Syncing business {}".format(i))
            # create the location
            location_dict = yelp_location_dict(og_business_dict['location'])
            location_dict['_created'] = created
            location_dict['_modified'] = created
            locations[location_dict['_identifier']] = location_dict
            
            # create the business node
            business_dict = yelp_business_dict(og_business_dict)
            business_dict['location_id'] = location_dict['_identifier']
            business_dict['_created'] = created
            business_dict['_modified'] = created
            if i == 0:
                businesses_writer.writerow(business_dict.keys())
            businesses_writer.writerow(business_dict.values())
            
            # create the categories
            for j, category_dict in enumerate(og_business_dict['categories']):
                category_dict = yelp_category_dict(category_dict)
                category_dict['_created'] = created
                category_dict['_modified'] = created
                categories[category_dict['alias']] = category_dict
                cat_biz_dict = OrderedDict({'business_id':business_dict['business_id'],
                                'alias':category_dict['alias']})
                if i == 0:
                    categories_businesses_writer.writerow(cat_biz_dict.keys())
                categories_businesses_writer.writerow(cat_biz_dict.values())
                
            l = len(og_business_dict['reviews'])
            for j, review_dict in enumerate(og_business_dict['reviews']):
                # print out a detailed status update
                time_sofar = time.time() - start
                h,m,s = sec_to_hms(time_sofar)
                print '\r Business: {0}/{1} Review: {2:>4}/{3:>4} - {4:.0f}:{5:.0f}:{6:.0f} so far'.format(
                         i+1, num_businesses, j+1, l, h,m,s),
                
                # create the user
                user_dict = yelp_user_dict(review_dict['user'])
                user_dict['id'] = num_users
                user_dict['_created'] = created
                user_dict['_modified'] = created
                num_users += 1
                if i == 0:
                    users_writer.writerow(user_dict.keys())
                users_writer.writerow(user_dict.values())
                
                # create the review
                review_dict = yelp_review_dict(review_dict)
                review_dict['business_id'] = business_dict['business_id']
                review_dict['user_id'] = user_dict['id']
                review_dict['_created'] = created
                review_dict['_modified'] = created
                if i == 0:
                    reviews_writer.writerow(review_dict.keys())
                reviews_writer.writerow(review_dict.values())
        
        # write out the locations
        for i, location_dict in enumerate(locations.values()):
            if i == 0:
                locations_writer.writerow(location_dict.keys())
            locations_writer.writerow(location_dict.values()) 
            
        # write out the categories
        for i, category_dict in enumerate(categories.values()):
            if i == 0:
                categories_writer.writerow(category_dict.keys())
            categories_writer.writerow(category_dict.values()) 
        
        # close up file handles
        locations_file.close()
        businesses_file.close()
        categories_file.close()
        categories_businesses_file.close()
        users_file.close()
        reviews_file.close()
        
        logger.info("Successfully wrote all data to csv")

def create_node_string(filename, labels, fields):
    labels_string = ':'.join(labels)
    fields_string = ''
    for field in fields:
        fields_string += '{}: row.{},'.format(field, field)
    fields_string = fields_string[:-1] # drop trailing ','
    return """
    USING PERIODIC COMMIT
    LOAD CSV WITH HEADERS FROM "file:///{0}" AS row
    CREATE (:{1} {{{2}}});""".format(filename, labels_string, fields_string)

def create_edge_string(filename, e1, e2, edge_label):
    return """
    USING PERIODIC COMMIT
    LOAD CSV WITH HEADERS FROM "file:///{0}" AS row
    MATCH (e1:{e1[label]} {{{e1[index_name]}: row.{e1[col_name]}}})
    MATCH (e2:{e2[label]} {{{e2[index_name]}: row.{e2[col_name]}}})
    CREATE (e1)-[:{edge_label}]->(e2);""".format(filename, e1=e1, e2=e2, edge_label=edge_label)

def cypher_bulk_create(download_history):
    # create the location nodes
    locations_statement = create_node_string('locations.csv',
                                          ['Location'],
                                          ['_created',
                                           '_modified',
                                           'latitude',
                                           'longitude',
                                           'line1',
                                           'line2',
                                           'line3',
                                           'city',
                                           'state',
                                           'postal_code',
                                           'country',
                                           'bbox_height',
                                           'bbox_width',
                                           '_identifier'
                                          ])
    db.run(locations_statement)
    
    # create the business nodes
    business_statement = create_node_string('businesses.csv',
                                         ['YelpBusiness'],
                                         ['_created',
                                          '_modified',
                                          'business_id',
                                          'name',
                                          'phone',
                                          'rating',
                                          'url',
                                          'business_url',
                                          '_last_updated',
                                          'is_closed'
                                         ])
    db.run(business_statement)
    
    # create the reviews
    reviews_statement = create_node_string('reviews.csv',
                                         ['Document', 'YelpReview'],
                                         ['_created',
                                          '_modified',
                                          'text',
                                          'rating',
                                          'review_id',
                                          '_authored_date'
                                         ])
    db.run(reviews_statement)
    
    # create the categories
    categories_statement = create_node_string('categories.csv',
                                            ['YelpCategory'],
                                            ['_created',
                                             '_modified',
                                             'alias',
                                             'title'
                                            ])
    db.run(categories_statement)
    
    # create the users
    users_statement = create_node_string('users.csv',
                                        ['YelpUser'],
                                        ['_created',
                                         '_modified',
                                         'id',
                                         'name',
                                         'photo_url'])
    db.run(users_statement)
    
    # create indicies for each node type
    db.run("CREATE INDEX ON :Location(_identifier);")
    db.run("CREATE INDEX ON :YelpBusiness(business_id);")
    db.run("CREATE INDEX ON :YelpCategory(alias);")
    db.run("CREATE INDEX ON :YelpReview(review_id);")
    db.run("CREATE INDEX ON :YelpUser(id);")

    
    # create business->location relations
    businesses_locations_statement = create_edge_string('businesses.csv',
                                                      {'label':'YelpBusiness',
                                                       'index_name':'business_id',
                                                       'col_name':'business_id'},
                                                      {'label':'Location',
                                                       'index_name':'_identifier',
                                                       'col_name':'location_id'},
                                                      'LOCATED_AT')
    db.run(businesses_locations_statement)
    
    # create business->category relations
    categories_businesses_statement = create_edge_string('categories_businesses.csv',
                                                         {'label':'YelpBusiness',
                                                          'index_name':'business_id',
                                                          'col_name':'business_id'},
                                                         {'label':'YelpCategroy',
                                                          'index_name':'alias',
                                                          'col_name':'alias'},
                                                         'HAS_CATEGORY')
    db.run(categories_businesses_statement)
    
    # create the review->business relations
    reviews_businesses_statement = create_edge_string('reviews.csv',
                                                     {'label':'YelpReview',
                                                      'index_name':'review_id',
                                                      'col_name':'review_id'},
                                                     {'label':'YelpBusiness',
                                                      'index_name':'business_id',
                                                      'col_name':'business_id'},
                                                     'IS_REVIEW_OF')
    db.run(reviews_businesses_statement)
    
    # create the user->review relations
    users_reviews_statement = create_edge_string('reviews.csv',
                                                 {'label':'YelpUser',
                                                  'index_name':'id',
                                                  'col_name':'user_id'},
                                                 {'label':'YelpReview',
                                                  'index_name':'review_id',
                                                  'col_name':'review_id'},
                                                 'AUTHORED')
    db.run(users_reviews_statement)
    logger.info('Yelp Data Bulk Insert Completed!')
    download_history.successful = True
    return download_history

def upsert_yelpfile_to_db(filename, download_history, last_download_date=None):
    with open(filename, 'rb') as datafile:
        node_count = 0
        upload_mod = 500
        nodes, relations = [], []
        # category_cache, location_cache, user_cache = {}, {}, {}
        for i, line in enumerate(datafile):
            try:
                business_dict = loads(line)
            except ValueError:
                logger.warning("Broken JSON Element. Skipping...")
                continue

            # skip businesses that haven't been updated since the last download
            if (last_download_date
                and datetime.strptime(business_dict['time_updated'], "%Y-%m-%dT%H:%M:%S").date()
                < last_download_date):
                logger.info('Skipping unchanged business')
                continue

            logger.info("Syncing business {}".format(i))
            # create the business node
            business_node = Node('YelpBusiness', 
                                 **yelp_business_dict(business_dict))
            nodes.append(business_node)

            # create the location
            location_node = Node('Location',
                                 **yelp_location_dict(business_dict['location']))
            nodes.append(location_node)
            # create the categories
            for category_dict in business_dict['categories']:
                category_node = Node('YelpCategory',
                                     **category_dict)
                nodes.append(category_node)
                # relate business->category
                relations.append(Relationship(business_node, 'HAS_CATEGORY', category_node))
            # connect business and location
            relations.append(Relationship(business_node, 'LOCATED_AT', location_node))
            l = len(business_dict['reviews'])
            for j, review_dict in enumerate(business_dict['reviews']):
                print 'review {} / {}'.format(j+1, l)
                # create the review
                review_node = Node('Document', 'YelpReview',
                                   **yelp_review_dict(review_dict))
                nodes.append(review_node)

                # create the user
                user_node = Node('YelpUser',
                                 **review_dict['user'])
                nodes.append(user_node)
                # connect the user->review and review->business
                relations.append(Relationship(user_node, "AUTHORED", review_node))
                relations.append(Relationship(review_node, "IS_REVIEW_OF", business_node))

                if len(nodes) > upload_mod:
                    p , relations = [], []

    download_history.successful = True
    return download_history

def yelp_business_dict(business_dict):
    return OrderedDict([
            ('business_id',xstr(business_dict['id'], encoding='utf8')),
            ('name',xstr(business_dict['name'], encoding='utf8')),
            ('phone',xstr(business_dict['phone'], encoding='utf8')),
            ('rating',business_dict['rating']),
            ('url',xstr(business_dict['url'], encoding='utf8')),
            ('business_url',xstr(business_dict['business_url'], encoding='utf8')),
            ('_last_updated',int(datetime.strptime(business_dict['time_updated'], "%Y-%m-%dT%H:%M:%S")
                                        .strftime('%s'))),
            ('is_closed',bool(business_dict['is_closed']))
            ])
        

def yelp_location_dict(location_dict):
    location = OrderedDict([
                ('latitude',location_dict['coordinate']['latitude']),
                ('longitude',location_dict['coordinate']['longitude']),
                ('line1',xstr(location_dict['address'][0], encoding='utf8')),
                ('line2',xstr(location_dict['address'][1], encoding='utf8')),
                ('line3',xstr(location_dict['address'][2], encoding='utf8')),
                ('city',xstr(location_dict['city'], encoding='utf8')),
                ('country',xstr(location_dict['country'], encoding='utf8')),
                ('postal_code',xstr(location_dict['postal_code'], encoding='utf8')),
                ('state',xstr(location_dict['state'], encoding='utf8'))
            ])
    if location['latitude'] is not None:
        location['bbox_width'] = 0
        location['bbox_height'] = 0
    else:
        location['bbox_width'] = None
        location['bbox_height'] = None
    location['_identifier'] = xstr(location_identifier(location), encoding='utf8')
    return OrderedDict(location)
    
def yelp_review_dict(review_dict):
    # there is a corner case where a review['text] field
    # will end in a '\' which causes an escape of the 
    # quote around the cell in the csv table
    # when Cypher does LOAD CSV.
    # to fix this we just replace '\' with '/'
    return OrderedDict([
        ('text',xstr(review_dict['text'].replace('\\', '/'), encoding='utf8')),
        ('rating',review_dict['rating']),
        ('_authored_date',int(datetime.strptime(review_dict['created'], '%Y-%m-%d')
                                     .strftime('%s'))),
        ('review_id',xstr(review_dict['id'], encoding='utf8'))
    ])

def yelp_category_dict(category_dict):
    return OrderedDict([
            ('alias', xstr(category_dict['alias'], encoding='utf8')),
            ('title', xstr(category_dict['title'], encoding='utf8'))
        ])

def yelp_user_dict(user_dict):
    photo_url = user_dict['photo_url'] if 'photo_url' in user_dict else ''
    return OrderedDict([
            ('name', xstr(user_dict['name'], encoding='utf8')),
            ('photo_url', xstr(photo_url, encoding='utf8'))
        ])


from random import shuffle # to shuffle list in place

def geocodeUnknownLocations(wait_time=2, run_time=240):
    """
    Geocode any locations that don't have Lat/Lons

    Only do so for up to `run_time` minutes, because this can take a very long time if most are unknown

    Also shuffle so that they all get equal probability of being tried

    Args:
        wait_time: how long to wait until timeout

    Returns: 
        None

    """
    geoLocator = Nominatim()
    # print geoLocator.geocode("548 riverside dr., NY, NY, 10027") # test
    db = get_db_session()
    unknowns = db.query(Location).filter(Location.latitude==None).all()
    shuffle(unknowns) # give them all a fighting chance
    logger.info("Attempting to geocode random unknown locations for %i minutes" % run_time)
    logger.info("%i Unkown locations to geocode" % len(unknowns))
    locations = []
    upload_mod = 100 # upload batch size

    start_time = time.time()
    run_time *= 60 # turn it into seconds

    for i, location in enumerate(unknowns):
        # max try time stopping criterion
        if (time.time() - start_time) > run_time:
            logger.info("Max geocoding time has elapsed... Stopping for this run")
            db.add_all(locations)
            db.commit()
            break
        # print location.street_address
        logger.info("Geocoding location %i..." % i)
        try:
            geo = geoLocator.geocode(location.street_address, timeout=wait_time)
            lat = geo.latitude
            lon = geo.longitude
            logger.info("\tSuccess!")
        except Exception as e:
            # print  "Exception: ", e
            logger.warning("\tGeocode failed, assigning NULL Lat/Long")
            lat = None
            lon = None
        location.latitude = lat
        location.longitude = lon
        locations.append(location)
        if i % upload_mod == 0:
            db.add_all(locations)
            db.commit()
            locations = []
    logger.info("Finished geocode attempts")
