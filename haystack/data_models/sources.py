""" Data Models for objects coming from document sources """
import datetime

from py2neo.ogm import GraphObject, Property, Label, Related, RelatedTo, RelatedFrom

from base import CustomGraphObject
from decorators import singleton_relation, datetime_getter, datetime_setter
from haystack.util.logger import get_logger
from haystack.util.conversion import xstr
logger = get_logger(__name__)

# TODO (teffland): Contribute a backref feature to py2neo OGM
        
class Document(CustomGraphObject):
    """ A base class that all other document types inherit.

        It just defines the 'text' property 
        and adds a 'Document' label to all nodes that inherit it.
    """
    text = Property()
    
    def __init__(self):
        super(Document, self).__init__()
        self.__ogm__.node.add_label("Document")    

def location_identifier(location_dict):
    """ Takes a dict of location attributes and returns a unique identifier.

        Defined outside of location because it is sometimes used w/o the class.
    """
    expected_attributes = [
        'line1',
        'line2',
        'line3',
        'city',
        'state',
        'postal_code',
        'bbox_width',
        'bbox_height'
    ]
    attribute_values = []
    for attr in expected_attributes:
        if attr in location_dict:
            attribute_values.append(xstr(location_dict[attr], encoding='utf8'))
        else:
            attribute_values.append(xstr("NULL_{}".format(attr), encoding='utf8'))
    return xstr(" | ".join(attribute_values), encoding='utf8')

class Location(CustomGraphObject):
    __primarykey__ = '_identifier'
    latitude = Property()
    longitude = Property()
    bbox_height = Property()
    bbox_width = Property()
    line1 = Property()
    line2 = Property()
    line3 = Property()
    city = Property()
    country = Property()
    postal_code = Property()
    state = Property()
    _identifier = Property()
    @property
    def identifier(self):
        self._identifier = location_identifier(dict(self))
        return self._identifier
    @identifier.setter
    def identifier(self, value):
        logger.warning("Cannot set identifier property of a location")

    businesses = RelatedFrom('YelpBusiness', "LOCATED_AT")

    def __init__(self, **properties):
        super(Location, self).__init__(**properties)
        self._identifier = location_identifier(dict(self))

########
# YELP #
########
class YelpReview(Document):
    __primarykey__ = "yelp_id"
    
    review_id = Property()
    rating = Property()
    _authored_date = Property()
    @property
    @datetime_getter
    def authored_date(self):
        return self._authored_date
    @authored_date.setter
    @datetime_setter
    def authored_date(self, value):
        self._authored_date = value

    _user = RelatedFrom("YelpUser", "AUTHORED")
    @property
    @singleton_relation
    def user(self):
        return self._user

    _business = RelatedTo("YelpBusiness", "IS_REVIEW_OF")
    @property
    @singleton_relation
    def business(self):
        return self._business

class YelpUser(CustomGraphObject):
    photo_url = Property()
    name = Property()

    reviews = RelatedTo("YelpReview", "AUTHORED")

class YelpBusiness(CustomGraphObject):
    __primarykey__ = 'business_id'

    business_id = Property()
    name = Property()
    phone = Property()
    rating = Property()
    url = Property()
    business_url = Property()
    is_closed = Property()
    _last_updated = Property()
    @property
    @datetime_getter
    def last_updated(self):
        return self._last_updated
    @last_updated.setter
    @datetime_setter
    def last_updated(self, value):
        self._last_updated = value

    categories = RelatedTo("YelpCategory", "HAS_CATEGORY")
    reviews = RelatedFrom("YelpReview", "IS_REVIEW_OF")
    _location = RelatedTo("Location", "LOCATED_AT")
    @property
    @singleton_relation
    def location(self):
        return self._location

class YelpCategory(CustomGraphObject):
    __primarykey__ = 'alias'

    alias = Property()
    title = Property()

    businesses = RelatedFrom("YelpBusiness", "HAS_CATEGORY")

class YelpDownloadHistory(CustomGraphObject):
    __primarykey__ = '_created'
    downloaded = Property()
    unzipped = Property()
    successful = Property()

   
###########
# TWITTER #
###########
class Tweet(Document):
    __primarykey__ = "twitter_id"
    
    twitter_id = Property()

    # a singleton relation
    _user = RelatedFrom("TwitterUser", "POSTED")
    @property
    @singleton_relation
    def user(self):
        return self._user
    
class TwitterUser(CustomGraphObject):
    __primarykey__ = "twitter_id"
    
    twitter_id = Property()
    screen_name = Property()
    
    tweets = RelatedTo("Tweet", "POSTED")