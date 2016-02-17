import foodbornenyc.models.models as models
from foodbornenyc.models.businesses import Business, YelpCategory
from foodbornenyc.models.businesses import businesses, categories, business_category_table
from foodbornenyc.models.locations import Location, locations
from foodbornenyc.models.documents import YelpReview, Document
from foodbornenyc.models.documents import yelp_reviews, documents, document_associations

from foodbornenyc.util.util import sec_to_hms, get_logger
logger = get_logger(__name__)

test_config = {
    'user': 'test',
    'password': 'user',
    'dbhost': 'test.db',
    'dbbackend':'sqlite'
}

logger.info("Resetting test tables")
# models.drop_all_tables(test_config)
# models.setup_db(test_config)

test_db = models.get_db_session(test_config)
db = models.get_db_session()

logger.info("Populating test tables")
businesslist = []
bcategorieslist = []
docasoclist = []
doclist = []
locationslist = []
reviewslist = []
ycategorieslist = []


for b in db.query(Business).order_by(Business.id)[0:5]:
    # print b
    # print b.__dict__
    businesslist.append(b.__dict__)
    bcategorieslist += [cat for cat in db.query(business_category_table).filter(business_category_table.c.business_id == b.id)[:]]
    docasoclist += [asoc for asoc in db.query(document_associations).filter(document_associations.c.assoc_id == b.id)[:]]
    doclist += [doc for doc in db.query(documents).filter(documents.c.assoc_id == b.id)[:]]
    locationslist += [loc for loc in db.query(locations).filter(locations.c.street_address == b.location_address)[:]]
    reviewslist += [review for review in db.query(yelp_reviews).filter(yelp_reviews.c.business_id == b.id)[:]]
    # ycategorieslist = [cat for cat in db.query(categories, business_category_table).filter(business_category_table.c.business_id == b.id).filter(business_category_table.c.category_alias == categories.alias)[:]]

# print businesslist
# print bcategorieslist
test_db.execute(businesses.insert(), businesslist)
print bcategorieslist
print docasoclist
test_db.execute(business_category_table.insert(), bcategorieslist)
test_db.execute(document_associations.insert(), docasoclist)
test_db.execute(documents.insert(), doclist)
test_db.execute(locations.insert(), locationslist)
test_db.execute(yelp_reviews.insert(), reviewslist)
# test_db.execute(categories.insert(), ycategorieslist)

test_db.commit()
