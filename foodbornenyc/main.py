#!/Users/thomaseffland/.virtualenvs/health/bin/python
"""
The commandline entrypoint into the package

    ** All modules are run through this **

    To add you functionality, just import you module and add click commands

"""
import click
from foodbornenyc.util.util import get_logger

logger = get_logger(__name__)

@click.group()
def main():
    pass

""" Add commands to the main group here"""

# model and database related
import foodbornenyc.models.models as models
@main.command()
def dropdb():
    """ Drop every single table in the database...DANGEROUS 
        pass 'yes' or 'y' if you really want to do this..."""
    really = raw_input("Are you sure? This will drop all the tables and cannot be reversed. Enter y or yes to continue")
    if really.lower() == "yes" or really.lower()=="y":
        models.drop_all_tables()
    else:
        logger.warning("Failed to drop because you weren't sure")

@main.command()
def initdb():
    """ Initialize database schema

    Notes: 
        If you have added a new data model, this will only add the new table.
        However, there are integrity errors that can occur if you edit an old table.
        In general editing an old data model requires merging the new columns with default values, 
        or reseting the table...
    """
    models.setup_db()

from foodbornenyc.sources import yelp_fast as Yelp
@main.command()
@click.option('-y', '--yelp', is_flag=True, help="update yelp")
# @profile # needs to be uncommented to profile yelp_fast
def download(yelp):
    """ download content from sources"""
    if yelp:
        fname = Yelp.downloadLatestYelpData()
        data = Yelp.unzipYelpFeed(fname)
        # data = 'foodbornenyc/sources/yelpfiles/yelp_businesses.json' # for testing w/o downloading
        Yelp.updateDBFromFeed(data, geocode=False)
        
    return

from foodbornenyc.methods import yelp_classify
@main.command()
@click.option('-s', '--since', default=7, help="Number of days in past to classify")
@click.option('-u', '--unseen', default=False, is_flag=True, help="Whether to classify any review that hasn't already been classified")
@click.option('-a', '--all', default=False, is_flag=True, help="Whether to do just classify all reviews.  Overrides --since")
@click.option('-v', '--verbose', default=1, help="Specify the verbosity level")
def classify_yelp(since, all, unseen, verbose):
    classifier = yelp_classify.YelpClassify()
    classifier.classify_reviews(all=all, any=unseen, since=since, verbose=verbose)

@main.command()
@click.option('-w', '--wait', default=2, help="Number of seconds before timeout")
@click.option('-t', '--time', default=240, help="Maximum amount of time to run geocode in minutes")
def geocode(wait, time):
    """ Take every location in DB without a (Lat, Lon) and attempt to reverse geocode them"""
    Yelp.geocodeUnknownLocations(wait_time=wait, run_time=time)


#######################
# DEPLOYMENT COMMANDS #
#######################
from foodbornenyc.deployment import yelp as DeployYelp
@main.command()
@click.option('-y', '--yelp', is_flag=True, help="Deploys continuous yelp system")
def deploy(yelp):
    """ Deploy various source components for continuous use by DOHMH """
    if yelp:
        DeployYelp.deploy()


##################################
# The MAIN function: DON'T TOUCH #
##################################      
if __name__ == "__main__":
    main()


### SIMPLE PYTHON EXPERIMENT ###
#from foodbornenyc.experiments.simpletwitterclassify import run
#run()
