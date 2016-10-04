#!/Users/thomaseffland/.virtualenvs/health/bin/python
"""The commandline entrypoint into the haystack

    ** All modules are run through this **

    To add you functionality, just import you module and add click commands

"""
import click
from time import time

from haystack.util.logger import get_logger
logger = get_logger(None, 'INFO')

@click.group()
def main():
    pass

from haystack.data_models.base import db
@main.command()
def drop_db():
    print "Are you sure you want to drop the database?"
    print "This action CANNOT BE UNDONE"
    response = raw_input("Enter 'Yes' to continue: ")
    if response.lower() == "yes":
        db.run('match (n) detach delete n')
        print "Successfully dropped database"
    else:
        print "Wrong response: aborting"

from haystack.sources import yelp as Yelp
@main.command()
@click.option('-y', '--yelp', is_flag=True, default=False, help='Option to download the yelp feed')
def download(yelp):
    if yelp:
        Yelp.update_yelp_from_feed()

if __name__ == '__main__':
    main()