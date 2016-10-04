""" Base GraphObject class """
import datetime

from py2neo import Graph
from py2neo.ogm import GraphObject, Property, Label, Related, RelatedTo, RelatedFrom

db = Graph(password="fbnyc")

from decorators import datetime_getter, datetime_setter

class CustomGraphObject(GraphObject):
    """ A Base OGM class that every user-defined type should subclass.
    
        Takes care of application-wide bookkeeping automatically
        
        These bookkeeping fields are:
            * created
            * modified
    """
    # both _created and _modified are expected to be int unix timestamps (local)
    _created = Property()
    _modified = Property()
    
    @property
    @datetime_getter
    def created(self):
        return self._created
    @created.setter
    def created(self, value):
        pass
        
    @property
    @datetime_getter
    def modified(self):
        return self._modified
    @modified.setter
    @datetime_setter
    def modified(self, date_value):
        self._modified = date_value
        
    # automatically set created and modified when initializing
    def __init__(self, **properties):
        super(CustomGraphObject, self).__init__()
        self._created = int(datetime.datetime.now().strftime('%s'))
        self._modified = int(datetime.datetime.now().strftime('%s'))
        # allows users to populated object properties at init
        # they will only be saved to the DB if the subclass
        # declares the `prop_name` as a `Property()`
        for prop_name, prop_value in properties.items():
            setattr(self, prop_name, prop_value)
        
    # automatically set modified when changing node in the db
    def __db_push__(self, graph):
        self.modified = datetime.datetime.now()
        super(CustomGraphObject, self).__db_push__(graph)
