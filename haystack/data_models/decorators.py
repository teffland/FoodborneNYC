""" Some Helpful decorators """
import datetime
def singleton_relation(relation):
    """ Decorator for singleton relation properties.

        py2neo doesn't natively support singleton relations.
        Ie, relations that can only have one constituent
        such as (Document) - [:Published_On] -> (Date).

        However for the OGM it is convenient not to have to iterate over a list
        of length 1 to recover the document.published_date.
        So this function defines a decorator for making a py2neo relation
        behave as if it can only have one value.

        This is accomplished in combo with the `@property` decorator.

        Example Usage:
        ```python
            class Document(GraphObject):
                _published_date = RelatedTo('Date' ,'Published_On')
                @property
                @singleton_relation
                def published_date(self):
                    return self._published_date
            ...
            doc = Document()
            date_published = doc.published_date
        ```

        instead of needing to do

        ```python
            date_published = list(doc.published_date)[0] if doc.published_date else None
        ```

        all over the application code
    """
    def singleton(*args, **kwargs):
        _list = list(relation(*args, **kwargs))
        if len(_list) > 1:
            raise ValueError, "Singleton Relation cannot have more than one related neighbor. Has {}".format(len(_list))
        elif len(_list) == 1:
            return _list[0]
        else:
            return None
    return singleton


def datetime_getter(property_getter):
    """ Decorator to make a `Property` a date datatype

        py2neo Properties can't natively handle type constraints
        but many should be dates.

        It would be nice for the OGM classes to know about their date types
        natively, but persist them consistently (since they can't be stored as datetimes.

        Applying this decorator over a property getter will return a datetime object
        from the timestamp persisted to the db.

        Example Usage:
        ```python
            class Document(GraphObject):
                _published_date = Property() # unix timestamp int
                @property
                @datetime_getter
                def published_date(self):
                    return self._published_date
            ...
            doc = Document()
            date_published = doc.published_date 
            # type(date_published) is datetime.datetime
        ```

        instead of needing to do

        ```python
            import datetime.datetime
            _published_date = Property() # unix timestamp int
            @property
            def published_date(self):
                return datetime.datetime.fromtimestamp(self._published_date)
        ```
    """
    def timestamp_as_datetime(*args, **kwargs):
        val = property_getter(*args, **kwargs)
        if not val:
            return val
        elif type(val) is not int:
            val = int(val)
        return datetime.datetime.fromtimestamp(val)
    return timestamp_as_datetime

def datetime_setter(property_setter):
    """ Decorator to set a `Property` with date datatype

        py2neo Properties can't natively handle type constraints
        but many should be dates.

        It would be nice for the OGM classes to know about their date types
        natively, but persist them consistently (since they can't be stored as datetimes.

        Applying this decorator over a property setter will automatically 
        convert datetimes to unix timestamp ints

        Example Usage:
        ```python
            class Document(GraphObject):
                _published_date = Property() # unix timestamp int
                @property
                @datetime_getter
                def published_date(self):
                    return self._published_date

                @published_date.setter
                @datetime_setter
                def published_date(self, date_value)
                    self._published_date = date_value
            ...
            doc = Document()
            doc.date_published = datetime.datetime.now()
        ```

        instead of needing to do

        ```python
            import datetime.datetime
            _published_date = Property() # unix timestamp int
            @property
            def published_date(self):
                return datetime.datetime.fromtimestamp(self._modified)
            @published_date.setter
            def published_date(self, date_value):
                if type(date_value) is str:
                    date_value = int(value)
                if type(date_value) is int:
                    date_value = datetime.datetime.fromtimestamp(date_value)
                self._published_date = int(date_value.strftime('%s'))
        ```

        For every Property that should act like a date.  
        Further this allows more robust object date handling 
        in the future by just updating this decorator

        TODO (teffland): Make this date handling more robust
            (potentially use date_util to parse arbitrary datestrings)
    """
    def date_value_as_timestamp(self, date_value, *args, **kwargs):
        if type(date_value) is str:
            date_value = int(value)
        if type(date_value) is int:
            date_value = datetime.datetime.fromtimestamp(date_value)
        date_value = int(date_value.strftime('%s'))
        property_setter(self, date_value, *args, **kwargs)
    return date_value_as_timestamp