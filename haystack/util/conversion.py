""" Special conversion functions """
def xstr(s, encoding=None):
    """Catch all to convert strings and NoneTypes to strings with proper encoding."""
    if s is None:
        s = ''
    if not isinstance(s, basestring):
        s = str(s)
    if encoding:
        if not isinstance(s, unicode):
            s = unicode(s, 'utf8')
        s = s.encode(encoding)
    return s