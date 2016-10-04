""" Timing Helpers """
def sec_to_hms(seconds):
    """Return triple of (hour,minutes,seconds) from seconds"""
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return h, m, s