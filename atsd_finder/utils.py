import urllib


def quote(string):

    return urllib.quote(string.encode('utf8'), safe='*')


def metric_quote(string):

    return urllib.quote(string.encode('utf8'), safe='').replace('.', '%2E')
