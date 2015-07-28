import urllib


_url_codes = {symbol: urllib.quote(symbol, '') \
              for symbol in ' ,:?\'"()[]{}<>/\\|*'}
_url_codes['.'] = '%2E'


def quote(string):

    return urllib.quote(string.encode('utf8'), '*')


def metric_quote(string):

    for symbol in _url_codes:
        string = string.replace(symbol, _url_codes[symbol])

    return string

