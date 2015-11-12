import urllib


def quote(string):

    return urllib.quote(string.encode('utf8'), safe='')


def metric_quote(string):

    return urllib.quote(string.encode('utf8'), safe='').replace('.', '%2E')


def unquote(string):

    return urllib.unquote(string.encode('utf8'))


def parse_path(path):
    """convert graphite metric to atsd metric-entity tuple

    :param path: `str`
    :return: (metric: `str`, entity: `str`)
    """

    metric_entity = path.split('.', 1)

    metric = metric_entity[0]

    if len(metric_entity) == 1:
        entity = '*'
    else:
        entity = metric_entity[1]

    metric = 'graphite_' + metric

    return metric, entity
