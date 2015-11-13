import urllib
import os

try:
    # noinspection PyUnresolvedReferences
    PID = unicode(os.getppid()) + ':' + unicode(os.getpid())
except AttributeError:
    PID = unicode(os.getpid())

try:
    from graphite.logger import log
    DEBUG = False
except:
    log = None
    DEBUG = True


def _instance_name(inst):
    if inst is None:
        return ''

    if isinstance(inst, basestring):
        return inst

    return type(inst).__name__


class GraphiteLogger(object):

    @staticmethod
    def info(msg, inst=None):
        inst_name = _instance_name(inst)
        log.info('[' + inst_name + ' ' + PID + '] ' + msg)

    @staticmethod
    def exception(msg, inst=None):
        inst_name = _instance_name(inst)
        log.exception('[' + inst_name + ' ' + PID + '] ' + msg)


class ConsoleLogger(object):

    @staticmethod
    def info(msg, inst=None):
        inst_name = _instance_name(inst)
        print('[' + inst_name + ' ' + PID + '] ' + msg)

    @staticmethod
    def exception(msg, inst=None):
        inst_name = _instance_name(inst)
        print('[' + inst_name + ' ' + PID + '] ' + msg)

if DEBUG:
    logger = ConsoleLogger()
else:
    logger = GraphiteLogger()


def get_logger():
    return logger


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
