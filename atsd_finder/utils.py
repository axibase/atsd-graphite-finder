import urllib
import os
import datetime

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


def strf_timestamp(sec):
    return datetime.datetime.fromtimestamp(sec).strftime('%Y-%m-%d %H:%M:%S')


def _round_step(step):
    """example: 5003 -> 5000
    """
    # TODO: add logic
    return step


def _median_delta(values):
    """get array of delta, find median value
    values should be sorted
    values should contains at least two value

    :param values: `list` of `Numbers`
    :return: `Number`
    """

    deltas = []
    for i in range(1, len(values)):
        deltas.append(values[i] - values[i - 1])

    deltas.sort()
    return deltas[len(deltas) // 2]


def regularize(series, step=None):
    """create values with equal periods

    :param step: `Number` seconds
    :param series: `[{t: long, v: float}]` should contains at least one value
    :return: time_info, values
    """

    # for sample in series:
    #     print(sample)
    times = [sample['t'] / 1000.0 for sample in series]

    if step is None:
        step = _median_delta(times)
        step = _round_step(step)

    # round to divisible by step
    start_time = ((series[0]['t'] / 1000.0) // step) * step
    end_time = ((series[-1]['t'] / 1000.0) // step + 1) * step

    # log.info('regularize {0}:{1}:{2}'
    #          .format(start_time, step, end_time), 'AtsdReader')

    number_points = int((end_time - start_time) // step)

    values = []
    sample_counter = 0

    for i in range(number_points):
        # on each step add some value

        if sample_counter > len(series) - 1:
            values.append(None)
            continue

        t = (start_time + i * step)
        sample = series[sample_counter]

        if abs(times[sample_counter] - t) <= step:
            values.append(sample['v'])
            sample_counter += 1
        else:
            values.append(None)

    time_info = (start_time,
                 start_time + number_points * step,
                 step)

    return time_info, values
