import requests
import json
import urlparse
import urllib
import ConfigParser
import fnmatch
import re
import os

from graphite.intervals import Interval, IntervalSet

try:
    from graphite.logger import log
    from django.conf import settings
except:  # debug env
    from graphite import settings
    import default_logger as log
    log.info('running in debugging environment')


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
        deltas.append(values[i] - values[i-1])

    deltas.sort()
    return deltas[len(deltas) // 2]


def _regularize(series):
    """create values with equal intervals

    :param series: should contains at least one value
    :return: time_info, values
    """

    # for sample in series:
    #     print(sample)
    times = [sample['t'] / 1000.0 for sample in series]
    step = _median_delta(times)
    step = _round_step(step)

    start_time = series[0]['t'] / 1000.0
    end_time = series[-1]['t'] / 1000.0 + step

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


def _str_to_sec(val):
    """
    :param val: `str` time interval
    :return: interval `float` seconds
    """
    unit = val[-1]
    num = val[:-1]
    if unit == 's':
        return float(num)
    elif unit == 'm':
        return float(num) * 60
    elif unit == 'h':
        return float(num) * 60 * 60
    elif unit == 'd':
        return float(num) * 24 * 60 * 60
    elif unit == 'y':
        return float(num) * 24 * 60 * 60 * 365.2425
    else:
        return float(val)


def _str_to_interval(val):
    """
    :param val: `str` time interval
    :return: interval `tuple` (count, unit)
    """
    unit = val[-1]
    num = val[:-1]
    if unit == 's':
        return float(num), 'SECOND'
    elif unit == 'm':
        return float(num) * 60, 'SECOND'
    elif unit == 'h':
        return float(num) * 60 * 60, 'SECOND'
    elif unit == 'd':
        return float(num) * 24 * 60 * 60, 'SECOND'
    elif unit == 'y':
        return float(num), 'YEAR'
    else:
        return float(val), 'SECOND'


class Aggregator(object):
    __slots__ = ('type', 'count', 'unit', 'interpolate')

    def __init__(self, type, count, unit='SECOND', interpolate='STEP'):
        # TODO: throw Exception if type == 'DETAIL'
        if not count:
            raise ValueError('Aggregator.count could not be ' + unicode(count))

        if unit == 'MILLISECOND':
            count /= 1000.0
            unit = 'SECOND'
        elif unit == 'MINUTE':
            count *= 60
            unit = 'SECOND'
        elif unit == 'HOUR':
            count *= 60 * 60
            unit = 'SECOND'
        elif unit == 'DAY':
            count *= 60 * 60 * 24
            unit = 'SECOND'

        #: `Number`
        self.count = count
        #: `str`
        self.unit = unit

        #: `str`
        self.type = type
        #: `str`
        self.interpolate = interpolate

    def json(self):
        return {
            'type': self.type,
            'interval': {'count': self.count, 'unit': self.unit},
            'interpolate': self.interpolate
        }


class IntervalSchema(object):
    # _map: interval -> step in seconds

    __slots__ = ('_map',)

    CONF_NAME = os.path.join(settings.CONF_DIR, 'interval-schema.conf')

    _config = ConfigParser.RawConfigParser()
    _config.read(CONF_NAME)
    log.info('[IntervalSchema] sections=' + str(_config.sections()))

    def __init__(self, metric):
        """
        :param metric: `str` metric name
        """

        def section_matches(section):

            if self._config.has_option(section, 'metric-pattern'):
                metric_pattern = self._config.get(section, 'metric-pattern')
                return fnmatch.fnmatch(metric, metric_pattern)
            return True

        for section in self._config.sections():
            if section_matches(section):

                try:
                    # intervals has form 'x:y, z:t'
                    intervals = self._config.get(section, 'retentions')  # str
                    items = re.split('\s*,\s*', intervals)  # list of str
                    pairs = (item.split(':') for item in items)  # str tuples
                    self._map = dict((_str_to_sec(b), _str_to_interval(a))
                                     for a, b in pairs)
                    return

                except Exception as e:
                    log.exception('could not parse section {:s} in {:s}'
                                  .format(section, IntervalSchema.CONF_NAME), e)

        self._map = {}

    def aggregator(self, interval):
        """find step for current interval using interval schema
        return step of lowest schema interval that bigger than current

        :param interval: `Number` seconds
        :return: :class:`.Aggregator` | None
        """

        intervals = self._map.keys()
        intervals.sort()

        count = 0
        unit = None
        for i in intervals:
            if i >= interval:
                count, unit = self._map[i]
                break

        if count:
            return Aggregator('AVG', count, unit)
        else:  # unit = None
            return None


class Instance(object):
    """
    series unique identifier
    """

    __slots__ = ('entity_name', 'metric_name', 'tags', '_session', '_context')

    def __init__(self, entity_name, metric_name, tags):
        # TODO check that node exists, get unique combination of fields
        #: `str`
        self.entity_name = entity_name
        #: `str`
        self.metric_name = metric_name
        #: `dict`
        self.tags = tags
        #: :class:`.Session`
        self._session = requests.Session()
        self._session.auth = (settings.ATSD_CONF['username'],
                              settings.ATSD_CONF['password'])
        #: `str` api path
        self._context = urlparse.urljoin(settings.ATSD_CONF['url'], 'api/v1/')

    def _request(self, method, path, data=None):
        """
        :param method: `str`
        :param path: `str` url after 'api/v1'
        :param data: `dict` or `list` json body of request
        :return: `dict` or `list` response.json()
        :raises RuntimeError: server response not 200
        """

        request = requests.Request(
            method=method,
            url=urlparse.urljoin(self._context, path),
            data=json.dumps(data)
        )

        # print '============request=========='
        # print '>>>method:', request.method
        # print '>>>path:', request.url
        # print '>>>data:', request.data
        # print '>>>params:', request.params
        # print '============================='

        prepared_request = self._session.prepare_request(request)
        response = self._session.send(prepared_request)

        # print '===========response=========='
        # print '>>>status:', response.status_code
        # print '>>>cookies:', response.cookies.items()
        # print '>>>content:', response.text
        # print '============================='

        if response.status_code != 200:
            raise RuntimeError('server response status_code={:d} {:s}'
                               .format(response.status_code, response.text))

        return response.json()

    def query_series(self, start_time, end_time, aggregator):
        """
        :param start_time: `Number` seconds
        :param end_time: `Number` seconds
        :param aggregator: :class:`.Aggregator` | None
        :return: series data [{t,v}]
        """

        tags_query = {}
        for key in self.tags:
            tags_query[key] = [self.tags[key]]

        data = {
            'queries': [
                {
                    'startTime': int(start_time * 1000),
                    'endTime': int(end_time * 1000),
                    'entity': self.entity_name,
                    'metric': self.metric_name,
                    'tags': tags_query
                }
            ]
        }

        if aggregator and not aggregator.type == 'DETAIL':
            # request regularized data
            data['queries'][0]['group'] = aggregator.json()

        resp = self._request('POST', 'series', data)

        return resp['series'][0]['data']

    def get_metric(self):
        """make meta api request

        :return: parsed json response
        """

        return self._request('GET',
                             'metrics/' + urllib.quote(self.metric_name, ''))

    def get_entity(self):
        """make meta api request

        :return: parsed json response
        """

        return self._request('GET',
                             'entities/' + urllib.quote(self.entity_name, ''))


class AtsdReader(object):
    __slots__ = ('_instance',
                 'aggregator',
                 '_interval_schema')

    def __init__(self, entity, metric, tags, aggregator=None):
        #: :class:`.Node`
        self._instance = Instance(entity, metric, tags)
        #: `Number` seconds, if 0 raw data

        #: :class:`.Aggregator` | `None`
        self.aggregator = aggregator

        #: :class:`.IntervalSchema`
        self._interval_schema = IntervalSchema(metric)

        log.info('[AtsdReader] init: entity=' + unicode(entity)
                 + ' metric=' + unicode(metric)
                 + ' tags=' + unicode(tags)
                 + ' url=' + unicode(settings.ATSD_CONF['url'])
                 + ' aggregator=' + str(aggregator))

    def fetch(self, start_time, end_time):
        """fetch time series

        :param start_time: `Number` seconds
        :param end_time: `Number` seconds
        """

        log.info(
            '[AtsdReader] fetching:  start_time={:f} end_time= {:f}'
            .format(start_time, end_time)
        )

        if self.aggregator:
            aggregator = self.aggregator
        else:
            aggregator = self._interval_schema.aggregator(end_time - start_time)

        series = self._instance.query_series(start_time,
                                             end_time,
                                             aggregator)

        log.info('[AtsdReader] get series of {:d} samples'.format(len(series)))

        if not len(series):
            return (start_time, end_time, end_time - start_time), [None]
        if len(series) == 1:
            return (start_time, end_time, end_time - start_time), [series[0]['v']]

        if aggregator and aggregator.unit == 'SECOND':
            # data regularized, send as is
            time_info = (float(series[0]['t']) / 1000,
                         float(series[-1]['t']) / 1000 + aggregator.count,
                         aggregator.count)

            values = [sample['v'] for sample in series]
        else:
            time_info, values = _regularize(series)

        log.info('[AtsdReader] fetched {:d} values, step={:f}'
                 .format(len(values), time_info[2]))

        return time_info, values

    def get_intervals(self):
        """
        :return: :class:`.IntervalSet`
        """

        log.info('[AtsdReader] getting_intervals')

        metric = self._instance.get_metric()
        try:
            entity = self._instance.get_entity()
        except RuntimeError:  # server response != 200
            end_time = metric['lastInsertTime']
        else:
            end_time = max(metric['lastInsertTime'], entity['lastInsertTime'])

        retention = metric['retentionInterval']
        start_time = (end_time - retention) if retention else 1

        return IntervalSet([Interval(start_time, end_time)])
