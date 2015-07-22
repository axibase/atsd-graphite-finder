import requests
import json
import urlparse
import urllib
import ConfigParser
import re

from graphite.intervals import Interval, IntervalSet
from graphite.local_settings import ATSD_CONF
try:
    from graphite.logger import log
except:
    import default_logger as log


INTERVAL_SCHEMA_FILE_NAME = 'c:/Users/Egor/IdeaProjects/atsd-graphite-finder/atsd_finder/interval-schema.conf'


def _get_interval_schema(node):

    config = ConfigParser.RawConfigParser()
    config.read(INTERVAL_SCHEMA_FILE_NAME)

    def section_matches(section):

        if config.has_option(section, 'metric-pattern'):
            return re.match(config.get(section, 'metric-pattern'), node.metric)
        return True

    for section in config.sections():
        if section_matches(section):

            try:
                # intervals has form 'x:y, z:t'
                intervals = config.get(section, 'retentions')  # str
                items = re.split('\s*,\s*', intervals)  # list of str
                pairs = (item.split(':') for item in items)  # gen of str tuples
                return dict((int(b), int(a)) for a, b in pairs)
            except Exception as e:
                log.exception('could not parse section {:s} in {:s}'
                              .format(section, INTERVAL_SCHEMA_FILE_NAME), e)

    return {}


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


class Node(object):

    slots = ('entity', 'metric', 'tags')

    def __init__(self, entity, metric, tags):
        # TODO check that node exists, get unique combination of fields
        #: `str`
        self.entity = entity
        #: `str`
        self.metric = metric
        #: `dict`
        self.tags = tags


class AtsdReader(object):
    __slots__ = ('_node',
                 '_session',
                 '_context',
                 'default_step',
                 'statistic',
                 '_interval_schema')

    def __init__(self, entity, metric, tags, step, statistic='DETAIL'):
        #: :class:`.Node`
        self._node = Node(entity, metric, tags)
        #: `Number` seconds, if 0 raw data
        self.default_step = step
        #: :class:`.AggregateType`
        self.statistic = statistic

        #: :class:`.Session`
        self._session = requests.Session()
        self._session.auth = (ATSD_CONF['username'],
                              ATSD_CONF['password'])

        #: `str` api path
        self._context = urlparse.urljoin(ATSD_CONF['url'], 'api/v1/')
        #: `dict` interval -> step
        self._interval_schema = _get_interval_schema(self._node)

        log.info('[AtsdReader] init: entity=' + unicode(entity)
                 + ' metric=' + unicode(metric)
                 + ' tags=' + unicode(tags)
                 + ' url=' + unicode(ATSD_CONF['url']))

    def fetch(self, start_time, end_time):
        """ fetch time series

        :param start_time: `Number` seconds
        :param end_time: `Number` seconds
        """

        log.info(
            '[AtsdReader] fetching:  start_time={:f} end_time= {:f}'
            .format(start_time, end_time)
        )

        if self.default_step:
            step = self.default_step
        else:
            step = self._get_appropriate_step(start_time, end_time)

        series = self._query_series(start_time, end_time, step)

        log.info('[AtsdReader] get series of {:d} samples'.format(len(series)))

        if not len(series):
            return (start_time, end_time, end_time - start_time), [None]
        if len(series) == 1:
            return (start_time, end_time, end_time - start_time), [series[0]['v']]

        if step:
            # data regularized, send as is
            time_info = (float(series[0]['t']) / 1000,
                         float(series[-1]['t']) / 1000 + step,
                         step)

            values = [sample['v'] for sample in series]
        else:
            time_info, values = _regularize(series)

        log.info('[AtsdReader] fetched {:d} values'.format(len(values)))

        return time_info, values

    def _get_appropriate_step(self, start_time, end_time):
        """find step for current interval using interval schema
        return step of lowest schema interval that bigger than current

        :param start_time: `Number` seconds
        :param end_time: `Number` seconds
        :return: step `Number` seconds
        """
        interval = end_time - start_time

        intervals = self._interval_schema.keys()
        intervals.sort()

        step = 0
        for i in intervals:
            step = self._interval_schema[i]

            if interval < i:
                break

        return step

    def _query_series(self, start_time, end_time, step):
        """
        :param start_time: `Number` seconds
        :param end_time: `Number` seconds
        :param step: `Number` seconds
        :return: series data [{t,v}]
        """

        tags_query = {}
        for key in self._node.tags:
            tags_query[key] = [self._node.tags[key]]

        data = {
            'queries': [
                {
                    'startTime': int(start_time * 1000),
                    'endTime': int(end_time * 1000),
                    'entity': self._node.entity,
                    'metric': self._node.metric,
                    'tags': tags_query
                }
            ]
        }

        if step:
            # request regularized data
            data['queries'][0]['aggregate'] = {
                'type': self.statistic,
                'interpolate': 'STEP',
                'interval': {'count': step, 'unit': 'SECOND'}
            }

        resp = self._request('POST', 'series', data)

        return resp['series'][0]['data']

    def get_intervals(self):
        """
        :return: :class:`.IntervalSet`
        """

        log.info('[AtsdReader] getting_intervals')

        metric = self._request('GET',
                               'metrics/' + urllib.quote(self._node.metric, ''))
        entity = self._request('GET',
                               'entities/' + urllib.quote(self._node.entity, ''))

        end_time = max(metric['lastInsertTime'], entity['lastInsertTime'])

        retention = metric['retentionInterval']
        start_time = (end_time - retention) if retention else 1

        return IntervalSet([Interval(start_time, end_time)])

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

        if response.status_code is not 200:
            raise RuntimeError('server response status_code='
                               + str(response.status_code))

        return response.json()
