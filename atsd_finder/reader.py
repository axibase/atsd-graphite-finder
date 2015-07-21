from graphite.intervals import Interval, IntervalSet
import requests
import json
import urlparse
import urllib
from graphite.local_settings import ATSD_CONF
try:
    from graphite.logger import log
except:
    import default_logger as log


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


class AtsdReader(object):
    __slots__ = ('entity_name',
                 'metric_name',
                 '_session',
                 '_context',
                 'tags',
                 'step',
                 'statistic')

    def __init__(self, entity, metric, tags, step, statistic='DETAIL'):
        #: `str` entity name
        self.entity_name = entity
        #: `str` metric name
        self.metric_name = metric
        #: `dict` tags, in format {`str`: `str`}
        self.tags = tags
        #: `Number` seconds, if 0 raw data
        self.step = step
        #: :class:`.AggregateType`
        self.statistic = statistic

        #: :class:`.Session`
        self._session = requests.Session()
        self._session.auth = (ATSD_CONF['username'],
                              ATSD_CONF['password'])

        #: `str` api path
        self._context = urlparse.urljoin(ATSD_CONF['url'], 'api/v1/')

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
            '[AtsdReader] fetching:  start_time={:f} end_time= {:f} step={:f}'
            .format(start_time, end_time, self.step)
        )

        series = self._query_series(start_time, end_time)

        log.info('[AtsdReader] get series of {:d} samples'.format(len(series)))

        if not len(series):
            return (start_time, end_time, end_time - start_time), [None]
        if len(series) == 1:
            return (start_time, end_time, end_time - start_time), [series[0]['v']]

        if self.step:
            # data regularized, send as is
            time_info = (float(series[0]['t']) / 1000,
                         float(series[-1]['t']) / 1000 + self.step,
                         self.step)

            values = [sample['v'] for sample in series]
        else:
            time_info, values = _regularize(series)

        log.info('[AtsdReader] fetched {:d} values'.format(len(values)))

        return time_info, values

    def _query_series(self, start_time, end_time):

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

        if self.step:
            # request regularized data
            data['queries'][0]['aggregate'] = {
                'types': [self.statistic],
                'interpolate': 'STEP',
                'interval': {'count': self.step, 'unit': 'SECOND'}
            }

        resp = self._request('POST', 'series', data)

        return resp['series'][0]['data']

    def get_intervals(self):

        log.info('[AtsdReader] getting_intervals')

        metric = self._request('GET',
                               'metrics/' + urllib.quote(self.metric_name, ''))
        entity = self._request('GET',
                               'entities/' + urllib.quote(self.entity_name, ''))

        end_time = max(metric['lastInsertTime'], entity['lastInsertTime'])

        retention = metric['retentionInterval']
        start_time = (end_time - retention) if retention else 1

        return IntervalSet([Interval(start_time, end_time)])

    def _request(self, method, path, data=None):
        """
        :param method: `str`
        :param path: `str` after 'api/v1'
        :param data: `dict` or `list`
        :return: `dict` or `list` response.json()
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
