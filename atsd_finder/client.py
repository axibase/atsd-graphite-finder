import requests
import urlparse
import json
from datetime import datetime

from . import utils

log = utils.get_logger()

try:
    # noinspection PyUnresolvedReferences
    from django.conf import settings
    from graphite.readers import FetchInProgress
except:  # debug env
    from graphite import settings
    from sample import FetchInProgress

# statistics applicable for aggregate, but not for group
NON_GROUP_STATS = ('FIRST',
                   'LAST',
                   'DELTA',
                   'WAVG',
                   'WTAVG',
                   'THRESHOLD_COUNT',
                   'THRESHOLD_DURATION',
                   'THRESHOLD_PERCENT')


class _FetchTimer(object):

    def __init__(self):
        self.waiting_fetches = 0
        self.time = None  # datetime

    def inc_fetches(self):
        if self.time is None:
            self.time = datetime.now()

        self.waiting_fetches += 1

    def dec_fetches(self):
        if self.waiting_fetches == 0:
            raise RuntimeError('_FetchTimer.waiting_fetches < 0')

        self.waiting_fetches -= 1

        if self.waiting_fetches == 0:
            fetch_duration = datetime.now() - self.time
            self.time = None

            log.info('fetch_duration=' + str(fetch_duration), self)


def _get_retention_interval(metric):
    days = metric['retentionInterval']

    return days * 24 * 60 * 60


class Instance(object):
    """
    series unique identifier
    """

    __slots__ = ('entity_name', 'metric_name', 'tags', '_client', 'path')

    def __init__(self, entity_name, metric_name, tags, path, client):
        #: `str`
        self.entity_name = entity_name
        #: `str`
        self.metric_name = metric_name
        #: `dict`
        self.tags = tags
        #: `str`
        self.path = path
        #: :class:`.AtsdClient`
        self._client = client

    def get_retention_interval(self):
        """
        :return: `Number` seconds or `None` if no default interval
        """
        try:
            return self._client.metric_intervals[self.metric_name]
        except KeyError:
            return None

    def fetch_series(self, start_time, end_time, aggregator):
        """send query

        :param start_time: `Number` seconds
        :param end_time: `Number` seconds
        :param aggregator: :class:`.Aggregator` | None
        :return: :class: `.FetchInProgress` <(start, end, step), [values]>
        """

        self._client.fetch_timer.inc_fetches()

        future = self._client.query_series(self, start_time, end_time, aggregator)

        inst = self

        def get_formatted_series():
            """get real values and regularize them

            :return: time_info, values
            """
            resp = future.waitForResults()
            series = resp['data']
            aggregated = False

            if not len(series):
                time_info = start_time, end_time, end_time - start_time
                values = [None]

            elif len(series) == 1:
                time_info = start_time, end_time, end_time - start_time
                values = [series[0]['v']]

            elif aggregator and aggregator.unit == 'SECOND':
                    # data regularized, send as is
                    time_info = (float(series[0]['t']) / 1000,
                                 float(series[-1]['t']) / 1000 + aggregator.count,
                                 aggregator.count)

                    values = [sample['v'] for sample in series]
                    aggregated = True

            else:
                time_info, values = utils.regularize(series)

            log.info('fetched {0} {1} values, interval={2} - {3}, step={4}sec'
                     .format(len(series),
                             'aggregated' if aggregated else 'raw',
                             utils.strf_timestamp(time_info[0]),
                             utils.strf_timestamp(time_info[1]),
                             time_info[2]),
                     'AtsdReader:' + str(id(inst)))

            self._client.fetch_timer.dec_fetches()

            return time_info, values

        return FetchInProgress(get_formatted_series)

    def get_metric(self):
        """make meta api request

        :return: parsed json response
        """

        return self._client.request('GET',
                                    'metrics/' + utils.quote(self.metric_name))

    def get_entity(self):
        """make meta api request

        :return: parsed json response
        """

        return self._client.request('GET',
                                    'entities/' + utils.quote(self.entity_name))


class QueryCollection(object):
    """store queries and responses for them
    each query has unique id, stored in requestId attr

    """

    def __init__(self):
        self._queries = {}
        self._responses = {}
        self._counter = 0

    def get_waiting_queries(self):
        waiting_queries = []
        for id_ in self._queries:
            if id_ not in self._responses:
                waiting_queries.append(self._queries[id_])

        return waiting_queries

    def add_response(self, response):
        """add response for existing query

        :param response: json
        """
        try:
            id_ = response['requestId']
        except KeyError:
            log.info('response without requestId: ' + unicode(response), self)
            return False

        if id_ in self._queries:
            self._responses[id_] = response
            return True

        log.info('no query for response: ' + unicode(response), self)
        return False

    def add_query(self, query):
        """
        :param query:  json
        :returns: unique id for query
        """

        self._counter += 1
        id_ = str(self._counter)

        query['requestId'] = id_

        self._queries[id_] = query

        # log.info('add query total=' + str(len(self._queries)), self)

        return id_

    def pop_response(self, query):
        """return and remove query entry, or return None if no response exists

        :param query: json
        :return: response or None
        :raises KeyError: if no such query
        """

        try:
            id_ = query['requestId']
        except KeyError:
            raise KeyError('no such query to get response')

        if id_ in self._responses:
            resp = self._responses[id_]
            del self._responses[id_]
            del self._queries[id_]

            # log.info('pop response total=' + str(len(self._responses)), self)
            return resp
        else:
            return None


class AtsdClient(object):
    def __init__(self):
        log.info('init', self)

        #: :class:`.Session`
        self._session = requests.Session()
        self._session.auth = (settings.ATSD_CONF['username'],
                              settings.ATSD_CONF['password'])
        #: `str` api path
        self._context = urlparse.urljoin(settings.ATSD_CONF['url'], 'api/v1/')

        self._query_storage = QueryCollection()

        #: metric_name: `str` -> retention_interval sec: `Number`
        self.metric_intervals = {}

        self.fetch_timer = _FetchTimer()

    def request(self, method, path, data=None, params=None):
        """
        :param params: `dict` query parameters
        :param method: `str`
        :param path: `str` url after 'api/v1'
        :param data: `dict` or `list` json body of request
        :return: `dict` or `list` response.json()
        :raises RuntimeError: server response not 200
        """

        request = requests.Request(
            method=method,
            url=urlparse.urljoin(self._context, path),
            params=params,
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

        log.info('request: duration = ' + str(response.elapsed)
                 + ', response-size = ' + str(len(response.content)),
                 self)

        if response.status_code != 200:
            raise RuntimeError('server response status_code={:d} {:s}'
                               .format(response.status_code, response.text))

        return response.json()

    def query_series(self, instance, start_time, end_time, aggregator):
        """
        :param instance: :class:`.Instance`
        :param start_time: `Number` seconds
        :param end_time: `Number` seconds
        :param aggregator: :class:`.Aggregator` | None
        :return: :class: `.FetchInProgress` <series json>
        """

        if aggregator and aggregator.unit == 'SECOND':
            step = aggregator.count
            start_time = (start_time // step) * step
            end_time = (end_time // step + 1) * step

        tags_query = {}
        for key in instance.tags:
            tags_query[key] = [instance.tags[key]]

        query = {'startTime': int(start_time * 1000),
                 'endTime': int(end_time * 1000),
                 'entity': instance.entity_name,
                 'metric': instance.metric_name,
                 'tags': tags_query}

        if aggregator and aggregator.type != 'DETAIL':
            # request regularized data
            if aggregator.type in NON_GROUP_STATS:
                query['group'] = {"type": "SUM",
                                  "interval": {"count": aggregator.count,
                                               "unit": aggregator.unit}}
                query['aggregate'] = aggregator.json()
            else:
                query['group'] = aggregator.json()

        self._query_storage.add_query(query)
        return FetchInProgress(lambda: self._get_response(query))

    @staticmethod
    def query_graphite_metrics(query, series, limit):
        """
        :param limit: `Number` response size
        :param query: `str` dot separated metric pattern
        :param series: `boolean` series query parameter
        :return: `json`
        """
        client = AtsdClient()

        params = {'query': query,
                  'format': 'completer',
                  'series': 'true' if series else 'false'}

        if limit is not None:
            params['limit'] = str(limit)

        resp = client.request('GET', 'graphite', params=params)

        if series:
            client._update_intervals(resp)

            for metric in resp['metrics']:
                if metric['is_leaf'] == 1:
                    series = metric['series']
                    metric['instance'] = Instance(series['entity'],
                                                  series['metric'],
                                                  series['tags'],
                                                  metric['path'],
                                                  client)

        return resp

    def _update_intervals(self, graphite_resp):
        """update self.metric_intervals

        :param graphite_resp: `json` atsd /graphite query response
        """
        metric_names = set()
        for metric in graphite_resp['metrics']:
            if metric['is_leaf'] == 1:
                m = metric['series']['metric']
                metric_names.add(m)

        # TODO: request retention interval

        # expression = utils.quote("name in ('" + "','".join(metric_names) + "')")
        #
        # log.info('request metrics, expression=' + expression, self)
        # metrics = self.request('GET', 'metrics?expression=' + expression)
        # log.info('update intervals for metrics {0}'
        #          .format([m['name'] for m in metrics]), self)

        for metric in metric_names:
            self.metric_intervals[metric] = 0

    def _get_response(self, query):
        """search response in _query_storage if not found make request

        :param query: json
        :return: json
        :raises KeyError: no such query in storage
        """

        response = self._query_storage.pop_response(query)

        if response is None:
            self._request_series()
            return self._query_storage.pop_response(query)

        return response

    def _request_series(self):
        """create batch request with queries in storage,
        add responses to storage
        """
        queries = self._query_storage.get_waiting_queries()
        data = {'queries': queries}

        # with open('/tmp/graphite-last-query.txt', 'w') as f:
        #     f.write(json.dumps(queries))

        log.info('batch request: ' + str(len(queries)) + ' queries', self)
        responses = self.request('POST', 'series', data)['series']
        log.info('batch response: ' + str(len(responses)) + ' series', self)

        # with open('/tmp/graphite-last-response.txt', 'w') as f:
        #     f.write(json.dumps(responses))

        for resp in responses:
            self._query_storage.add_response(resp)
