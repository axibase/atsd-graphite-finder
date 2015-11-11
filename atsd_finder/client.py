import requests
import urlparse
import json
import random

try:
    from graphite.logger import log
    from django.conf import settings
    from graphite.readers import FetchInProgress
except:  # debug env
    from graphite import settings
    import default_logger as log
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


class QueryStorage(object):
    """store queries and responses for them
    each query has unique id, stored in requestId attr

    """

    def __init__(self):
        self._queries = {}
        self._responses = {}

    def get_waiting_queries(self):
        waiting_queries = []
        for id_ in self._queries:
            if not id_ in self._responses:
                waiting_queries.append(self._queries[id_])

        return waiting_queries

    def add_response(self, response):
        """add response for existing query

        :param response: json
        """
        try:
            id_ = response['requestId']
        except KeyError:
            log.info('response without requestId: ' + unicode(response))
            return False

        if id_ in self._queries:
            self._responses[id_] = response
            return True

        log.info('no query for response: ' + unicode(response))
        return False

    def add_query(self, query):
        """
        :param query:  json
        """
        id_ = str(random.randint(0, 999999))
        while id_ in self._queries:
            id_ = str(random.randint(0, 999999))

        query['requestId'] = str(id_)

        self._queries[id_] = query

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
            return resp
        else:
            return None


class AtsdClient(object):

    def __init__(self):
        #: :class:`.Session`
        self._session = requests.Session()
        self._session.auth = (settings.ATSD_CONF['username'],
                              settings.ATSD_CONF['password'])
        #: `str` api path
        self._context = urlparse.urljoin(settings.ATSD_CONF['url'], 'api/v1/')

        self._query_storage = QueryStorage()

    def request(self, method, path, data=None):
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

        responses = self.request('POST', 'series', data)['series']
        log.info('batch request, length=' + str(len(queries)))

        for resp in responses:
            self._query_storage.add_response(resp)