import requests
import urlparse
import json

try:
    from graphite.logger import log
    from django.conf import settings
except:  # debug env
    from graphite import settings
    import default_logger as log


# statistics applicable for aggregate, but not for group
NON_GROUP_STATS = ('FIRST',
                   'LAST',
                   'DELTA',
                   'WAVG',
                   'WTAVG',
                   'THRESHOLD_COUNT',
                   'THRESHOLD_DURATION',
                   'THRESHOLD_PERCENT')


class Client(object):

    def __init__(self):
        #: :class:`.Session`
        self._session = requests.Session()
        self._session.auth = (settings.ATSD_CONF['username'],
                              settings.ATSD_CONF['password'])
        #: `str` api path
        self._context = urlparse.urljoin(settings.ATSD_CONF['url'], 'api/v1/')

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
        :return: series json
        """

        if aggregator and aggregator.unit == 'SECOND':
            step = aggregator.count
            start_time = (start_time // step) * step
            end_time = (end_time // step + 1) * step

        tags_query = {}
        for key in instance.tags:
            tags_query[key] = [instance.tags[key]]

        data = {
            'queries': [
                {
                    'startTime': int(start_time * 1000),
                    'endTime': int(end_time * 1000),
                    'entity': instance.entity_name,
                    'metric': instance.metric_name,
                    'tags': tags_query
                }
            ]
        }

        if aggregator and aggregator.type != 'DETAIL':
            # request regularized data
            if aggregator.type in NON_GROUP_STATS:
                data['queries'][0]['group'] = {"type": "SUM",
                                               "interval": {
                                                   "count": aggregator.count,
                                                   "unit": aggregator.unit}}
                data['queries'][0]['aggregate'] = aggregator.json()
            else:
                data['queries'][0]['group'] = aggregator.json()

        return self.request('POST', 'series', data)

