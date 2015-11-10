import requests
import urlparse
import json

try:
    from graphite.logger import log
    from django.conf import settings
except:  # debug env
    from graphite import settings
    import default_logger as log


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
