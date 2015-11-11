# -*- coding: utf-8 -*-

import requests
import os

from graphite.local_settings import ATSD_CONF
from .utils import quote, metric_quote, unquote

from .reader import AtsdReader, Aggregator
from .client import AtsdClient
try:
    from graphite.logger import log
except:
    import default_logger as log

from graphite.node import BranchNode, LeafNode


class AtsdFinderG(object):

    def __init__(self):

        self._client = AtsdClient()

        try:
            # noinspection PyUnresolvedReferences
            self.pid = unicode(os.getppid()) + ':' + unicode(os.getpid())
        except AttributeError:
            self.pid = unicode(os.getpid())

        self.log_info('init')

        self.url_base = ATSD_CONF['url'] + '/api/v1'
        self.auth = (ATSD_CONF['username'], ATSD_CONF['password'])

    def log_info(self, message):

        log.info('[' + self.__class__.__name__ + ' ' + self.pid + '] ' + message)

    def log_exc(self, message):

        log.exception('[' + self.__class__.__name__ + ' ' + self.pid + '] ' + message)

    def _make_branch(self, path):

        self.log_info('Branch path = ' + path)

        return BranchNode(path)

    def _make_leaf(self, path):

        self.log_info('Leaf path = ' + path)

        metric_entity = path.split('.', 1)

        if len(metric_entity) == 1:
            metric = metric_entity
            entity = '*'
        else:
            metric = metric_entity[0]
            entity = metric_entity[1]

        reader = AtsdReader(self._client, entity, metric, {}, None)

        self.log_info('Leaf path = ' + path)

        return LeafNode(path, reader)

    def find_nodes(self, query):

        try:

            self.log_info('query = ' + query.pattern)

            url = self.url_base + '/graphite?query=' + query.pattern + '&format=completer'
            self.log_info('request_url = ' + url)

            response = requests.get(url, auth=self.auth)
            self.log_info('status = ' + unicode(response.status_code))
            self.log_info('json = ' + unicode(response.json()))

            for metric in response.json()['metrics']:

                if metric['is_leaf'] == 0:
                    yield self._make_branch(metric['path'][0:-1])
                else:
                    yield self._make_leaf(metric['path'])

        except StandardError as e:

            self.log_exc(unicode(e))