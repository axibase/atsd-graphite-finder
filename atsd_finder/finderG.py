# -*- coding: utf-8 -*-

import os

from .reader import AtsdReader
from .client import AtsdClient
import utils

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

    def log_info(self, message):

        log.info('[' + self.__class__.__name__ + ' ' + self.pid + '] ' + message)

    def log_exc(self, message):

        log.exception('[' + self.__class__.__name__ + ' ' + self.pid + '] ' + message)

    def _make_branch(self, path):

        self.log_info('Branch path = ' + path)

        return BranchNode(path)

    def _make_leaf(self, path):

        self.log_info('Leaf path = ' + path)

        metric, entity = utils.parse_path(path)

        reader = AtsdReader(self._client, entity, metric, {})

        return LeafNode(path, reader)

    def find_nodes(self, query):
        """
        :param query: :class: `.FindQuery'
        :return: `generator`<Node>
        """

        try:

            self.log_info('query = ' + query.pattern)

            response = self._client.query_graphite_metrics(query.pattern)
            self.log_info('response = ' + unicode(response))

            for metric in response['metrics']:

                if metric['is_leaf'] == 0:
                    yield self._make_branch(metric['path'][0:-1])
                else:
                    yield self._make_leaf(metric['path'])

        except StandardError as e:

            self.log_exc(unicode(e))
