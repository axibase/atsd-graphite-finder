# -*- coding: utf-8 -*-

import os

from .reader import AtsdReader, EmptyReader
from .client import AtsdClient
import utils

from graphite.node import BranchNode, LeafNode

log = utils.get_logger()


class AtsdFinderG(object):

    def __init__(self):

        try:
            # noinspection PyUnresolvedReferences
            self.pid = unicode(os.getppid()) + ':' + unicode(os.getpid())
        except AttributeError:
            self.pid = unicode(os.getpid())

        self._log_info('init')

    def _log_info(self, message):

        log.info(message, self)

    def _log_exc(self, message):

        log.exception(message, self)

    def _make_branch(self, path):

        self._log_info('Branch path = ' + path)

        return BranchNode(path)

    def _make_leaf(self, path, series, client):

        self._log_info('Leaf path = ' + path)

        if series is None:

            return LeafNode(path, EmptyReader())

        else:

            try:

                reader = AtsdReader(client, series['entity'], series['metric'], series['tags'])

                return LeafNode(path, reader)

            except StandardError as e:

                self._log_exc(unicode(e))

    def find_nodes(self, query):
        """
        :param query: :class: `.FindQuery'
        :return: `generator`<Node>
        """

        self._log_info('query = ' + unicode(query.__dict__))

        try:

            if query.pattern == '':

                raise StopIteration

            elif query.startTime is None:

                response = AtsdClient().query_graphite_metrics(query.pattern, False)
                self._log_info('response = ' + unicode(response))

                for metric in response['metrics']:

                    if metric['is_leaf'] == 0:
                        yield self._make_branch(metric['path'][0:-1])
                    else:
                        yield self._make_leaf(metric['path'], None, None)

            else:

                client = AtsdClient()

                response = client.query_graphite_metrics(query.pattern, True)
                self._log_info('response = ' + unicode(response))

                for metric in response['metrics']:

                    if metric['is_leaf'] == 0:
                        yield self._make_branch(metric['path'][0:-1])
                    else:
                        yield self._make_leaf(metric['path'], metric['series'], client)

        except StandardError as e:

            self._log_exc(unicode(e))
