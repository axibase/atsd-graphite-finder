# -*- coding: utf-8 -*-

import os
import time

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

        log.info('init', self)

    def _make_branch(self, path):

        #log.info('Branch path = ' + path, self)

        return BranchNode(path)

    def _make_leaf(self, path, series, client):

        #log.info('Leaf path = ' + path, self)

        if series is None:

            return LeafNode(path, EmptyReader())

        else:

            try:

                reader = AtsdReader(client, series['entity'], series['metric'], series['tags'])

                return LeafNode(path, reader)

            except StandardError as e:

                log.exception(unicode(e), self)

    def find_nodes(self, query):
        """
        :param query: :class: `.FindQuery'
        :return: `generator`<Node>
        """

        log.info('query = ' + unicode(query.__dict__), self)

        try:

            if query.pattern == '':

                raise StopIteration

            elif query.startTime is None:

                response = AtsdClient().query_graphite_metrics(query.pattern, False)
                log.info('response', self)

                start_time = time.time()

                for metric in response['metrics']:

                    if metric['is_leaf'] == 0:
                        yield self._make_branch(metric['path'][0:-1])
                    else:
                        yield self._make_leaf(metric['path'], None, None)

                log.info('tree ready in ' + ('%.2f' % (time.time - start_time)) + 's', self)

            else:

                client = AtsdClient()

                response = client.query_graphite_metrics(query.pattern, True)
                log.info('response', self)

                start_time = time.time()

                for metric in response['metrics']:

                    if metric['is_leaf'] == 0:
                        yield self._make_branch(metric['path'][0:-1])
                    else:
                        yield self._make_leaf(metric['path'], metric['series'], client)

                log.info('tree ready in ' + ('%.2f' % (time.time - start_time)) + 's', self)

        except StandardError as e:

            log.exception(unicode(e), self)
