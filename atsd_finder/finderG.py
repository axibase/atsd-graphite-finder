# -*- coding: utf-8 -*-

import time

from .reader import AtsdReader, EmptyReader
from .client import AtsdClient
from . import utils

from graphite.node import BranchNode, LeafNode

log = utils.get_logger()


class AtsdFinderG(object):

    def __init__(self):

        log.info('init', self)

    def _make_branch(self, path):

        #log.info('Branch path = ' + path, self)

        return BranchNode(path)

    def _make_leaf(self, path, instance=None):

        #log.info('Leaf path = ' + path, self)

        if instance is None:

            return LeafNode(path, EmptyReader())

        else:

            try:

                reader = AtsdReader(instance)

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

            if query.pattern == '' or '(' in query.pattern:

                raise StopIteration

            elif query.startTime is None:

                if '*' in query.pattern[:-1] or (len(query.pattern) > 1 and query.pattern[-2] != '.'):
                    log.info('auto-complete query', self)
                    limit = 100
                else:
                    limit = None

                response = AtsdClient.query_graphite_metrics(query.pattern, False, limit)
                log.info('response', self)

                limit = float('inf') if limit is None else limit

                start_time = time.time()

                for i, metric in enumerate(response['metrics']):

                    if i >= limit:
                        break

                    if metric['is_leaf'] == 0:
                        yield self._make_branch(metric['path'][0:-1])
                    else:
                        yield self._make_leaf(metric['path'])

                log.info('tree ready in ' + ('%.2f' % (time.time() - start_time)) + 's', self)

            else:

                response = AtsdClient.query_graphite_metrics(query.pattern, True, None)
                log.info('response', self)

                start_time = time.time()

                for metric in response['metrics']:

                    if metric['is_leaf'] == 0:
                        yield self._make_branch(metric['path'][0:-1])
                    else:
                        yield self._make_leaf(metric['path'], metric['instance'])

                log.info('tree ready in ' + ('%.2f' % (time.time() - start_time)) + 's', self)

        except StandardError as e:

            log.exception(unicode(e), self)
