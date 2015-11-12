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


def _get_retention_interval(metric):
    end = metric['lastInsertTime'] / 1000
    days = metric['retentionInterval']

    if days == 0:
        start = 0
    else:
        start = end - days * 24 * 60 * 60
    return start, end


def _parse_path(path):
    """convert graphite metric to atsd metric-entity tuple

    :param path: `str`
    :return: (metric: `str`, entity: `str`)
    """

    metric_entity = path.split('.', 1)

    metric = metric_entity[0]

    if len(metric_entity) == 1:
        entity = '*'
    else:
        entity = metric_entity[1]

    metric = 'graphite_' + metric

    return metric, entity


class AtsdFinderG(object):

    def __init__(self):

        self._client = AtsdClient()

        try:
            # noinspection PyUnresolvedReferences
            self.pid = unicode(os.getppid()) + ':' + unicode(os.getpid())
        except AttributeError:
            self.pid = unicode(os.getpid())

        self.log_info('init')

        #: metric_name: `str` -> retention_interval: (`Number`, `Number`)
        self._metric_intervals = {}

    def log_info(self, message):

        log.info('[' + self.__class__.__name__ + ' ' + self.pid + '] ' + message)

    def log_exc(self, message):

        log.exception('[' + self.__class__.__name__ + ' ' + self.pid + '] ' + message)

    def _make_branch(self, path):

        self.log_info('Branch path = ' + path)

        return BranchNode(path)

    def _make_leaf(self, path):

        self.log_info('Leaf path = ' + path)

        metric, entity = _parse_path(path)

        retention_interval = self._metric_intervals[metric]
        reader = AtsdReader(self._client, entity, metric, {},
                            retention_interval=retention_interval)

        return LeafNode(path, reader)

    def _update_intervals(self, graphite_resp):
        """update self._metric_intervals

        :param graphite_resp: `json` atsd /graphite query response
        """
        metric_names = set()
        for metric in graphite_resp['metrics']:
            if metric['is_leaf']:
                m, _ = _parse_path(metric['path'])
                metric_names.add(m)

        expression = utils.quote("name in ('" + "','".join(metric_names) + "')")

        metrics = self._client.request('GET', 'metrics?expression=' + expression)
        self.log_info('update intervals for metrics {0}'
                      .format([m['name'] for m in metrics]))

        for metric in metrics:
            self._metric_intervals[metric['name']] = _get_retention_interval(metric)

    def find_nodes(self, query):

        try:

            self.log_info('query = ' + query.pattern)

            path = 'graphite?query=' + query.pattern + '&format=completer'
            self.log_info('request_url = ' + path)

            response = self._client.request('GET', path)
            self.log_info('response = ' + unicode(response))

            self._update_intervals(response)

            for metric in response['metrics']:

                if metric['is_leaf'] == 0:
                    yield self._make_branch(metric['path'][0:-1])
                else:
                    yield self._make_leaf(metric['path'])

        except StandardError as e:

            self.log_exc(unicode(e))
