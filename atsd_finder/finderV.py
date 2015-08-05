# -*- coding: utf-8 -*-

import requests
import json
import fnmatch
import os
import copy

from graphite.local_settings import ATSD_CONF
from .utils import quote, metric_quote, unquote

from .reader import AtsdReader, Aggregator
try:
    from graphite.logger import log
except:
    import default_logger as log

from graphite.node import BranchNode, LeafNode


class AtsdFinderV(object):

    def __init__(self):

        self.name = '[AtsdFinderV]'

        try:
            # noinspection PyUnresolvedReferences
            self.pid = unicode(os.getppid()) + ':' + unicode(os.getpid())
        except AttributeError:
            self.pid = unicode(os.getpid())

        self.log_info('init')

        self.url_base = ATSD_CONF['url'] + '/api/v1'
        self.auth = (ATSD_CONF['username'], ATSD_CONF['password'])

        try:
            self.views =  ATSD_CONF['views']
        except:
            self.views = {}

    def log_info(self, message):

        log.info('[' + self.__class__.__name__ + ' ' + self.pid + '] ' + message)

    def log_exc(self, message):

        log.exception('[' + self.__class__.__name__ + ' ' + self.pid + '] ' + message)

    def get_info(self, pattern, leaf_request):

        info = {
            'valid': True,
            'tags': {}
        }

        tokens = pattern.split('.')

        try:
            tokens[:] = [unquote(token) for token in tokens] if pattern != '' else []
        except:
            info['valid'] = False
            return info

        self.log_info('tokens = ' + unicode(tokens))

        info['tokens'] = len(tokens)

        if len(tokens) == 0:
            return info

        try:
            view = self.views[tokens[0]]
        except:
            info['valid'] = False
            return info

        info['view'] = tokens[0]

        for i, token in enumerate(tokens[1:]):

            level = view[i]

            if 'global' in level:
                info = self.extract_var(info, level['global'])

            token_type = level['type']
            token_desc = level['value']

            if len(token) == 0:
                continue

            if token[0] == '[' and ']' in  token:
                prefix = token[token.find('[')+1:token.find(']')]
                token = token[token.find(']')+2:]
            else:
                prefix = ''

            if token_type == 'collection':

                for desc in level['value']:

                    is_leaf = desc['is leaf'] if 'is leaf' in desc else False
                    desc_prefix = desc['prefix'] if 'prefix' in desc else ''

                    if (is_leaf == leaf_request or i != info['tokens'] - 1) \
                        and prefix == desc_prefix:

                        if 'global' in desc:
                            info = self.extract_var(info, desc['global'])

                        token_type = desc['type']
                        token_desc = desc['value']

                        break

            if token_type in ['view', 'entity', 'metric']:

                info[token_type] = token

            elif token_type in ['entity folder', 'metric folder', 'aggregator']:

                for cell in token_desc:

                    if token == cell.values()[0]:

                        info[token_type] = cell.keys()[0]
                        break

            elif token_type == 'tag':

                tag_values = token.split(', ')

                for j, tag_name in enumerate(token_desc):

                    tag_value = tag_values[j]
                    info['tags'][tag_name] = tag_value

            elif token_type in ['period', 'interval']:

                for time_dict in token_desc:

                    if token == time_dict['label']:

                        if time_dict['count'] != 0:
                            info[token_type] = time_dict
                        else:
                            info[token_type] = None

                        break

        return info

    def extract_var(self, info, scope):

        for token in scope:

            token_type = token['type']
            token_value = token['value'][0]

            if token_type != 'tag':

                info[token_type] = token_value

            else:

                for tag_name in token_value:
                    tag_value = token_value[tag_name]
                    info['tags'][tag_name] = tag_value

        self.log_info(unicode(info) + ' ' + unicode(scope))
        return info

    def make_branch(self, path):

        self.log_info('Branch path = ' + path)

        return BranchNode(path)

    def make_leaf(self, path, info):

        self.log_info('Leaf path = ' + path)

        entity = info['entity'] if 'entity' in info else '*'
        metric = info['metric']
        tags = info['tags']
        interval = info['interval'] if 'interval' in info else None

        if not 'period' in info or info['period'] == None:
            reader = AtsdReader(entity, metric, tags, interval)
        else:
            period_count = info['period']['count']
            period_unit = info['period']['unit']
            aggregator = info['aggregator'].upper() if 'aggregator' in info else 'AVG'
            reader = AtsdReader(entity, metric, tags, interval,
                                Aggregator(aggregator, period_count, period_unit))

        return LeafNode(path, reader)

    def find_nodes(self, query):

        try:

            self.log_info('query = ' + query.pattern)

            if len(query.pattern) == 0:
                raise StopIteration

            pattern_match = query.pattern

            if query.pattern[-1] == '*':

                leaf_request = False

                if len(query.pattern) == 1 or query.pattern[-2] == '.':
                    pattern = query.pattern[:-2]
                else:
                    if '.' in query.pattern:
                        pattern = query.pattern.rsplit('.', 1)[0]
                    else:
                        pattern = ''

            else:

                leaf_request = True
                pattern = query.pattern

            g_info = self.get_info(pattern, leaf_request)
            self.log_info('initial info = ' + json.dumps(g_info))

            if not g_info['valid']:
                raise StopIteration

            if g_info['tokens'] == 0:

                for view_name in self.views:

                    path = metric_quote(view_name)

                    if fnmatch.fnmatch(path, pattern_match):
                        yield self.make_branch(path)

            else:

                view = self.views[g_info['view']]

                ind = g_info['tokens'] - 1
                length = len(view)

                if ind > length:

                    raise StopIteration

                elif ind < length and not leaf_request:

                    level = view[g_info['tokens'] - 1]
                    self.log_info('level = ' + unicode(level))

                    level_type = level['type']
                    level_value = level['value']

                    if 'global' in level:
                        g_info = self.extract_var(g_info, level['global'])

                    self.log_info('global info = ' + json.dumps(g_info))

                    tokens = []

                    if level_type == 'collection':
                        for token in level_value:
                            tokens.append(token)
                    else:
                        tokens.append(level)

                    self.log_info('descs = ' + unicode(tokens))

                    for token in tokens:

                        info = copy.deepcopy(g_info)

                        if 'local' in token:
                            info = self.extract_var(info, token['local'])

                        self.log_info('local info = ' + unicode(info))

                        token_type = token['type']
                        token_value = token['value']
                        is_leaf = token['is leaf'] if 'is leaf' in token else False

                        prefix = '[' + token['prefix'] + '] ' if 'prefix' in token else ''

                        if token_type == 'const':

                            for string in token_value:

                                path = pattern + '.' + metric_quote(prefix + string)

                                if fnmatch.fnmatch(path, pattern_match):

                                    if not is_leaf:
                                        yield self.make_branch(path)
                                    elif 'metric' in info:
                                        yield self.make_leaf(path, info)

                        elif token_type in ['entity folder', 'metric folder']:

                            for folder_dict in token_value:

                                folder = folder_dict.keys()[0]

                                if token_type not in info \
                                   or token_type in info and fnmatch.fnmatch(folder, info[token_type]):

                                    path = pattern + '.' + metric_quote(prefix + folder_dict[folder])

                                    if fnmatch.fnmatch(path, pattern_match):

                                        if not is_leaf:
                                            yield self.make_branch(path)
                                        elif 'metric' in info:
                                            yield self.make_leaf(path, info)

                        elif token_type == 'entity':

                            folders = []

                            for expr in token_value:

                                if expr != '*':
                                    if 'entity folder' not in info or fnmatch.fnmatch(expr, info['entity folder']):
                                        folder = expr
                                    else:
                                        folder = ''
                                elif 'entity folder' in info:
                                    folder = info['entity folder']
                                else:
                                    folder = '*'

                                if not folder in ['']:
                                    folders.append(folder)

                            if '*' in folders:
                                folders = ['*']

                            if not is_leaf and not 'metric' in info:

                                expressions = ['name%20like%20%27' + quote(folder) + '%27' for folder in folders]
                                tail = '?expression=' + '%20or%20'.join(expressions)

                                url = self.url_base + '/entities' + tail
                                self.log_info('request_url = ' + url)

                                response = requests.get(url, auth=self.auth)
                                self.log_info('status = ' + unicode(response.status_code))

                                for entity in response.json():

                                    path = pattern + '.' + metric_quote(prefix + entity['name'])

                                    if fnmatch.fnmatch(path, pattern_match):
                                        yield self.make_branch(path)

                            elif 'metric' in info:

                                url = self.url_base + '/metrics/' + quote(info['metric'])+ '/entity-and-tags'
                                self.log_info('request_url = ' + url)

                                response = requests.get(url, auth=self.auth)
                                self.log_info('status = ' + unicode(response.status_code))

                                entities = set()

                                for combo in response.json():

                                    entities.add(combo['entity'])

                                for entity in entities:

                                    matches = False

                                    for folder in folders:
                                        if fnmatch.fnmatch(entity, folder):
                                            matches = True
                                            break

                                    if not matches:
                                        continue

                                    path = pattern + '.' + metric_quote(prefix + entity)

                                    if fnmatch.fnmatch(path, pattern_match):

                                        if not is_leaf:
                                            yield self.make_branch(path)
                                        else:
                                            info['entity'] = entity
                                            yield self.make_leaf(path, info)

                        elif token_type == 'metric':

                            folders = []

                            for expr in token_value:

                                if expr != '*':
                                    if 'metric folder' not in info or fnmatch.fnmatch(expr, info['metric folder']):
                                        folder = expr
                                    else:
                                        folder = ''
                                elif 'metric folder' in info:
                                    folder = info['metric folder']
                                else:
                                    folder = '*'

                                if folder != '':
                                    folders.append(folder)

                            if '*' in folders:
                                folders = ['*']

                            expressions = ['name%20like%20%27' + quote(folder) + '%27' for folder in folders]
                            tail = '?expression=' + '%20or%20'.join(expressions)

                            if not 'entity' in info:
                                url = self.url_base + '/metrics'
                            else:
                                url = self.url_base + '/entities/' + quote(info['entity'])+ '/metrics'

                            url = url + tail
                            self.log_info('request_url = ' + url)

                            response = requests.get(url, auth=self.auth)
                            self.log_info('status = ' + unicode(response.status_code))

                            for metric in response.json():

                                path = pattern + '.' + metric_quote(prefix + metric['name'])

                                if fnmatch.fnmatch(path, pattern_match):

                                    if not is_leaf:
                                        yield self.make_branch(path)
                                    else:
                                        info['metric'] = metric['name']
                                        yield self.make_leaf(path, info)

                        elif token_type == 'tag':

                            if 'metric' in info:

                                url = self.url_base + '/metrics/' + quote(info['metric'])+ '/entity-and-tags'
                                self.log_info('request_url = ' + url)

                                response = requests.get(url, auth=self.auth)
                                self.log_info('status = ' + unicode(response.status_code))

                                tag_combos = []

                                for combo in response.json():

                                    tags = combo['tags']

                                    if 'entity' in info and info['entity'] != combo['entity']:
                                        continue

                                    contains = True

                                    for tag_name in token_value:
                                        if not tag_name in tags:
                                            contains = False
                                            break

                                    if not contains:
                                        continue

                                    matches = True

                                    for tag_name in tags:
                                        if tag_name in info['tags'] and info['tags'][tag_name] != tags[tag_name]:
                                            matches = False
                                            break

                                    tag_combo = {}
                                    tag_values = []

                                    for tag_name in token_value:
                                        tag_combo[tag_name] = tags[tag_name]
                                        tag_values.append(tags[tag_name])

                                    if matches and not tag_combo in tag_combos:

                                        tag_combos.append(tag_combo)

                                        path = pattern + '.' + metric_quote(prefix + ', '.join(tag_values))

                                        if fnmatch.fnmatch(path, pattern_match):

                                            if not is_leaf:
                                                yield self.make_branch(path)
                                            else:
                                                t_info = copy.deepcopy(info)
                                                t_info['tags'].update(tag_combo)
                                                yield self.make_leaf(path, t_info)

                        elif token_type == 'aggregator':

                            for aggregator_dict in token_value:

                                aggregator = aggregator_dict.keys()[0]

                                path = pattern + '.' + metric_quote(prefix + aggregator_dict[aggregator])

                                if fnmatch.fnmatch(path, pattern_match):

                                    if not is_leaf:
                                        yield self.make_branch(path)
                                    elif 'metric' in info:
                                        info['aggregator'] = aggregator
                                        yield self.make_leaf(path, info)

                        elif token_type == 'period':

                            for period in token_value:

                                period_label = period['label']

                                path = pattern + '.' + metric_quote(prefix + period_label)

                                if fnmatch.fnmatch(path, pattern_match):

                                    if not is_leaf:
                                        yield self.make_branch(path)
                                    elif 'metric' in info:
                                        info['period'] = period if period['count'] != 0 else None
                                        yield self.make_leaf(path, info)

                        elif token_type == 'interval':

                            for interval in token_value:

                                interval_label = interval['label']

                                path = pattern + '.' + metric_quote(prefix + interval_label)

                                if fnmatch.fnmatch(path, pattern_match):

                                    if not is_leaf:
                                        yield self.make_branch(path)
                                    elif 'metric' in info:
                                        info['interval'] = interval if interval['count'] != 0 else None
                                        yield self.make_leaf(path, info)

                elif leaf_request:

                    if 'metric' in g_info:
                        yield self.make_leaf(pattern, g_info)

        except StandardError as e:

            self.log_exc(unicode(e))