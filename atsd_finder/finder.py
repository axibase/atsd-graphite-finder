# -*- coding: utf-8 -*-

import requests
import urllib
import json
import fnmatch
import re
import os

from graphite.local_settings import ATSD_CONF
from .utils import quote, metric_quote

from .reader import AtsdReader, Aggregator
try:
    from graphite.logger import log
except:
    import default_logger as log
    
from graphite.node import BranchNode, LeafNode


class AtsdFinder(object):

    roots = {'entities', 'metrics'}
    periods = [1, 60, 3600, 86400]
    period_names = ['1 sec', '1 min', '1 hour', '1 day']

    def __init__(self):
    
        self.name = '[AtsdFinder]'

        try:
            # noinspection PyUnresolvedReferences
            self.pid = unicode(os.getppid()) + ':' + unicode(os.getpid())
        except AttributeError:
            self.pid = unicode(os.getpid())

        self.log('init')

        self.url_base = ATSD_CONF['url'] + '/api/v1'
        self.auth = (ATSD_CONF['username'], ATSD_CONF['password'])

        try:
            self.entity_folders = ATSD_CONF['entity_folders']
        except KeyError:
            self.entity_folders = 'abcdefghijklmnopqrstuvwxyz_'

        try:
            self.metric_folders = ATSD_CONF['metric_folders']
        except KeyError:
            self.metric_folders = 'abcdefghijklmnopqrstuvwxyz_'
            
        try:
            self.aggregators = ATSD_CONF['aggregators']
        except KeyError:
            self.aggregators = {
                'avg'               : 'Average',
                'min'               : 'Minimum',
                'max'               : 'Maximum',
                'sum'               : 'Sum',
                'count'             : 'Count',
                'first'             : 'First value',
                'last'              : 'Last value',
                'percentile_999'    : 'Percentile 99.9%',
                'percentile_99'     : 'Percentile 99%',
                'percentile_995'    : 'Percentile 99.5%',
                'percentile_95'     : 'Percentile 95%',
                'percentile_90'     : 'Percentile 90%',
                'percentile_75'     : 'Percentile 75%',
                'median'            : 'Median',
                'standard_deviation': 'Standard deviation',
                'delta'             : 'Delta',
                'wavg'              : 'Weighted average',
                'wtavg'             : 'Weighted time average',
            }
            
        self.aggregators = {v: k for k, v in self.aggregators.items()}

    def log(self, message):
    
        log.info('[' + self.__class__.__name__ + ' ' + self.pid + '] ' + message)
    
    def get_info(self, pattern):

        info = {
            'valid': True,
            'tags': {}
        }
            
        tokens = pattern.split('.')
        try:
            tokens[:] = [urllib.unquote(token) for token in tokens] if pattern != '' else []
        except:
            info['valid'] = False
            return info
        
        self.log('tokens = ' + unicode(tokens))
        
        info['tokens'] = len(tokens)

        if len(tokens) == 0:
            return info
        
        if not tokens[0] in ['entities', 'metrics']:
            info['valid'] = False
            return info
        
        if not tokens[0] in ['entities', 'metrics']:
            info['valid'] = False
            return info
            
        info['type'] = tokens[0]
        
        if info['tokens'] > 1:
            if info['type'] == 'entities':
                info['entity folder'] = tokens[1]
            else:
                info['metric folder'] = tokens[1]
        
        if info['tokens'] > 2:
            if info['type'] == 'entities':
                info['entity'] = tokens[2]
            else:
                info['metric'] = tokens[2]
        
        if info['tokens'] > 3:
            if info['type'] == 'entities':
                info['metric'] = tokens[3]
            else:
                info['entity'] = tokens[3]
        
        all_tags = True
        
        if info['tokens'] > 4:
        
            for i, token in enumerate(tokens[4:]):
            
                if token in ['detail', 'stats']:
                    all_tags = False
                    break
                
                tag = token.split(': ', 1)
                
                if len(tag) != 2:
                    info['valid'] = False
                    return info
                
                tag_name = tag[0]
                tag_value = tag[1]
            
                info['tags'][tag_name] = tag_value
        
            if not all_tags:

                i = i + 4

                self.log('detail token: ' + tokens[i])

                if tokens[i] == 'detail':
                    info['detail'] = True
                else:
                    info['detail'] = False

                if info['tokens'] > i + 1:
                    info['aggregator'] = self.aggregators[tokens[i + 1]]

                if info['tokens'] > i + 2:
                    info['period'] = self.periods[self.period_names.index(tokens[i + 2])]
        
        return info

    def find_nodes(self, query):
    
        self.log('query = ' + query.pattern)
        
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
        
        info = self.get_info(pattern)
        self.log(json.dumps(info))
        
        if not info['valid']:
            
            raise StopIteration

        if info['tokens'] == 0:

            for root in self.roots:
                
                if fnmatch.fnmatch(root, pattern_match):
                    # self.log('path = ' + root)
                    yield BranchNode(metric_quote(root))
        
        elif info['tokens'] == 1:

            if info['type'] == 'entities':
                for folder in self.entity_folders:
                    
                    path = pattern + '.' + metric_quote(folder)
                    
                    if fnmatch.fnmatch(path, pattern_match):
                        # self.log('path = ' + path)
                        yield BranchNode(path)

            elif info['type'] == 'metrics':
                for folder in self.metric_folders:

                    path = pattern + '.' + metric_quote(folder)
                    
                    if fnmatch.fnmatch(path, pattern_match):
                        # self.log('path = ' + path)
                        yield BranchNode(path)

        elif info['tokens'] == 2:
        
            if info['type'] == 'entities':
                folder = info['entity folder']
            else:  # info['type'] == 'metrics':
                folder = info['metric folder']

            if folder[0] == "_":
                other = True
                url = self.url_base + '/' + quote(info['type'])
            else:
                other = False
                url = self.url_base + '/' + quote(info['type']) + '?expression=name%20like%20%27' + quote(folder) + '*%27'

            self.log('request_url = ' + url + '')

            response = requests.get(url, auth=self.auth)

            #self.log('response = ' + response.text)
            self.log('status = ' + unicode(response.status_code))

            for smth in response.json():

                if not other:
                    
                    path = pattern + '.' + metric_quote(smth['name'])
                    
                    if fnmatch.fnmatch(path, pattern_match):
                        # self.log('path = ' + path)
                        yield BranchNode(path)

                else:

                    matches = False

                    if info['type'] == 'entities':
                        for folder in self.entity_folders:

                            if re.match(folder + '.*', smth['name']):

                                    matches = True
                                    break

                    elif info['type'] == 'metrics':
                        for folder in self.metric_folders:

                            if re.match(folder + '.*', smth['name']):

                                    matches = True
                                    break

                    if not matches:
                        
                        path = pattern + '.' + metric_quote(smth['name'])
                        
                        if fnmatch.fnmatch(path, pattern_match):
                            # self.log('path = ' + path)
                            yield BranchNode(path)

        elif info['tokens'] == 3:

            if info['type'] == 'entities':

                url = self.url_base + '/entities/' + quote(info['entity']) + '/metrics'
                self.log('request_url = ' + url)

                response = requests.get(url, auth=self.auth)

                #self.log('response = ' + response.text)
                self.log('status = ' + unicode(response.status_code))

                for metric in response.json():
                    
                    path = pattern + '.' + metric_quote( metric['name'])
                    
                    if fnmatch.fnmatch(path, pattern_match):
                        # self.log('path = ' + path)
                        yield BranchNode(path)

            elif info['type'] == 'metrics':

                url = self.url_base + '/metrics/' + quote(info['metric'])+ '/entity-and-tags'
                self.log('request_url = ' + url)

                response = requests.get(url, auth=self.auth)

                #self.log('response = ' + response.text)
                self.log('status = ' + unicode(response.status_code))

                entities = set()

                for entity in response.json():

                    entities.add(entity['entity'])

                for entity in entities:
                    
                    path = pattern + '.' + metric_quote(entity)
                    
                    if fnmatch.fnmatch(path, pattern_match):
                        # self.log('path = ' + path)
                        yield BranchNode(path)

        elif info['tokens'] > 3 and not 'detail' in info:

            entity = info['entity']
            metric = info['metric']

            tags = info['tags']

            url = self.url_base + '/metrics/' + quote(metric) + '/entity-and-tags'
            self.log('request_url = ' + url)

            response = requests.get(url, auth=self.auth)

            #self.log('response = ' + response.text)
            self.log('status = ' + unicode(response.status_code))

            tag_combos = []

            for combo in response.json():
                if combo['entity'] == entity:
                    tag_combos.append(combo['tags'])

            tag_names = set()

            for tag_combo in tag_combos:
                for tag_name in tag_combo:
                    tag_names.add(tag_name)

            tag_names = list(tag_names)
            tag_names.sort()

            true_tag_combos = []

            found = False
            
            labels = []

            for tag_combo in tag_combos:

                suitable = True

                for tag_name in tags:

                    if tag_name in tag_combo and tag_combo[tag_name] == tags[tag_name]:
                        pass
                    else:
                        suitable = False
                        break

                if suitable:
                    for tag_name in tag_names:
                        if not tag_name in tags and tag_name in tag_combo:

                            found = True

                            label = tag_name + ': ' + tag_combo[tag_name]
                            
                            if not label in labels:
                            
                                labels.append(label)
                            
                                path = pattern + '.' + metric_quote(tag_name + ': ' + tag_combo[tag_name])
                                
                                if fnmatch.fnmatch(path, pattern_match):
                                    # self.log('path = ' + path)
                                    yield BranchNode(path)
                                
                            break
                            
            if not found:
                
                path = pattern + '.' + metric_quote('detail')
                
                if fnmatch.fnmatch(path, pattern_match):
                    # self.log('path = ' + path)
                    reader = AtsdReader(entity, metric, tags)
                    yield LeafNode(path, reader)
                
                path = pattern + '.' + metric_quote('stats')
                
                if fnmatch.fnmatch(path, pattern_match):
                    # self.log('path = ' + path)
                    yield BranchNode(path)
                
        elif not 'aggregator' in info:
        
            if info['detail']:
            
                if fnmatch.fnmatch(pattern, pattern_match):
            
                    entity = info['entity']
                    metric = info['metric']
                    tags = info['tags']
                    
                    reader = AtsdReader(entity, metric, tags)
                    
                    yield LeafNode(pattern, reader)
            
            else:
            
                for aggregator in self.aggregators:
                    
                    path = pattern + '.' + metric_quote(aggregator)
                    
                    if fnmatch.fnmatch(path, pattern_match):
                        # self.log('path = ' + path)
                        yield BranchNode(path)
            
        elif not 'period' in info:

            entity = info['entity']
            metric = info['metric']
            tags = info['tags']
            aggregator = info['aggregator'].upper()
            
            for period_name in self.period_names:
            
                path = pattern + '.' + metric_quote(period_name)
                
                if fnmatch.fnmatch(path, pattern_match):
                
                    # self.log('path = ' + path)
                    
                    period = self.periods[self.period_names.index(period_name)]
                    self.log('aggregator = ' + aggregator + ', period = ' + unicode(period))

                    if period != 0:
                        reader = AtsdReader(entity, metric, tags, None, Aggregator(aggregator, period))
                    else:
                        reader = AtsdReader(entity, metric, tags)
                    
                    yield LeafNode(path, reader)
                
        else:
        
            if fnmatch.fnmatch(pattern, pattern_match):
        
                entity = info['entity']
                metric = info['metric']
                tags = info['tags']
                
                aggregator = info['aggregator'].upper()
                period = info['period']
                self.log('aggregator = ' + aggregator + ', period = ' + unicode(period))

                if period != 0:
                    reader = AtsdReader(entity, metric, tags, None, Aggregator(aggregator, period))
                else:
                    reader = AtsdReader(entity, metric, tags)
                
                yield LeafNode(pattern, reader)