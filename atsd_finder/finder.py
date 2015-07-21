import requests
import urllib
import json
import re
import os

from graphite.local_settings import ATSD_CONF
from graphite.node import BranchNode, LeafNode

from .reader import AtsdReader
try:
    from graphite.logger import log
except:
    import default_logger as log


class AtsdBranchNode(BranchNode):

    __slots__ = ('label')

    def __init__(self, path, label):
    
        super(AtsdBranchNode, self).__init__(path)
        self.label = label
        self.local = False


class AtsdLeafNode(LeafNode):

    __slots__ = ('label')

    def __init__(self, path, label, reader):
    
        super(AtsdLeafNode, self).__init__(path, reader)
        self.label = label
        self.local = False
    
    
def arr2tags(arr):

    tags = {}

    log.info('[AtsdFinder] tags = ' + unicode(arr))
                
    for tag in arr:
    
        tag_nv = tag['value'].split(':')
        log.info('[AtsdFinder] tag n:v = ' + unicode(tag_nv))
        tags[tag_nv[0]] = tag_nv[1]

    log.info('[AtsdFinder] parsed tags = ' + unicode(tags))

    return tags
    
  
def quote(string):

    return urllib.quote(string, safe = '')
    
    
def full_quote(string):

    return urllib.quote(string, safe = '').replace('.', '%2E')
    
    
def unquote(string):

    return urllib.unquote(string)


def get_info(pattern):

    info = {
        'tags': {}
    }

    pattern = pattern[:-2] if pattern[-1] == '*' else pattern
        
    tokens = pattern.split('.')
    tokens[:] = [json.loads(unquote(token)) for token in tokens] if pattern != '' else []
    
    specific = ['tag', 'const']
    
    for token in tokens:
        
        token_type = token['type']
        token_value = token['value']
        
        if not token_type in specific:
        
            info[token_type] = token_value
            
        elif token_type == 'tag':
        
            tag_nv = token_value.split(':')
            tag_name = tag_nv[0]
            tag_value = tag_nv[1]
        
            info['tags'][tag_name] = tag_value
            
    log.info('[AtsdInfo] ' + json.dumps(info))
    
    return info

class AtsdFinder(object):

    roots = {'entities', 'metrics'}
    intervals = [0, 60, 3600, 86400]
    interval_names = ['detail', '1 min', '1 hour', '1 day']
    aggregators = {
        'Detail'                : 'detail',
        'Count'                 : 'count',
        'Minimum'               : 'min',
        'Maximum'               : 'max',
        'Average'               : 'avg',
        'Median'                : 'median',
        'Sum'                   : 'sum',
        'Percentile 99.9%'      : 'percentile_999',
        'Percentile 99.5%'      : 'percentile_995',
        'Percentile 99%'        : 'percentile_99',
        'Percentile 95%'        : 'percentile_95',
        'Percentile 90%'        : 'percentile_90',
        'Percentile 75%'        : 'percentile_75',
        'First value'           : 'first',
        'Last value'            : 'last',
        'Delta'                 : 'delta',
        'Weighted average'      : 'wavg',
        'Weighted time average' : 'wtavg',
        'Standard deviation'    : 'standard_deviation'
    };

    def __init__(self):
        
        try:
            log.info('[AtsdFinder] init: pid = ' + unicode(os.getppid()) + ' : ' + unicode(os.getpid()))
        except AttributeError:
            log.info('[AtsdFinder] init: pid = ' + unicode(os.getpid()))

        self.url_base = ATSD_CONF['url'] + '/api/v1'
        self.auth = (ATSD_CONF['username'], ATSD_CONF['password'])

        try:
            self.entity_folders = ATSD_CONF['entity_folders']
        except:
            self.entity_folders = 'abcdefghijklmnopqrstuvwxyz_'

        try:
            self.metric_folders = ATSD_CONF['metric_folders']
        except:
            self.metric_folders = 'abcdefghijklmnopqrstuvwxyz_'

    def find_nodes(self, query):
    
        info = get_info(query.pattern)

        log.info('[AtsdFinder] finding nodes: query = ' + unicode(query.pattern))

        pattern = query.pattern[:-2] if query.pattern[-1] == '*' else query.pattern
        
        tokens = pattern.split('.')
        tokens[:] = [json.loads(unquote(token)) for token in tokens] if pattern != '' else []
            
        log.info('[AtsdFinder] ' + unicode(len(tokens)) + ' tokens')

        if not tokens:

            for root in self.roots:
            
                cell = {
                    'type': 'type',
                    'value': root
                }
                
                log.info('[AtsdFinder] path = ' + root)

                yield AtsdBranchNode(full_quote(json.dumps(cell)), root)

        elif len(tokens) == 1:

            if info['type'] == 'entities':
                for folder in self.entity_folders:
                
                    cell = {
                        'type': 'entity folder',
                        'value': folder
                    }

                    path = pattern + '.' + full_quote(json.dumps(cell))
                    log.info('[AtsdFinder] path = ' +  path)

                    yield AtsdBranchNode(path, folder)

            elif info['type'] == 'metrics':
                for folder in self.metric_folders:
                
                    cell = {
                        'type': 'metric folder',
                        'value': folder
                    }

                    path = pattern + '.' + full_quote(json.dumps(cell))
                    log.info('[AtsdFinder] path = ' +  path)

                    yield AtsdBranchNode(path, folder)

        elif len(tokens) == 2:
        
            if info['type'] == 'entities':
                folder = info['entity folder']
            if info['type'] == 'metrics':
                folder = info['metric folder']

            if folder[0] == "_":
                other = True
                url = self.url_base + '/' + quote(info['type'])
            else:
                other = False
                url = self.url_base + '/' + quote(info['type']) + '?expression=name%20like%20%27' + quote(folder) + '*%27'

            log.info('[AtsdFinder] request_url = ' + unicode(url) + '')

            response = requests.get(url, auth=self.auth)

            #log.info('[AtsdFinder] response = ' + response.text)
            log.info('[AtsdFinder] status = ' + unicode(response.status_code))

            for smth in response.json():

                if not other:

                    label = unicode(smth['name']).encode('punycode')[:-1]
                    
                    if info['type'] == 'entities':
                        cell = {
                            'type': 'entity',
                            'value': label
                        }
                    elif info['type'] == 'metrics':
                        cell = {
                            'type': 'metric',
                            'value': label
                        }
                    
                    path = pattern + '.' + full_quote(json.dumps(cell))
                    log.info('[AtsdFinder] path = ' + path)

                    yield AtsdBranchNode(path, label)

                else:

                    matches = False

                    if info['type'] == 'entities':
                        for folder in self.entity_folders:

                            if re.match(folder + '.*', unicode(smth['name'])):

                                    matches = True
                                    break

                    elif info['type'] == 'metrics':
                        for folder in self.metric_folders:

                            if re.match(folder + '.*', unicode(smth['name'])):

                                    matches = True
                                    break

                    if not matches:

                        label = unicode(smth['name']).encode('punycode')[:-1]
                        
                        if info['type'] == 'entities':
                            cell = {
                                'type': 'entity',
                                'value': label
                            }
                        elif info['type'] == 'metrics':
                            cell = {
                                'type': 'metric',
                                'value': label
                            }
                        
                        path = pattern + '.' + full_quote(json.dumps(cell))
                        log.info('[AtsdFinder] path = ' + path)

                        yield AtsdBranchNode(path, label)

        elif len(tokens) == 3:

            if info['type'] == 'entities':

                url = self.url_base + '/entities/' + quote(info['entity']) + '/metrics'
                log.info('[AtsdFinder] request_url = ' + url)

                response = requests.get(url, auth=self.auth)

                #log.info('[AtsdFinder] response = ' + response.text)
                log.info('[AtsdFinder] status = ' + unicode(response.status_code))

                for metric in response.json():

                    label = unicode(metric['name']).encode('punycode')[:-1]
                    
                    cell = {
                        'type': 'metric',
                        'value': label
                    }
                    
                    path = pattern + '.' + full_quote(json.dumps(cell))
                    log.info('[AtsdFinder] path = ' + path)

                    yield AtsdBranchNode(path, label)

            elif info['type'] == 'metrics':

                url = self.url_base + '/metrics/' + quote(info['metric'])+ '/entity-and-tags'
                log.info('[AtsdFinder] request_url = ' + url)

                response = requests.get(url, auth=self.auth)

                #log.info('[AtsdFinder] response = ' + response.text)
                log.info('[AtsdFinder] status = ' + unicode(response.status_code))

                entities = set()

                for entity in response.json():

                    entities.add(entity['entity'])

                for entity in entities:

                    label = unicode(entity).encode('punycode')[:-1]
                    
                    cell = {
                        'type': 'entity',
                        'value': label
                    }
                    
                    path = pattern + '.' + full_quote(json.dumps(cell))
                    log.info('[AtsdFinder] path = ' + path)
                    
                    yield AtsdBranchNode(path, label)

        elif len(tokens) > 3 and not 'interval' in info:

            entity = info['entity']
            metric = info['metric']

            tags = info['tags']

            url = self.url_base + '/metrics/' + quote(metric) + '/entity-and-tags'
            log.info('[AtsdFinder] request_url = ' + url)

            response = requests.get(url, auth=self.auth)

            #log.info('[AtsdFinder] response = ' + response.text)
            log.info('[AtsdFinder] status = ' + unicode(response.status_code))

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

                if (suitable):
                    for tag_name in tag_names:
                        if not tag_name in tags and tag_name in tag_combo:

                            found = True

                            label = unicode(tag_name + ':' + tag_combo[tag_name])
                            
                            cell = {
                                'type': 'tag',
                                'value': label
                            }
                            
                            if not label in labels:
                            
                                labels.append(label)
                            
                                path = pattern + '.' + full_quote(json.dumps(cell))
                                log.info('[AtsdFinder] path = ' + path)
                                
                                yield AtsdBranchNode(path, label)
                                
                            break
                            
            if not found:
            
                for interval_name in self.interval_names:
                
                    cell = {
                        'type': 'interval',
                        'value': interval_name
                    }
                
                    path = pattern + '.' +  full_quote(json.dumps(cell))
                    log.info('[AtsdFinder] path = ' + path)
                    
                    interval = self.intervals[self.interval_names.index(interval_name)]
                    log.info('[AtsdFinder] interval = ' + unicode(interval))
                    
                    try:
                        reader = AtsdReader(entity, metric, tags, interval)
                    except:
                        reader = None
                    
                    yield AtsdLeafNode(path, interval_name, reader)
                    
        else:

            entity = info['entity']
            metric = info['metric']
                
            tags = info['tags']
            
            interval = self.intervals[self.interval_names.index(info['interval'])]
            log.info('[AtsdFinder] interval = ' + unicode(interval))
            
            try:
                reader = AtsdReader(entity, metric, tags, interval)
            except:
                reader = None
            
            yield AtsdLeafNode(pattern, interval, reader)