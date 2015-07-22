import requests
import urllib
import json
import re
import os

from graphite.local_settings import ATSD_CONF

from .reader import AtsdReader
try:
    from graphite.logger import log
except:
    import default_logger as log
    
from .node import AtsdBranchNode, AtsdLeafNode

  
def quote(string):

    return urllib.quote(string, safe='')
    
    
def full_quote(string):

    return urllib.quote(string, safe='').replace('.', '%2E')
    
    
def unquote(string):

    return urllib.unquote(string)


def get_info(pattern):

    info = {
        'tags': {}
    }
        
    tokens = pattern.split('.')
    tokens[:] = [json.loads(unquote(token)) for token in tokens] if pattern != '' else []
    
    info['tokens'] = len(tokens)
    
    specific = ['tag', 'const']
    
    for token in tokens:
        
        token_type = token.keys()[0]
        token_value = token[token_type]
        
        if not token_type in specific:
        
            info[token_type] = token_value
            
        elif token_type == 'tag':
        
            tag_name = token_value.keys()[0]
            tag_value = token_value[tag_name]
        
            info['tags'][tag_name] = tag_value
            
    log.info('[AtsdInfo] ' + json.dumps(info))
    
    return info


class AtsdFinder(object):

    roots = {'entities', 'metrics'}
    intervals = [60, 3600, 86400]
    interval_names = ['1 min', '1 hour', '1 day']

    def __init__(self):
    
        self.name = '[AtsdFinder]'

        try:
            # noinspection PyUnresolvedReferences
            pid = unicode(os.getppid()) + ' : ' + unicode(os.getpid())
        except AttributeError:
            pid = unicode(os.getpid())

        log.info(self.name + ' init: pid = ' + pid)

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
            }

    def find_nodes(self, query):

        log.info(self.name + ' finding nodes: query = ' + unicode(query.pattern))
        
        pattern = query.pattern[:-2] if query.pattern[-1] == '*' else query.pattern
        
        info = get_info(pattern)

        if info['tokens'] == 0:

            for root in self.roots:
            
                cell = {'type': root}
                
                log.info(self.name + ' path = ' + root)

                yield AtsdBranchNode(full_quote(json.dumps(cell)), root)
                
        elif 'type' not in info:
        
            raise StopIteration

        elif info['tokens'] == 1:

            if info['type'] == 'entities':
                for folder in self.entity_folders:
                
                    cell = {'entity folder': folder}

                    path = pattern + '.' + full_quote(json.dumps(cell))
                    log.info(self.name + ' path = ' + path)

                    yield AtsdBranchNode(path, folder)

            elif info['type'] == 'metrics':
                for folder in self.metric_folders:
                
                    cell = {'metric folder': folder}

                    path = pattern + '.' + full_quote(json.dumps(cell))
                    log.info(self.name + ' path = ' + path)

                    yield AtsdBranchNode(path, folder)

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

            log.info(self.name + ' request_url = ' + unicode(url) + '')

            response = requests.get(url, auth=self.auth)

            #log.info(self.name + ' response = ' + response.text)
            log.info(self.name + ' status = ' + unicode(response.status_code))

            for smth in response.json():

                if not other:

                    label = unicode(smth['name']).encode('punycode')[:-1]
                    
                    if info['type'] == 'entities':
                        cell = {'entity': label}
                    elif info['type'] == 'metrics':
                        cell = {'metric': label}
                    
                    path = pattern + '.' + full_quote(json.dumps(cell))
                    log.info(self.name + ' path = ' + path)

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
                            cell = {'entity': label}
                        elif info['type'] == 'metrics':
                            cell = {'metric': label}
                        
                        path = pattern + '.' + full_quote(json.dumps(cell))
                        log.info(self.name + ' path = ' + path)

                        yield AtsdBranchNode(path, label)

        elif info['tokens'] == 3:

            if info['type'] == 'entities':

                url = self.url_base + '/entities/' + quote(info['entity']) + '/metrics'
                log.info(self.name + ' request_url = ' + url)

                response = requests.get(url, auth=self.auth)

                #log.info(self.name + ' response = ' + response.text)
                log.info(self.name + ' status = ' + unicode(response.status_code))

                for metric in response.json():

                    label = unicode(metric['name']).encode('punycode')[:-1]
                    
                    cell = {'metric': label}
                    
                    path = pattern + '.' + full_quote(json.dumps(cell))
                    log.info(self.name + ' path = ' + path)

                    yield AtsdBranchNode(path, label)

            elif info['type'] == 'metrics':

                url = self.url_base + '/metrics/' + quote(info['metric'])+ '/entity-and-tags'
                log.info(self.name + ' request_url = ' + url)

                response = requests.get(url, auth=self.auth)

                #log.info(self.name + ' response = ' + response.text)
                log.info(self.name + ' status = ' + unicode(response.status_code))

                entities = set()

                for entity in response.json():

                    entities.add(entity['entity'])

                for entity in entities:

                    label = unicode(entity).encode('punycode')[:-1]
                    
                    cell = {'entity': label}
                    
                    path = pattern + '.' + full_quote(json.dumps(cell))
                    log.info(self.name + ' path = ' + path)
                    
                    yield AtsdBranchNode(path, label)

        elif info['tokens'] > 3 and not 'detail' in info:

            entity = info['entity']
            metric = info['metric']

            tags = info['tags']

            url = self.url_base + '/metrics/' + quote(metric) + '/entity-and-tags'
            log.info(self.name + ' request_url = ' + url)

            response = requests.get(url, auth=self.auth)

            #log.info(self.name + ' response = ' + response.text)
            log.info(self.name + ' status = ' + unicode(response.status_code))

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

                            label = unicode(tag_name + ': ' + tag_combo[tag_name])
                            
                            cell = {'tag': {tag_name: tag_combo[tag_name]}}
                            
                            if not label in labels:
                            
                                labels.append(label)
                            
                                path = pattern + '.' + full_quote(json.dumps(cell))
                                log.info(self.name + ' path = ' + path)
                                
                                yield AtsdBranchNode(path, label)
                                
                            break
                            
            if not found:
            
                cell = {'detail': True}
                
                path = pattern + '.' + full_quote(json.dumps(cell))
                log.info(self.name + ' path = ' + path)
                
                reader = AtsdReader(entity, metric, tags, 0)
                
                yield AtsdLeafNode(path, u'detail', reader)
                
                cell = {'detail': False}
                
                path = pattern + '.' + full_quote(json.dumps(cell))
                log.info(self.name + ' path = ' + path)
                
                yield AtsdBranchNode(path, u'stats')
                
        elif not 'aggregator' in info:
        
            if info['detail']:
            
                entity = info['entity']
                metric = info['metric']
                tags = info['tags']
                
                reader = AtsdReader(entity, metric, tags, 0)
                
                yield AtsdLeafNode(pattern, u'detail', reader)
            
            else:
            
                for aggregator in self.aggregators:
                
                    cell = {'aggregator': aggregator}
                    
                    path = pattern + '.' + full_quote(json.dumps(cell))
                    log.info(self.name + ' path = ' + path)
                    
                    yield AtsdBranchNode(path, aggregator)
            
        elif not 'interval' in info:

            entity = info['entity']
            metric = info['metric']
            tags = info['tags']
            aggregator = self.aggregators[info['aggregator']].upper()
            
            for interval_name in self.interval_names:
                
                cell = {'interval': interval_name}
            
                path = pattern + '.' + full_quote(json.dumps(cell))
                log.info(self.name + ' path = ' + path)
                
                interval = self.intervals[self.interval_names.index(interval_name)]
                log.info(self.name + ' aggregator = ' + aggregator + ', interval = ' + unicode(interval))
                
                reader = AtsdReader(entity, metric, tags, interval, aggregator)
                
                yield AtsdLeafNode(path, interval_name, reader)
                
        else:
        
            entity = info['entity']
            metric = info['metric']
            tags = info['tags']
            aggregator = self.aggregators[info['aggregator']].upper()
        
            interval = self.intervals[self.interval_names.index(info['interval'])]
            log.info(self.name + ' aggregator = ' + aggregator + ', interval = ' + unicode(interval))
            
            reader = AtsdReader(entity, metric, tags, interval, aggregator)
            
            yield AtsdLeafNode(pattern, interval, reader)