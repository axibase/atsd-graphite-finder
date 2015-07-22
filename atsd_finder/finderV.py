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


class AtsdFinderV(object):

    roots = {'entities', 'metrics'}
    intervals = [60, 3600, 86400]
    interval_names = ['1 min', '1 hour', '1 day']

    def __init__(self):

        try:
            # noinspection PyUnresolvedReferences
            pid = unicode(os.getppid()) + ' : ' + unicode(os.getpid())
        except AttributeError:
            pid = unicode(os.getpid())

        log.info('[AtsdFinderV] init: pid = ' + pid)

        self.url_base = ATSD_CONF['url'] + '/api/v1'
        self.auth = (ATSD_CONF['username'], ATSD_CONF['password'])
            
        try:
            self.builds =  ATSD_CONF['builds']
        except:
            self.builds = {}

    def find_nodes(self, query):

        log.info('[AtsdFinderV] finding nodes: query = ' + unicode(query.pattern))
        
        pattern = query.pattern[:-2] if query.pattern[-1] == '*' else query.pattern
        
        info = get_info(pattern)
            
        if info['tokens'] == 0:
        
            for build_name in self.builds:
        
                cell = {'build': build_name}
                
                path = full_quote(json.dumps(cell))
                log.info('[AtsdFinderV] path = ' + path)
            
                yield AtsdBranchNode(path, build_name)
            
        else:
        
            if 'build' not in info:
            
                raise StopIteration

            build = self.builds[info['build']]
                
            ind = info['tokens'] - 1
            length = len(build)
                
            if ind > length:
            
                raise StopIteration
                
            elif ind < length:
            
                if ind == length - 1:
                    last = True
                else:
                    last = False
                
                token = build[info['tokens'] - 1]
                
                token_type = token.keys()[0]
                token_value = token[token_type]
                
                if token_type == 'entity':
                
                    folder = token_value
                    
                    if not last and not 'metric' in info:
                    
                        if folder != '':
                            url = self.url_base + '/entities?expression=name%20like%20%27' + quote(folder) + '*%27'
                        else:
                            url = self.url_base + '/entities'
                            
                        log.info('[AtsdFinderV] request_url = ' + url)
                            
                        response = requests.get(url, auth=self.auth)
                        log.info('[AtsdFinderV] status = ' + unicode(response.status_code))
                        
                        for entity in response.json():

                            label = unicode(entity['name']).encode('punycode')[:-1]
                            
                            cell = {'entity': unicode(entity['name'])}
                            
                            path = pattern + '.' + full_quote(json.dumps(cell))
                            log.info('[AtsdFinderV] path = ' + path)

                            yield AtsdBranchNode(path, label)
                        
                    elif 'metric' in info:
                    
                        url = self.url_base + '/metrics/' + quote(info['metric'])+ '/entity-and-tags'
                        log.info('[AtsdFinderV] request_url = ' + url)

                        response = requests.get(url, auth=self.auth)
                        log.info('[AtsdFinderV] status = ' + unicode(response.status_code))

                        entities = set()

                        for combo in response.json():

                            entities.add(combo['entity'])

                        for entity in entities:
                        
                            if not re.match(folder + '.*', unicode(entity)):
                                continue

                            label = unicode(entity).encode('punycode')[:-1]
                            
                            cell = {'entity': unicode(entity)}
                            
                            path = pattern + '.' + full_quote(json.dumps(cell))
                            log.info('[AtsdFinderV] path = ' + path)
                            
                            if not last:
                            
                                yield AtsdBranchNode(path, label)
                                
                            else:
                            
                                metric = info['metric']
                                tags = info['tags']
                                interval = info['interval'] if 'interval' in info else 0
                                aggregator = info['aggregator'].upper() if 'aggregator' in info else 'AVG'
                                
                                reader = AtsdReader(entity, metric, tags, interval, aggregator)
                    
                                yield AtsdLeafNode(path, label, reader)
                            
                elif token_type == 'metric':
                
                    folder = token_value
                    
                    if not 'entity' in info:
                        url = self.url_base + '/metrics'
                    else:
                        url = self.url_base + '/entities/' + quote(info['entity'])+ '/metrics'
                        
                    if folder != '':
                        url = url + '?expression=name%20like%20%27' + quote(folder) + '*%27'
                        
                    log.info('[AtsdFinderV] request_url = ' + url)
                        
                    response = requests.get(url, auth=self.auth)
                    log.info('[AtsdFinderV] status = ' + unicode(response.status_code))
                    
                    for metric in response.json():

                        label = unicode(metric['name']).encode('punycode')[:-1]
                        
                        cell = {'metric': unicode(metric['name'])}
                        
                        path = pattern + '.' + full_quote(json.dumps(cell))
                        log.info('[AtsdFinderV] path = ' + path)
                        
                        if not last:

                            yield AtsdBranchNode(path, label)
                            
                        else:
                        
                            entity = info['entity'] if 'entity' in info else '*'
                            tags = info['tags']
                            interval = info['interval'] if 'interval' in info else 0
                            aggregator = info['aggregator'].upper() if 'aggregator' in info else 'AVG'
                                
                            reader = AtsdReader(entity, metric, tags, interval, aggregator)
                            
                            yield AtsdLeafNode(path, label, reader)
                        
                elif token_type == 'aggregator':
                
                    for aggregator in token_value:
                    
                        label = unicode(aggregator).encode('punycode')[:-1]
                        
                        cell = {'aggregator': unicode(token_value[aggregator])}
                        
                        path = pattern + '.' + full_quote(json.dumps(cell))
                        log.info('[AtsdFinderV] path = ' + path)
                        
                        if not last:

                            yield AtsdBranchNode(path, label)
                            
                        elif 'metric' in info:
                        
                            entity = info['entity'] if 'entity' in info else '*'
                            metric = info['metric']
                            tags = info['tags']
                            interval = info['interval'] if 'interval' in info else 0
                                
                            reader = AtsdReader(entity, metric, tags, interval, aggregator)
                            
                            yield AtsdLeafNode(path, label, reader)
                        
                elif token_type == 'interval':
                
                    for interval in token_value:
                    
                        label = unicode(interval).encode('punycode')[:-1]
                        
                        cell = {'interval': token_value[interval]}
                        
                        path = pattern + '.' + full_quote(json.dumps(cell))
                        log.info('[AtsdFinderV] path = ' + path)

                        if not last:

                            yield AtsdBranchNode(path, label)
                            
                        elif 'metric' in info:
                        
                            entity = info['entity'] if 'entity' in info else '*'
                            metric = info['metric']
                            tags = info['tags']
                            aggregator = info['aggregator'].upper() if 'aggregator' in info else 'AVG'
                                
                            reader = AtsdReader(entity, metric, tags, interval, aggregator)
                            
                            yield AtsdLeafNode(path, label, reader)
                
            else:
            
                if 'metric' in info:
            
                    entity = info['entity'] if 'entity' in info else '*'
                    metric = info['metric']
                    tags = info['tags']
                    interval = info['interval'] if 'interval' in info else 0
                    aggregator = info['aggregator'].upper() if 'aggregator' in info else 'AVG'
                    
                    reader = AtsdReader(entity, metric, tags, interval, aggregator)
                            
                    yield AtsdLeafNode(pattern, '', reader)