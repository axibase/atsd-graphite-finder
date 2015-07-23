import requests
import urllib
import json
import fnmatch
import os
import copy

from graphite.local_settings import ATSD_CONF

from .reader import AtsdReader
try:
    from graphite.logger import log
except:
    import default_logger as log
    
from .node import AtsdBranchNode, AtsdLeafNode
 
  
def quote(string):

    return urllib.quote(string, safe='*')
    
    
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
        
            for tag_name in token_value:
            
                tag_value = token_value[tag_name]
                
                info['tags'][tag_name] = tag_value
            
    log.info('[AtsdInfo] ' + json.dumps(info))
    
    return info


class AtsdFinderV(object):

    roots = {'entities', 'metrics'}
    intervals = [60, 3600, 86400]
    interval_names = ['1 min', '1 hour', '1 day']

    def __init__(self):
    
        self.name = '[AtsdFinderV]'

        try:
            # noinspection PyUnresolvedReferences
            pid = unicode(os.getppid()) + ' : ' + unicode(os.getpid())
        except AttributeError:
            pid = unicode(os.getpid())

        self.log('init: pid = ' + pid)

        self.url_base = ATSD_CONF['url'] + '/api/v1'
        self.auth = (ATSD_CONF['username'], ATSD_CONF['password'])
            
        try:
            self.builds =  ATSD_CONF['builds']
        except:
            self.builds = {}
            
    def log(self, message):
    
        log.info('[' + self.__class__.__name__ + '] ' + message)

    def find_nodes(self, query):

        self.log('finding nodes: query = ' + unicode(query.pattern))
        
        pattern = query.pattern[:-2] if query.pattern[-1] == '*' else query.pattern
        
        info = get_info(pattern)
            
        if info['tokens'] == 0:
        
            for build_name in self.builds:
        
                cell = {'build': build_name}
                
                path = full_quote(json.dumps(cell))
                # self.log('path = ' + path)
            
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
                
                token_type = token['type']
                token_value = token['value']
                
                if token_type == 'const':
                
                    label = unicode(token_value).encode('punycode')[:-1]
                        
                    cell = {'const': unicode(token_value)}
                        
                    path = pattern + '.' + full_quote(json.dumps(cell))
                
                    if not last:
                    
                        # self.log('path = ' + path)

                        yield AtsdBranchNode(path, label)
                    
                    elif 'metric' in info:
                    
                        # self.log('path = ' + path)
                    
                        entity = info['entity'] if 'entity' in info else '*'
                        metric = info['metric']
                        tags = info['tags']
                        interval = info['interval'] if 'interval' in info else 0
                        aggregator = info['aggregator'].upper() if 'aggregator' in info else 'AVG'
                        
                        reader = AtsdReader(entity, metric, tags, interval, aggregator)
                            
                        yield AtsdLeafNode(pattern, label, reader)
                        
                elif token_type in ['entity folder', 'metric folder']:
                    
                    for folder in token_value:
                    
                        if token_type not in info \
                           or token_type in info and fnmatch.fnmatch(unicode(folder), info[token_type]):
                    
                            label = unicode(folder).encode('punycode')[:-1]
                            
                            cell = {token_type: unicode(folder)}
                            
                            path = pattern + '.' + full_quote(json.dumps(cell))

                            if not last:
                            
                                # self.log('path = ' + path)
                                
                                yield AtsdBranchNode(path, label)
                                
                            elif 'metric' in info:
                                
                                # self.log('path = ' + path)
                                
                                entity = info['entity'] if 'entity' in info else '*'
                                metric = info['metric']
                                tags = info['tags']
                                interval = info['interval'] if 'interval' in info else 0
                                aggregator = info['aggregator'].upper() if 'aggregator' in info else 'AVG'
                                
                                reader = AtsdReader(entity, metric, tags, interval, aggregator)
                    
                                yield AtsdLeafNode(path, label, reader)
                
                elif token_type == 'entity':
                
                    for expr in token_value:
                    
                        expr = unicode(expr)
                
                        if expr != '*':
                            if 'entity folder' not in info or fnmatch.fnmatch(expr, info['entity folder']):
                                folder = expr
                            else:
                                folder = ''
                        elif 'entity folder' in info:
                            folder = info['entity folder']
                        else:
                            folder = '*'
                        
                        if not last and not 'metric' in info and folder != '':
                        
                            if folder != '*':
                                url = self.url_base + '/entities?expression=name%20like%20%27' + quote(folder) + '%27'
                            else:
                                url = self.url_base + '/entities'
                                
                            self.log('request_url = ' + url)
                                
                            response = requests.get(url, auth=self.auth)
                            self.log('status = ' + unicode(response.status_code))
                            
                            for entity in response.json():

                                label = unicode(entity['name']).encode('punycode')[:-1]
                                
                                cell = {'entity': unicode(entity['name'])}
                                
                                path = pattern + '.' + full_quote(json.dumps(cell))
                                # self.log('path = ' + path)

                                yield AtsdBranchNode(path, label)
                            
                        elif 'metric' in info:
                        
                            url = self.url_base + '/metrics/' + quote(info['metric'])+ '/entity-and-tags'
                            self.log('request_url = ' + url)

                            response = requests.get(url, auth=self.auth)
                            self.log('status = ' + unicode(response.status_code))

                            entities = set()

                            for combo in response.json():

                                entities.add(combo['entity'])

                            for entity in entities:
                            
                                if not fnmatch.fnmatch(unicode(entity), folder):
                                    continue

                                label = unicode(entity).encode('punycode')[:-1]
                                
                                cell = {'entity': unicode(entity)}
                                
                                path = pattern + '.' + full_quote(json.dumps(cell))
                                # self.log('path = ' + path)
                                
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
                            
                        if folder == '':
                            continue
                        
                        if not 'entity' in info:
                            url = self.url_base + '/metrics'
                        else:
                            url = self.url_base + '/entities/' + quote(info['entity'])+ '/metrics'
                            
                        if folder != '*':
                            url = url + '?expression=name%20like%20%27' + quote(folder) + '%27'
                            
                        self.log('request_url = ' + url)
                            
                        response = requests.get(url, auth=self.auth)
                        self.log('status = ' + unicode(response.status_code))
                        
                        for metric in response.json():

                            label = unicode(metric['name']).encode('punycode')[:-1]
                            
                            cell = {'metric': unicode(metric['name'])}
                            
                            path = pattern + '.' + full_quote(json.dumps(cell))
                            # self.log('path = ' + path)
                            
                            if not last:

                                yield AtsdBranchNode(path, label)
                                
                            else:
                            
                                entity = info['entity'] if 'entity' in info else '*'
                                tags = info['tags']
                                interval = info['interval'] if 'interval' in info else 0
                                aggregator = info['aggregator'].upper() if 'aggregator' in info else 'AVG'
                                    
                                reader = AtsdReader(entity, metric, tags, interval, aggregator)
                                
                                yield AtsdLeafNode(path, label, reader)
                            
                elif token_type == 'tag':
                
                    if 'metric' in info:
                
                        url = self.url_base + '/metrics/' + quote(info['metric'])+ '/entity-and-tags'
                        self.log('request_url = ' + url)

                        response = requests.get(url, auth=self.auth)
                        self.log('status = ' + unicode(response.status_code))

                        tag_combos = [];

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
                            
                                label = unicode(', '.join(tag_values)).encode('punycode')[:-1]
                            
                                cell = {'tag': tag_combo}
                            
                                path = pattern + '.' + full_quote(json.dumps(cell))
                                # self.log('path = ' + path)
                            
                                if not last:
                                
                                    yield AtsdBranchNode(path, label)
                                
                                else:
                                
                                    tags = copy.deepcopy(info['tags'])
                                    tags.update(tag_combo)
                                
                                    entity = info['entity'] if 'entity' in info else '*'
                                    metric = info['metric']
                                    interval = info['interval'] if 'interval' in info else 0
                                    aggregator = info['aggregator'].upper() if 'aggregator' in info else 'AVG'
                                    
                                    reader = AtsdReader(entity, metric, tags, interval, aggregator)
                                    
                                    self.log('label = ' + label)
                                        
                                    yield AtsdLeafNode(path, label, reader)
                        
                elif token_type == 'aggregator':
                
                    for aggregator_dict in token_value:
                    
                        aggregator = aggregator_dict.keys()[0]
                        aggregator_label = aggregator_dict[aggregator]
                    
                        label = unicode(aggregator_label).encode('punycode')[:-1]
                        
                        cell = {'aggregator': unicode(aggregator)}
                        
                        path = pattern + '.' + full_quote(json.dumps(cell))
                        # self.log('path = ' + path)
                        
                        if not last:

                            yield AtsdBranchNode(path, label)
                            
                        elif 'metric' in info:
                        
                            entity = info['entity'] if 'entity' in info else '*'
                            metric = info['metric']
                            tags = info['tags']
                            interval = info['interval'] if 'interval' in info else 0
                                
                            reader = AtsdReader(entity, metric, tags, interval, aggregator.upper())
                            
                            yield AtsdLeafNode(path, label, reader)
                        
                elif token_type == 'interval':
                
                    for interval_dict in token_value:
                    
                        interval = interval_dict.keys()[0]
                        interval_label = interval_dict[interval]
                    
                        label = unicode(interval_label).encode('punycode')[:-1]
                        
                        cell = {'interval': interval}
                        
                        path = pattern + '.' + full_quote(json.dumps(cell))
                        # self.log('path = ' + path)

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