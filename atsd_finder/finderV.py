import requests
import urllib
import json
import fnmatch
import os
import copy

from graphite.local_settings import ATSD_CONF

from .reader import AtsdReader, Aggregator
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
        
    def get_info(self, pattern):

        info = {
            'valid': True,
            'tags': {}
        }
            
        tokens = pattern.split('.')
        try:
            tokens[:] = [json.loads(unquote(token)) for token in tokens] if pattern != '' else []
        except:
            info['valid'] = False
            return info
        
        self.log('tokens = ' + json.dumps(tokens))
        
        info['tokens'] = len(tokens)
        
        if len(tokens) == 0:
            return info
            
        info['build'] = tokens[0]['value']
        build = self.builds[info['build']]
        
        specific = ['tag', 'const']
        
        for i, token in enumerate(tokens[1:]):
            
            token_type = token['type']
            token_value = token['value']
            
            level = build[i]
            
            if 'global' in level:
                for g_token in level['global']:
                
                    g_token_type = g_token['type']
                    g_token_value = g_token['value']
                    
                    if not g_token_type in specific:
                        
                        info[g_token_type] = g_token_value
                        
                    elif g_token_type == 'tag':
                    
                        for tag_name in g_token_value:
                
                            tag_value = g_token_value[tag_name]
                            
                            info['tags'][tag_name] = tag_value
                    
            if not token_type in specific:
            
                info[token_type] = token_value
                
            elif token_type == 'tag':
            
                for tag_name in token_value:
                
                    tag_value = token_value[tag_name]
                    
                    info['tags'][tag_name] = tag_value
        
        return info

    def find_nodes(self, query):

        # self.log('finding nodes: query = ' + unicode(query.pattern))
        
        if len(query.pattern) != 0 and query.pattern[-1] != '*':
            leaf_request = True
        else:
            leaf_request = False
        
        pattern = query.pattern[:-2] if not leaf_request else query.pattern
        # self.log(pattern)
        
        g_info = self.get_info(pattern)
        
        if g_info['valid'] == False:
            raise StopIteration
            
        if g_info['tokens'] == 0:
        
            for build_name in self.builds:
        
                cell = {'type': 'build',
                        'value': build_name,
                        'label': build_name}
                
                path = full_quote(json.dumps(cell))
                # self.log('path = ' + path)
            
                yield AtsdBranchNode(path, build_name)
            
        else:
        
            if 'build' not in g_info:
                raise StopIteration

            build = self.builds[g_info['build']]
                
            ind = g_info['tokens'] - 1
            length = len(build)
                
            if ind > length:
            
                raise StopIteration
                
            elif ind < length and not leaf_request:
                
                level = build[g_info['tokens'] - 1]
                self.log('level = ' + unicode(level))
                
                level_type = level['type']
                level_value = level['value']
                
                specific = ['tag', 'const']
                
                if 'global' in level:
                    for g_token in level['global']:
                    
                        g_token_type = g_token['type']
                        g_token_value = g_token['value']
                        
                        if not g_token_type in specific:
                            
                            g_info[g_token_type] = g_token_value
                            
                        elif g_token_type == 'tag':
                        
                            for tag_name in g_token_value:
                            
                                tag_value = g_token_value[tag_name]
                                g_info['tags'][tag_name] = tag_value
                                
                self.log('global info = ' + json.dumps(g_info))
                
                tokens = []
                
                if level_type == 'multiple':
                    for token in level_value:
                        tokens.append(token)
                else:
                    tokens.append(level)
                    
                self.log('tokens = ' + unicode(tokens))
                    
                for token in tokens:
                
                    info = copy.deepcopy(g_info)
                    
                    specific = ['tag', 'const']
                    
                    if 'local' in token:
                        for l_token in token['local']:
                        
                            l_token_type = l_token['type']
                            l_token_value = l_token['value']
                            
                            if not l_token_type in specific:
                                
                                info[l_token_type] = l_token_value
                                
                            elif l_token_type == 'tag':
                            
                                for tag_name in l_token_value:
                                
                                    tag_value = l_token_value[tag_name]
                                    info['tags'][tag_name] = tag_value
                                    
                    self.log('local info = ' + unicode(info))
                
                    token_type = token['type']
                    token_value = token['value']
                    last = token['is leaf'] if 'is leaf' in token else False
                    
                    if token_type == 'const':
                    
                        for string in token_value:
                    
                            label = unicode(string).encode('punycode')[:-1]
                                
                            cell = {'type': 'const',
                                    'value': unicode(string),
                                    'label': label}
                                
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

                                if interval != 0:
                                    reader = AtsdReader(entity, metric, tags,
                                                        Aggregator(interval,
                                                                   aggregator))
                                else:
                                    reader = AtsdReader(entity, metric, tags)
                                    
                                yield AtsdLeafNode(path, label, reader)
                            
                    elif token_type in ['entity folder', 'metric folder']:
                        
                        for folder_dict in token_value:
                        
                            folder = folder_dict.keys()[0]
                            folder_label = folder_dict[folder]
                        
                            if token_type not in info \
                               or token_type in info and fnmatch.fnmatch(unicode(folder), info[token_type]):
                        
                                label = unicode(folder_label).encode('punycode')[:-1]
                                
                                cell = {'type': token_type,
                                        'value': unicode(folder),
                                        'label': label}
                                
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

                                    if interval != 0:
                                        reader = AtsdReader(entity, metric, tags,
                                                            Aggregator(interval,
                                                                       aggregator))
                                    else:
                                        reader = AtsdReader(entity, metric, tags)
                        
                                    yield AtsdLeafNode(path, label, reader)
                    
                    elif token_type == 'entity':

                        folders = []

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
                                
                            if not folder in ['']:
                                folders.append(folder)
                            
                        if '*' in folders:
                            folders = ['*']
                            
                        if not last and not 'metric' in info:
                        
                            expressions = ['name%20like%20%27' + quote(folder) + '%27' for folder in folders]
                            tail = '?expression=' + '%20or%20'.join(expressions)
                        
                            url = self.url_base + '/entities' + tail
                            self.log('request_url = ' + url)
                                
                            response = requests.get(url, auth=self.auth)
                            self.log('status = ' + unicode(response.status_code))
                            
                            for entity in response.json():

                                label = unicode(entity['name']).encode('punycode')[:-1]
                                
                                cell = {'type': 'entity',
                                        'value': unicode(entity['name']),
                                        'label': label}
                                
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
                            
                                matches = False
                            
                                for folder in folders:
                                    if fnmatch.fnmatch(unicode(entity), folder):
                                        matches = True
                                        break
                                        
                                if not matches:
                                    continue

                                label = unicode(entity).encode('punycode')[:-1]
                                
                                cell = {'type': 'entity',
                                        'value': unicode(entity),
                                        'label': label}
                                
                                path = pattern + '.' + full_quote(json.dumps(cell))
                                # self.log('path = ' + path)
                                
                                if not last:
                                
                                    yield AtsdBranchNode(path, label)
                                    
                                else:
                                
                                    metric = info['metric']
                                    tags = info['tags']
                                    interval = info['interval'] if 'interval' in info else 0
                                    aggregator = info['aggregator'].upper() if 'aggregator' in info else 'AVG'

                                    if interval != 0:
                                        reader = AtsdReader(entity, metric, tags,
                                                            Aggregator(interval,
                                                                       aggregator))
                                    else:
                                        reader = AtsdReader(entity, metric, tags)
                        
                                    yield AtsdLeafNode(path, label, reader)
                                
                    elif token_type == 'metric':

                        folders = []

                        for expr in token_value:
                        
                            expr = unicode(expr)

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
                        self.log('request_url = ' + url)
                            
                        response = requests.get(url, auth=self.auth)
                        self.log('status = ' + unicode(response.status_code))
                        
                        for metric in response.json():

                            label = unicode(metric['name']).encode('punycode')[:-1]
                            
                            cell = {'type': 'metric',
                                    'value': unicode(metric['name']),
                                    'label': label}
                            
                            path = pattern + '.' + full_quote(json.dumps(cell))
                            # self.log('path = ' + path)
                            
                            if not last:

                                yield AtsdBranchNode(path, label)
                                
                            else:
                            
                                entity = info['entity'] if 'entity' in info else '*'
                                tags = info['tags']
                                interval = info['interval'] if 'interval' in info else 0
                                aggregator = info['aggregator'].upper() if 'aggregator' in info else 'AVG'

                                if interval != 0:
                                    reader = AtsdReader(entity, metric['name'], tags,
                                                        Aggregator(interval,
                                                                   aggregator))
                                else:
                                    reader = AtsdReader(entity, metric['name'], tags)
                                
                                yield AtsdLeafNode(path, label, reader)
                                
                    elif token_type == 'tag':
                    
                        if 'metric' in info:
                    
                            url = self.url_base + '/metrics/' + quote(info['metric'])+ '/entity-and-tags'
                            self.log('request_url = ' + url)

                            response = requests.get(url, auth=self.auth)
                            self.log('status = ' + unicode(response.status_code))

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
                                
                                    label = unicode(', '.join(tag_values)).encode('punycode')[:-1]
                                
                                    cell = {'type': 'tag',
                                            'value': tag_combo,
                                            'label': label}
                                
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

                                        if interval != 0:
                                            reader = AtsdReader(entity, metric, tags,
                                                                Aggregator(
                                                                    interval,
                                                                    aggregator))
                                        else:
                                            reader = AtsdReader(entity, metric, tags)
                                        
                                        self.log('label = ' + label)
                                            
                                        yield AtsdLeafNode(path, label, reader)
                            
                    elif token_type == 'aggregator':
                    
                        for aggregator_dict in token_value:
                        
                            aggregator = aggregator_dict.keys()[0]
                            aggregator_label = aggregator_dict[aggregator]
                        
                            label = unicode(aggregator_label).encode('punycode')[:-1]
                            
                            cell = {'type': 'aggregator',
                                    'value': unicode(aggregator),
                                    'label': label}
                            
                            path = pattern + '.' + full_quote(json.dumps(cell))
                            # self.log('path = ' + path)
                            
                            if not last:

                                yield AtsdBranchNode(path, label)
                                
                            elif 'metric' in info:
                            
                                entity = info['entity'] if 'entity' in info else '*'
                                metric = info['metric']
                                tags = info['tags']
                                interval = info['interval'] if 'interval' in info else 0

                                if interval != 0:
                                    reader = AtsdReader(entity, metric, tags,
                                                        Aggregator(interval,
                                                                   aggregator.upper()))
                                else:
                                    reader = AtsdReader(entity, metric, tags)
                                
                                yield AtsdLeafNode(path, label, reader)
                            
                    elif token_type == 'interval':
                    
                        for interval_dict in token_value:
                        
                            interval = interval_dict.keys()[0]
                            interval_label = interval_dict[interval]
                        
                            label = unicode(interval_label).encode('punycode')[:-1]
                            
                            cell = {'type': 'interval',
                                    'value': interval,
                                    'label': label}
                            
                            path = pattern + '.' + full_quote(json.dumps(cell))
                            # self.log('path = ' + path)

                            if not last:

                                yield AtsdBranchNode(path, label)
                                
                            elif 'metric' in info:
                            
                                entity = info['entity'] if 'entity' in info else '*'
                                metric = info['metric']
                                tags = info['tags']
                                aggregator = info['aggregator'].upper() if 'aggregator' in info else 'AVG'

                                if interval != 0:
                                    reader = AtsdReader(entity, metric, tags,
                                                        Aggregator(interval,
                                                                   aggregator))
                                else:
                                    reader = AtsdReader(entity, metric, tags)
                                
                                yield AtsdLeafNode(path, label, reader)
                
            elif leaf_request:
            
                if 'metric' in g_info:
            
                    entity = g_info['entity'] if 'entity' in g_info else '*'
                    metric = g_info['metric']
                    tags = g_info['tags']
                    interval = g_info['interval'] if 'interval' in g_info else 0
                    aggregator = g_info['aggregator'].upper() if 'aggregator' in g_info else 'AVG'

                    if interval != 0:
                        reader = AtsdReader(entity, metric, tags,
                                            Aggregator(interval, aggregator))
                    else:
                        reader = AtsdReader(entity, metric, tags)
                            
                    yield AtsdLeafNode(pattern, '', reader)