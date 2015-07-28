# -*- coding: utf-8 -*-

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

    return urllib.quote(string.encode('utf8'), safe='*')
    
    
def full_quote(string):

    return urllib.quote(string.encode('utf8'), safe='').replace('.', '%2E')
    
    
def unquote(string):

    return urllib.unquote(string.encode('utf8'))
    

class AtsdFinderV(object):

    def __init__(self):
    
        self.name = '[AtsdFinderV]'

        try:
            # noinspection PyUnresolvedReferences
            self.pid = unicode(os.getppid()) + ':' + unicode(os.getpid())
        except AttributeError:
            self.pid = unicode(os.getpid())

        self.log('init')

        self.url_base = ATSD_CONF['url'] + '/api/v1'
        self.auth = (ATSD_CONF['username'], ATSD_CONF['password'])
            
        try:
            self.builds =  ATSD_CONF['builds']
        except:
            self.builds = {}
            
    def log(self, message):
    
        log.info('[' + self.__class__.__name__ + ' ' + self.pid + '] ' + message)
        
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
        
        self.log('tokens = ' + json.dumps(tokens))
        
        info['tokens'] = len(tokens)
        
        if len(tokens) == 0:
            return info

        info['build'] = tokens[0]
        
        try:
            build = self.builds[info['build']]
        except:
            return info
        
        specific = ['tag', 'const']
        
        for i, token in enumerate(tokens[1:]):
        
            level = build[i]
            
            if 'global' in level:
                for g_token in level['global']:
                
                    g_token_type = g_token['type']
                    g_token_value = g_token['value'][0]
                    
                    if not g_token_type in specific:
                        
                        info[g_token_type] = g_token_value
                        
                    elif g_token_type == 'tag':
                    
                        for tag_name in g_token_value:
                
                            tag_value = g_token_value[tag_name]
                            
                            info['tags'][tag_name] = tag_value

            token_type = level['type']
            token_desc = level['value']
            
            if token_type == 'collection':
            
                for desc in level['value']:
                
                    is_leaf = desc['is leaf'] if 'is leaf' in desc else False
                    
                    if is_leaf == leaf_request or i != info['tokens'] - 1:
                    
                        token_type = desc['type']
                        token_desc = desc['value']
                        
                        break
            
            if token_type in ['build', 'entity', 'metric']:
            
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
            
            elif token_type == 'interval':
            
                for interval_dict in token_desc:
                    if token == interval_dict['label']:
                    
                        info[token_type] = interval_dict
                        break
        
        return info

    def find_nodes(self, query):

        # self.log('finding nodes: query = ' + query.pattern)
        
        if len(query.pattern) != 0 and query.pattern[-1] != '*':
            leaf_request = True
        else:
            leaf_request = False
        
        pattern = query.pattern[:-2] if not leaf_request else query.pattern
        # self.log(pattern)
        
        g_info = self.get_info(pattern, leaf_request)
        self.log('initial info = ' + json.dumps(g_info))
        
        if not g_info['valid']:
            raise StopIteration
            
        if g_info['tokens'] == 0:
        
            for build_name in self.builds:

                path = full_quote(build_name)
                # self.log('path = ' + path)
            
                yield AtsdBranchNode(path, build_name)
        
        else:
        
            if 'build' not in g_info or not g_info['build']:
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
                        g_token_value = g_token['value'][0]
                        
                        if not g_token_type in specific:
                            
                            g_info[g_token_type] = g_token_value
                            
                        elif g_token_type == 'tag':
                        
                            for tag_name in g_token_value:
                            
                                tag_value = g_token_value[tag_name]
                                g_info['tags'][tag_name] = tag_value
                                
                self.log('global info = ' + json.dumps(g_info))
                
                tokens = []
                
                if level_type == 'collection':
                    for token in level_value:
                        tokens.append(token)
                else:
                    tokens.append(level)
                    
                self.log('descs = ' + unicode(tokens))
                    
                for token in tokens:
                
                    info = copy.deepcopy(g_info)
                    
                    specific = ['tag', 'const']
                    
                    if 'local' in token:
                        for l_token in token['local']:
                        
                            l_token_type = l_token['type']
                            l_token_value = l_token['value'][0]
                            
                            if not l_token_type in specific:
                                
                                info[l_token_type] = l_token_value
                                
                            elif l_token_type == 'tag':
                            
                                for tag_name in l_token_value:
                                
                                    tag_value = l_token_value[tag_name]
                                    info['tags'][tag_name] = tag_value
                                    
                    self.log('local info = ' + unicode(info))
                
                    token_type = token['type']
                    token_value = token['value']
                    is_leaf = token['is leaf'] if 'is leaf' in token else False
                    
                    if token_type == 'const':
                    
                        for string in token_value:

                            path = pattern + '.' + full_quote(string)
                        
                            if not is_leaf:
                            
                                # self.log('path = ' + path)

                                yield AtsdBranchNode(path, string)
                            
                            elif 'metric' in info:
                            
                                # self.log('path = ' + path)
                            
                                entity = info['entity'] if 'entity' in info else '*'
                                metric = info['metric']
                                tags = info['tags']
                                    
                                if not 'interval' in info or info['interval']['count'] == 0:
                                    reader = AtsdReader(entity, metric, tags)
                                else:
                                    interval_count = info['interval']['count']
                                    interval_unit = info['interval']['unit']
                                    aggregator = info['aggregator'].upper() if 'aggregator' in info else 'AVG'
                                    reader = AtsdReader(entity, metric, tags, Aggregator(aggregator, interval_count, interval_unit))
                                    
                                yield AtsdLeafNode(path, string, reader)
                            
                    elif token_type in ['entity folder', 'metric folder']:
                        
                        for folder_dict in token_value:
                        
                            folder = folder_dict.keys()[0]
                            folder_label = folder_dict[folder]
                        
                            if token_type not in info \
                               or token_type in info and fnmatch.fnmatch(folder, info[token_type]):

                                path = pattern + '.' + full_quote(folder_label)

                                if not is_leaf:
                                
                                    # self.log('path = ' + path)
                                    
                                    yield AtsdBranchNode(path, folder_label)
                                    
                                elif 'metric' in info:
                                    
                                    # self.log('path = ' + path)
                                    
                                    entity = info['entity'] if 'entity' in info else '*'
                                    metric = info['metric']
                                    tags = info['tags']
                                        
                                    if not 'interval' in info or info['interval']['count'] == 0:
                                        reader = AtsdReader(entity, metric, tags)
                                    else:
                                        interval_count = info['interval']['count']
                                        interval_unit = info['interval']['unit']
                                        aggregator = info['aggregator'].upper() if 'aggregator' in info else 'AVG'
                                        reader = AtsdReader(entity, metric, tags, Aggregator(aggregator, interval_count, interval_unit))
                        
                                    yield AtsdLeafNode(path, folder_label, reader)
                    
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
                            self.log('request_url = ' + url)
                                
                            response = requests.get(url, auth=self.auth)
                            self.log('status = ' + unicode(response.status_code))
                            
                            for entity in response.json():

                                label = entity['name']

                                path = pattern + '.' + full_quote(label)
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
                                    if fnmatch.fnmatch(entity, folder):
                                        matches = True
                                        break
                                        
                                if not matches:
                                    continue

                                path = pattern + '.' + full_quote(entity)
                                # self.log('path = ' + path)
                                
                                if not is_leaf:
                                
                                    yield AtsdBranchNode(path, entity)
                                    
                                else:
                                
                                    metric = info['metric']
                                    tags = info['tags']
                                        
                                    if not 'interval' in info or info['interval']['count'] == 0:
                                        reader = AtsdReader(entity['name'], metric, tags)
                                    else:
                                        interval_count = info['interval']['count']
                                        interval_unit = info['interval']['unit']
                                        aggregator = info['aggregator'].upper() if 'aggregator' in info else 'AVG'
                                        reader = AtsdReader(entity['name'], metric, tags, Aggregator(aggregator, interval_count, interval_unit))
                        
                                    yield AtsdLeafNode(path, entity, reader)
                                
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
                        self.log('request_url = ' + url)
                            
                        response = requests.get(url, auth=self.auth)
                        self.log('status = ' + unicode(response.status_code))
                        
                        for metric in response.json():

                            label = metric['name']

                            path = pattern + '.' + full_quote(label)
                            # self.log('path = ' + path)
                            
                            if not is_leaf:

                                yield AtsdBranchNode(path, label)
                                
                            else:
                            
                                entity = info['entity'] if 'entity' in info else '*'
                                tags = info['tags']
                                
                                if not 'interval' in info or info['interval']['count'] == 0:
                                    reader = AtsdReader(entity, metric['name'], tags)
                                else:
                                    interval_count = info['interval']['count']
                                    interval_unit = info['interval']['unit']
                                    aggregator = info['aggregator'].upper() if 'aggregator' in info else 'AVG'
                                    reader = AtsdReader(entity, metric['name'], tags, Aggregator(aggregator, interval_count, interval_unit))
                                
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
                                
                                    label = ', '.join(tag_values)

                                    path = pattern + '.' + full_quote(label)
                                    # self.log('path = ' + path)
                                
                                    if not is_leaf:
                                    
                                        yield AtsdBranchNode(path, label)
                                    
                                    else:
                                    
                                        tags = copy.deepcopy(info['tags'])
                                        tags.update(tag_combo)
                                    
                                        entity = info['entity'] if 'entity' in info else '*'
                                        metric = info['metric']
                                        
                                        if not 'interval' in info or info['interval']['count'] == 0:
                                            reader = AtsdReader(entity, metric, tags)
                                        else:
                                            interval_count = info['interval']['count']
                                            interval_unit = info['interval']['unit']
                                            aggregator = info['aggregator'].upper() if 'aggregator' in info else 'AVG'
                                            reader = AtsdReader(entity, metric, tags, Aggregator(aggregator, interval_count, interval_unit))
                                            
                                        yield AtsdLeafNode(path, label, reader)
                            
                    elif token_type == 'aggregator':
                    
                        for aggregator_dict in token_value:
                        
                            aggregator = aggregator_dict.keys()[0]
                            aggregator_label = aggregator_dict[aggregator]

                            path = pattern + '.' + full_quote(aggregator_label)
                            # self.log('path = ' + path)
                            
                            if not is_leaf:

                                yield AtsdBranchNode(path, aggregator_label)
                                
                            elif 'metric' in info:
                            
                                entity = info['entity'] if 'entity' in info else '*'
                                metric = info['metric']
                                tags = info['tags']
                                    
                                if not 'interval' in info or info['interval']['count'] == 0:
                                    reader = AtsdReader(entity, metric, tags)
                                else:
                                    interval_count = info['interval']['count']
                                    interval_unit = info['interval']['unit']
                                    reader = AtsdReader(entity, metric, tags, Aggregator(aggregator, interval_count, interval_unit))
                                
                                yield AtsdLeafNode(path, aggregator_label, reader)
                            
                    elif token_type == 'interval':
                    
                        for interval_dict in token_value:
                        
                            interval_count = interval_dict['count']
                            interval_unit = interval_dict['unit']
                            interval_label = interval_dict['label']

                            path = pattern + '.' + full_quote(interval_label)
                            # self.log('path = ' + path)

                            if not is_leaf:

                                yield AtsdBranchNode(path, interval_label)
                                
                            elif 'metric' in info:
                            
                                entity = info['entity'] if 'entity' in info else '*'
                                metric = info['metric']
                                tags = info['tags']
                                    
                                if interval_count == 0:
                                    reader = AtsdReader(entity, metric, tags)
                                else:
                                    aggregator = info['aggregator'].upper() if 'aggregator' in info else 'AVG'
                                    reader = AtsdReader(entity, metric, tags, Aggregator(aggregator, interval_count, interval_unit))
                                
                                yield AtsdLeafNode(path, interval_label, reader)
                
            elif leaf_request:
            
                if 'metric' in g_info:
            
                    entity = g_info['entity'] if 'entity' in g_info else '*'
                    metric = g_info['metric']
                    tags = g_info['tags']
                        
                    if not 'interval' in g_info or g_info['interval']['count'] == 0:
                        reader = AtsdReader(entity, metric, tags)
                    else:
                        interval_count = g_info['interval']['count']
                        interval_unit = g_info['interval']['unit']
                        aggregator = g_info['aggregator'].upper() if 'aggregator' in g_info else 'AVG'
                        reader = AtsdReader(entity, metric, tags,
                                            Aggregator(aggregator, interval_count, interval_unit))
                
                    yield AtsdLeafNode(pattern, '', reader)