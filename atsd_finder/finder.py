import requests
import urllib
from graphite.local_settings import ATSD_CONF
import re
import os

from .reader import AtsdReader
try:
    from graphite.logger import log
except:
    import default_logger as log


class AtsdNode(object):

    __slots__ = ('name', 'path', 'token', 'local', 'is_leaf')

    def __init__(self, path, token):
    
        self.path = path
        self.name = path.split('.')[-1]
        self.token = token
        self.local = True
        self.is_leaf = False

    def __repr__(self):
    
        return '<%s[%x]: %s>' % (self.__class__.__name__, id(self), self.path)


class AtsdBranchNode(AtsdNode):

    pass


class AtsdLeafNode(AtsdNode):

    __slots__ = ('reader', 'intervals')

    def __init__(self, path, token, reader):
    
        AtsdNode.__init__(self, path, token)
        self.reader = reader
        self.intervals = reader.get_intervals()
        self.is_leaf = True

    def fetch(self, startTime, endTime):
    
        return self.reader.fetch(startTime, endTime)

    def __repr__(self):
    
        return '<LeafNode[%x]: %s (%s)>' % (id(self), self.path, self.reader)
    
    
def arr2tags(arr):

    tags = {}

    log.info('[AtsdFinder] tags = ' + unicode(arr))
                
    for tag in arr:
    
        tag_nv = tag.split(':')
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


class AtsdFinder(object):

    roots = {'entities', 'metrics'}
    intervals = [0, 60, 3600, 86400]
    interval_names = ['detail', '1 min', '1 hour', '1 day']

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

        log.info('[AtsdFinder] finding nodes: query=' + unicode(query.pattern))

        pattern = query.pattern[:-2] if query.pattern[-1] == '*' else query.pattern
        
        tokens = pattern.split('.')
        tokens[:] = [unquote(token) for token in tokens]
            
        log.info('[AtsdFinder] tokens = ' + unicode(tokens))

        if not tokens or tokens[0] == '':

            for root in self.roots:

                log.info('[AtsdFinder] path = ' + root)

                yield AtsdBranchNode(root, root)

        elif len(tokens) == 1:

            if tokens[0] == 'entities':
                for folder in self.entity_folders:

                    path = pattern + '.' + folder
                    log.info('[AtsdFinder] path = ' +  path)

                    yield AtsdBranchNode(path, folder)

            elif tokens[0] == 'metrics':
                for folder in self.metric_folders:

                    path = pattern + '.' + folder
                    log.info('[AtsdFinder] path = ' +  path)

                    yield AtsdBranchNode(path, folder)

        elif len(tokens) == 2:

            if tokens[0] in self.roots:

                if not tokens[1] or tokens[1][0] == "_":
                    other = True
                    url = self.url_base + '/' + quote(tokens[0])
                else:
                    other = False
                    url = self.url_base + '/' + quote(tokens[0]) + '?expression=name%20like%20%27' + quote(tokens[1]) + '*%27'

                log.info('[AtsdFinder] request_url = ' + unicode(url) + '')

                response = requests.get(url, auth=self.auth)

                #log.info('[AtsdFinder] response = ' + response.text)
                log.info('[AtsdFinder] status = ' + unicode(response.status_code))

                for smth in response.json():

                    if not other:

                        name = unicode(smth['name']).encode('punycode')[:-1]
                        path = pattern + '.' + full_quote(name)
                        log.info('[AtsdFinder] path = ' + path)

                        yield AtsdBranchNode(path, name)

                    else:

                        matches = False

                        if tokens[0] == 'entities':
                            for folder in self.entity_folders:

                                if re.match(folder + '.*', unicode(smth['name'])):

                                        matches = True
                                        break

                        elif tokens[0] == 'metrics':
                            for folder in self.metric_folders:

                                if re.match(folder + '.*', unicode(smth['name'])):

                                        matches = True
                                        break

                        if not matches:

                            name = unicode(smth['name']).encode('punycode')[:-1]
                            path = pattern + '.' + full_quote(name)
                            log.info('[AtsdFinder] path = ' + path)

                            yield AtsdBranchNode(path, name)

        elif len(tokens) == 3:

            if tokens[0] == 'entities':

                url = self.url_base + '/entities/' + quote(tokens[2]) + '/metrics'
                log.info('[AtsdFinder] request_url = ' + url)

                response = requests.get(url, auth=self.auth)

                #log.info('[AtsdFinder] response = ' + response.text)
                log.info('[AtsdFinder] status = ' + unicode(response.status_code))

                for metric in response.json():

                    name = unicode(metric['name']).encode('punycode')[:-1]
                    path = pattern + '.' + full_quote(name)
                    log.info('[AtsdFinder] path = ' + path)

                    yield AtsdBranchNode(path, name)

            elif tokens[0] == 'metrics':

                url = self.url_base + '/metrics/' + quote(tokens[2])+ '/entity-and-tags'
                log.info('[AtsdFinder] request_url = ' + url)

                response = requests.get(url, auth=self.auth)

                #log.info('[AtsdFinder] response = ' + response.text)
                log.info('[AtsdFinder] status = ' + unicode(response.status_code))

                entities = set()

                for entity in response.json():

                    entities.add(entity['entity'])

                for entity in entities:

                    name = unicode(entity).encode('punycode')[:-1]
                    path = pattern + '.' + full_quote(name)
                    log.info('[AtsdFinder] path = ' + path)
                    
                    yield AtsdBranchNode(path, name)

        elif len(tokens) > 3 and not tokens[-1] in self.interval_names:

            if tokens[0] == 'entities':

                entity = tokens[2]
                metric = tokens[3]

            else:

                entity = tokens[3]
                metric = tokens[2]

            tags = arr2tags(tokens[4:])

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
            
            names = []

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

                            name = unicode(tag_name + ':' + tag_combo[tag_name])
                            
                            if not name in names:
                            
                                names.append(name)
                            
                                path = pattern + '.' + full_quote(name)
                                log.info('[AtsdFinder] path = ' + path)
                                
                                yield AtsdBranchNode(path, name)
                                
                            break
                            
            if not found:
            
                for interval_name in self.interval_names:
                
                    path = pattern + '.' +  full_quote(interval_name)
                    log.info('[AtsdFinder] path = ' + path)
                    
                    interval = self.intervals[self.interval_names.index(interval_name)]
                    log.info('[AtsdFinder] interval = ' + unicode(interval))
                    
                    try:
                        reader = AtsdReader(entity, metric, tags, interval)
                    except:
                        reader = None
                    
                    yield AtsdLeafNode(path, interval_name, reader)
                    
        else:

            if tokens[0] == 'entities':

                entity = tokens[2]
                metric = tokens[3]

            else:

                entity = tokens[3]
                metric = tokens[2]
                
            tags = arr2tags(tokens[4:-1])
            
            log.info('[AtsdFinder] path = ' + pattern)
            
            interval = self.intervals[self.interval_names.index(tokens[-1])]
            log.info('[AtsdFinder] interval = ' + unicode(interval))
            
            try:
                reader = AtsdReader(entity, metric, tags, interval)
            except:
                reader = None
            
            yield AtsdLeafNode(pattern, interval, reader)