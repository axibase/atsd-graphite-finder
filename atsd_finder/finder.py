import requests
import urllib
import atsd_conf
import sys
import re

from .reader import AtsdReader
try:
    from graphite.logger import log
except:
    import default_logger as log


class AtsdNode(object):

    __slots__ = ('name', 'path', 'local', 'is_leaf')

    def __init__(self, path):
    
        self.path = path
        self.name = path.replace('vvv.', '#').split('.')[-1].replace('#', 'vvv.')
        self.local = True
        self.is_leaf = False

    def __repr__(self):
    
        return '<%s[%x]: %s>' % (self.__class__.__name__, id(self), self.path)


class AtsdBranchNode(AtsdNode):

    pass


class AtsdLeafNode(AtsdNode):

    __slots__ = ('reader', 'intervals')

    def __init__(self, path, reader):
    
        AtsdNode.__init__(self, path)
        self.reader = reader
        self.intervals = reader.get_intervals()
        self.is_leaf = True

    def fetch(self, startTime, endTime):
    
        return self.reader.fetch(startTime, endTime)

    def __repr__(self):
    
        return '<LeafNode[%x]: %s (%s)>' % (id(self), self.path, self.reader)


def str2json(string):

    return '{"' + string.replace(':', '":"').replace('&', '","') + '"}'
    
    
def arr2tags(arr):

    tags = {}

    log.info('[AtsdFinder] tags = ' + unicode(arr))
                
    for tag in arr:
    
        tag_nv = tag.replace('vvv.', '.').replace('vvv_', ' ').split(':')
        log.info('[AtsdFinder] tag n:v = ' + unicode(tag_nv))
        tags[tag_nv[0]] = tag_nv[1]
        
    log.info('[AtsdFinder] parsed tags = ' + unicode(tags))
        
    return tags
    

class AtsdFinder(object):

    roots = {'entities', 'metrics'}
    intervals = [0, 60, 3600, 86400]
    interval_names = ['raw', '1_min', '1_hour', '1_day']

    def __init__(self):
    
        log.info('[AtsdFinder] init')

        self.url_base = atsd_conf.url + '/api/v1'
        self.auth = (atsd_conf.username, atsd_conf.password)
        
        try:
            self.entity_catalogues = atsd_conf.entity_catalogues
        except:
            self.entity_catalogues = 'abcdefghijklmnopqrstuvwxyz_'

        try:
            self.metric_catalogues = atsd_conf.metric_catalogues
        except:
            self.metric_catalogues = 'abcdefghijklmnopqrstuvwxyz_'

    def find_nodes(self, query):
    
        log.info('[AtsdFinder] finding nodes: query=' + unicode(query.pattern))
        #log.info('[AtsdFinder] finding nodes: query=' + unicode(query))

        pattern = query.pattern[:-2] if query.pattern[-1] == '*' else query.pattern
        #pattern = query[:-2] if query[-1] == '*' else query
        
        tokens = pattern.replace('vvv.', '#').split('.')
        tokens[:] = [token.replace('#', '.') for token in tokens]

        log.info('[AtsdFinder] ' + unicode(len(tokens)) + ' tokens')

        if not tokens or tokens[0] == '':

            for root in self.roots:
            
                log.info('[AtsdFinder] path = ' + root)

                yield AtsdBranchNode(root)

        elif len(tokens) == 1:
        
            if tokens[0] == 'entities':
                for catalogue in self.entity_catalogues:
                
                    path = pattern + '.' + catalogue
                    log.info('[AtsdFinder] path = ' +  path)
                    
                    yield AtsdBranchNode(path)

            elif tokens[0] == 'metrics':
                for catalogue in self.metric_catalogues:
                
                    path = pattern + '.' + catalogue
                    log.info('[AtsdFinder] path = ' +  path)
                    
                    yield AtsdBranchNode(path)

        elif len(tokens) == 2:

            if tokens[0] in self.roots:
                
                if not tokens[1] or tokens[1][0] == "_":
                    other = True
                    url = self.url_base + '/' + urllib.quote(tokens[0], safe = '')
                else:
                    other = False
                    url = self.url_base + '/' + urllib.quote(tokens[0], safe = '') + '?expression=name%20like%20%27' + urllib.quote(tokens[1], safe = '') + '*%27'
                    
                # url = self.url_base + '/entities/safeway/metrics?limit=2'
                log.info('[AtsdFinder] request_url = ' + unicode(url) + '')
                
                try:
                    response = requests.get(url, auth=self.auth)
                except:
                    log.info('[AtsdFinder] request = ' + unicode(url) + 'vvvn' + unicode(sys.exc_info()[0]))
                    
                #log.info('[AtsdFinder] response = ' + response.text)
                log.info('[AtsdFinder] status = ' + unicode(response.status_code))

                for smth in response.json():
                
                    if not other:

                        path = pattern + '.' + unicode(smth['name']).replace('.', 'vvv.').encode('punycode')[:-1]
                        log.info('[AtsdFinder] path = ' + path)
                        
                        yield AtsdBranchNode(path)
                        
                    else:
                    
                        matches = False
                    
                        if tokens[0] == 'entities':
                            for catalogue in self.entity_catalogues:
                            
                                if re.match(catalogue + '.*', unicode(smth['name'])):
                                        
                                        matches = True
                                        break
                        
                        elif tokens[0] == 'metrics':
                            for catalogue in self.metric_catalogues:
                            
                                if re.match(catalogue + '.*', unicode(smth['name'])):
                                        
                                        matches = True
                                        break
                                
                        if not matches:
                        
                            path = pattern + '.' + unicode(smth['name']).replace('.', 'vvv.').encode('punycode')[:-1]
                            log.info('[AtsdFinder] path = ' + path)
                            
                            yield AtsdBranchNode(path)

        elif len(tokens) == 3:

            if tokens[0] == 'entities':

                url = self.url_base + '/entities/' + urllib.quote(tokens[2]) + '/metrics'
                log.info('[AtsdFinder] request_url = ' + url)
                
                try:
                    response = requests.get(url, auth=self.auth)
                except:
                    log.info('[AtsdFinder] error = ' + unicode(sys.exc_info()[0]))
                    
                #log.info('[AtsdFinder] response = ' + response.text)
                log.info('[AtsdFinder] status = ' + unicode(response.status_code))

                for metric in response.json():
                
                    path = pattern + '.' + unicode(metric['name']).replace('.', 'vvv.').encode('punycode')[:-1]
                    log.info('[AtsdFinder] path = ' + path)
                    
                    yield AtsdBranchNode(path)

            elif tokens[0] == 'metrics':

                url = self.url_base + '/metrics/' + urllib.quote(tokens[2], safe='')+ '/entity-and-tags'
                log.info('[AtsdFinder] request_url = ' + url)
                
                try:
                    response = requests.get(url, auth=self.auth)  
                except:
                    log.info('[AtsdFinder] error = ' + unicode(sys.exc_info()[0]))
                    
                #log.info('[AtsdFinder] response = ' + response.text)
                log.info('[AtsdFinder] status = ' + unicode(response.status_code))
                         
                entities = set()

                for entity in response.json():

                    entities.add(entity['entity'])

                for entity in entities:
                
                    path = pattern + '.' + unicode(entity).replace('.', 'vvv.').encode('punycode')[:-1]
                    log.info('[AtsdFinder] path = ' + path)
                    
                    yield AtsdBranchNode(path)

        elif len(tokens) > 3 and not tokens[-1] in self.interval_names:

            if tokens[0] == 'entities':

                entity = tokens[2]
                metric = tokens[3]

            else:

                entity = tokens[3]
                metric = tokens[2]
                
            tags = arr2tags(tokens[4:])

            url = self.url_base + '/metrics/' + urllib.quote(metric, safe='') + '/entity-and-tags'
            log.info('[AtsdFinder] request_url = ' + url)
            
            try:
                response = requests.get(url, auth=self.auth)
            except:
                log.info('[AtsdFinder] error = ' + unicode(sys.exc_info()[0]))
                
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
                        
                            path = pattern + '.' + unicode(tag_name + ':' + tag_combo[tag_name])\
                                .replace('.', 'vvv.')\
                                .replace(' ', 'vvv_')
                            log.info('[AtsdFinder] path = ' + path)
                            
                            yield AtsdBranchNode(path)
                            
                            break
                            
            if not found:
            
                for interval_name in self.interval_names:
            
                    path = pattern + '.' + interval_name
                    log.info('[AtsdFinder] path = ' + path)
                    
                    interval = self.intervals[self.interval_names.index(interval_name)]
                    log.info('[AtsdFinder] interval = ' + unicode(interval))
                    
                    try:
                        reader = AtsdReader(entity, metric, tags, interval)
                    except:
                        reader = None
                    
                    yield AtsdLeafNode(path, reader)
                    
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
            
            yield AtsdLeafNode(pattern, reader)