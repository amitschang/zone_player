import socket
import threading
import Queue
import sys
import os
import json
import uuid
import sqlite3
import signal
import urllib
import time

class sink:
    '''class representing a network attached player using this protocol'''
    def __init__ (self, address, port=9988):
        self.address = address
        self.port = port
        self.status = {
            'connected':False,
            'state':'initializing',
            'track':None,
            'time':-1,
            'vol':100,
            'contact':0,
            'age':0,
        }

    def connect (self):
        self.sock = socket.socket()
        try:
            self.sock.settimeout(1)
            self.sock.connect((self.address,self.port))
            self.status['connected'] = True
            return 1
        except:
            return 0

    def close (self):
        self.sock.close()

    def report (self):
        return {
            'address': '%s:%d' %(self.address,self.port),
        }

    def send_command (self, command, args=''):
        if type(args) == list:
            full_command = command+' '+' '.join(args)
        else:
            full_command = command+' '+args
            
        try:
            self.sock.settimeout(0)
            self.sock.recv(2048)
        except:
            pass

        try:
            self.sock.settimeout(0.2)
            self.sock.send('\n'+full_command.strip())
            response = self.sock.recv(2048)
            # print 'response: %s' %(response.strip())
            response = response.split('\n')[-2].split(' ')
            if response[0] == 'ok':
                self.status['state'] = response[1]
                self.status['track'] = response[2]
                self.status['time'] = response[3]
                self.status['vol'] = response[4]
                self.status['contact'] = time.time()
            else:
                return 0
        except:
            e = sys.exc_info()
            if e[0] == socket.timeout:
                print 'got socket timeout on sink %s:%d' %(self.address,self.port)
                return 0
            else:
                self.status['connected'] = False
                print 'socket error, attempting to reconnect'
                self.connect()

    def play (self):
        return self.send_command('play')

    def pause (self):
        return self.send_command('pause')

    def stop (self):
        return self.send_command('stop')

    def track (self,filename):
        return self.send_command('track',filename)

    def getstate (self):
        self.send_command('getstate')
        self.status['age'] = time.time() - self.status['contact']
        return self.status

    def time (self, stime=None):
        if stime:
            return self.send_command('settime', str(stime))
        else:
            return self.send_command('gettime')

    def vol (self, volume=None):
        if volume:
            return self.send_command('vol', str(volume))
        else:
            return self.send_command('getvol')


class sink_manager (threading.Thread):
    def __init__ (self,sink=None,name=None,aliases=[]):
        threading.Thread.__init__(self)
        self.queue = Queue.Queue()
        self.sinks = []
        if sink:
            self.add_sink(sink)
        if name:
            self.name = name
        else:
            self.name = uuid.uuid1()
        self.aliases = aliases
        self.playlist = []
        self.lastadd = 0

    def add_sink (self, sink):
        sink.connect()
        self.sinks.append(sink)

    def report (self):
        return {
            'name': self.name,
            'aliases': self.aliases,
            'sinks': [ s.report() for s in self.sinks ],
        }

    def playnext (self):
        if len(self.playlist) > 0:
            n = self.playlist.pop(0)
            self.sinks[0].track(n)
            self.lastadd = time.time()
            print 'added track at time %d'%self.lastadd

    def run (self):
        while True:
            try:
                cmd = self.queue.get(True, 1)
            except:
                cmd = None
                pass
            if cmd:
                if cmd['_cmd'] == 'enqueue':
                    self.playlist.append(cmd['file'])
                    res={'file':cmd['file']}
                if cmd['_cmd'] == 'playnow':
                    self.playlist.insert(0,cmd['file'])
                    self.playnext()
                    res={'file':cmd['file']}
                if cmd['_cmd'] == 'next':
                    self.playnext()
                    res={'status':'okay'}
                if cmd['_cmd'] == 'clear':
                    self.playlist = []
                    res={'status':'okay'}
                if cmd['_cmd'] == 'vol':
                    self.sinks[0].vol(cmd['vol'])
                    res={'status':'okay'}
                if cmd['_cmd'] == 'pause':
                    self.sinks[0].pause()
                    res={'status':'okay'}
                if cmd['_cmd'] == 'play':
                    self.sinks[0].play()
                    res={'status':'okay'}
                if cmd['_cmd'] == 'stop':
                    self.sinks[0].stop()
                    res={'status':'okay'}
                if cmd['_cmd'] == 'status':
                    res = self.sinks[0].getstate()
                    res['playlist'] = self.playlist
                if '_queue' in cmd and cmd['_queue']:
                    cmd['_queue'].put(res)

            if time.time() - self.sinks[0].status['contact'] > 0.3:
                self.sinks[0].getstate()
        
            if self.sinks[0].status['state'] == 'stopped':
                if time.time() - self.lastadd > 1:
                    print 'status stopped and %d s since last add, playing next' %(time.time() - self.lastadd)
                    self.playnext()

class main_manager:
    def __init__ (self):
        self.queue = Queue.Queue()
        self.sinks = []
        self.zones = []
        self.media = {}

    def create_sink (self, address, port):
        s = sink(address, port)
        m = sink_manager(s,aliases=['zone%d' %(len(self.zones)+1)])
        m.start()
        self.zones.append(m)

    def add_media (self, media, entrypoint='library'):
        if type(entrypoint) == list:
            for i in entrypoint:
                self.media[entrypoint] = media
        else:                
            self.media[entrypoint] = media
        media.add_manager(self.queue)        

    def list_zones (self):
        return [ z.report() for z in self.zones ]

    def get_zone_names (self):
        names = []
        for z in self.zones:
            names.append(z.name)
            names.extend(z.aliases)
        return names

    def get_zone (self, name):
        for z in self.zones:
            if z.name == name:
                return z
            elif name in z.aliases:
                return z
            
    def serve (self):
        while True:
            request = self.queue.get()
            routed = False
            print 'main_manager got item', request
            # route to the appropriate component
            if request['_class'] == 'media':
                if request['_ctx'] in self.media:
                    self.media[request['_ctx']].queue.put(request)
                    routed = True
            elif request['_class'] == 'player':
                print 'got player class, zone list is:',self.get_zone_names()
                if request['_ctx'] in self.get_zone_names():
                    self.get_zone(request['_ctx']).queue.put(request)
                elif request['_ctx'] == '*':
                    for i in self.zones:
                        i.queue.put(request)
                else:
                    request['_queue'].put({'_err':'no such player %s' %request['_ctx']})
            elif request['_class'] == 'main':
                if request['_cmd'] == 'list':
                    request['_queue'].put(self.list_zones())
            
class api_handler (threading.Thread):
    def __init__ (self, mainq, conn):
        threading.Thread.__init__(self)
        self.conn = conn        
        self.mainq = mainq
        self.queue = Queue.Queue()
        self.status = 0
        
    def parse_headers (self, headers):
        #
        # the parts are made of /class/context/command?params
        #
        # Start by resetting from received headers, and only set this
        # if we have a recognized command form
        #
        self.request_type = None
        self.request_params = {}

        headers = headers.split('\n')
        request = headers[0].split()
        reqtype = request[0]
        request = request[1].strip('/ ').split('/')
        request = [ urllib.unquote_plus(i) for i in request ]
        if reqtype != 'GET' or len(request) != 3:
            return
        self.request_class = request[0]
        self.request_ctx = request[1]
        cmd = request[2].split('?')
        self.request_cmd = cmd[0]
        # check the params, should be key=val or if =val not present
        # assume True
        if len(cmd) > 1:
            for p in cmd[1].split('&'):
                kv = p.split('=')
                if kv[0] in ['_class','_ctx','_cmd']:
                    continue
                if len(kv) == 1:
                    self.request_params[kv[0]] = True
                else:
                    self.request_params[kv[0]] = kv[1]
        # Everything kosher, so come back with request type
        self.request_type = reqtype
        # now check for keep-alive header, we allow it
        self.request_keepalive = False
        for h in headers:
            kv = h.lower().split(':')
            if len(kv) != 2:
                continue
            if kv[0] == 'connection' and kv[0] == 'keep-alive':
                self.request_keepalive = True

    def send_response (self, data):
        response =  [
            'HTTP/1.1 200 OK',
            'Content-Type: text/json',
            'Access-Control-Allow-Origin: *',
            'Content-Length: %d' %(len(data)),
        ]
        if self.request_keepalive:
            response.append('Connection: keep-alive')

        response = '\n'.join(response)+'\n\n'+data
        self.conn.send(response)

    def run (self):
        while True:
            headers = self.conn.recv(2048)
            print 'handler recieved request: %s' %headers
            if headers == '':
                print 'closed connection'
                self.status = 1
                if self.queue:
                    self.conn = self.queue.get()
                    self.status = 0
                    continue
                else:
                    break
            self.parse_headers(headers)
            if self.request_type:
                # build the dict to send to main queue
                self.request_params['_class'] = self.request_class
                self.request_params['_ctx'] = self.request_ctx
                self.request_params['_cmd'] = self.request_cmd
                self.request_params['_queue'] = self.queue
                # pass the data to the main q and wait for response,
                self.mainq.put(self.request_params)
                data = self.queue.get()
            else:
                data = { '_error': 'could not parse request' }

            # put together the json
            if 'pretty' in self.request_params:
                data = json.dumps(data,indent=4)+'\n'
            else:
                data = json.dumps(data)

            self.send_response(data)

class api_server (threading.Thread):
    def __init__ (self, port, mainq):
        threading.Thread.__init__(self)
        self.port = port
        self.threads = []
        self.mainq = mainq

    def serve (self):
        self.sock = socket.socket()
        self.sock.bind(('0.0.0.0',self.port))
        self.sock.listen(5)
        while True:
            c = self.sock.accept()
            o = None
            for i in self.threads:
                print 'thread %d status = %d, alive = %s' %(i.ident,i.status,i.is_alive())
                if i.status and i.is_alive():
                    o = i
                    break
            if o:
                o.queue.put(c[0])
            else:
                self.threads.append(api_handler(self.mainq, c[0]))
                self.threads[-1].start()

    def run (self):
        self.serve()

class media_manager (threading.Thread):
    def __init__ (self, dbfile):
        threading.Thread.__init__(self)
        self.queue = Queue.Queue()
        self.ikeys = ['title','filename','tracknum',
                      'artist','album','length']
        self.dbfile = dbfile
        self.db = None
        self.mgr_queue = None
        
    def add_manager (self, mgr_queue):
        self.mgr_queue = mgr_queue

    def connect (self):
        if not self.db:
            self.db = sqlite3.connect(self.dbfile)
            self.db.row_factory = sqlite3.Row

    def search (self, query):
        q = 'SELECT * FROM tracks'
        r = []
        if query != '*':
            aterms = []
            for term in query.split():
                print 'processing term',term
                kv = term.split(':')
                if len(kv) == 2:
                    if kv[0] in self.ikeys:
                        aterms.append(kv[0] + " LIKE '%%%s%%'" %kv[1])
                        print '1',aterms
                else:
                    aterms.append(
                        "title LIKE '%%%s%%' OR " %(term) +\
                        "album LIKE '%%%s%%' OR " %(term) +\
                        "artist LIKE '%%%s%%'"    %(term)
                    )
                    print '2',aterms

            q += ' WHERE (' + ') AND ('.join(aterms) +')' + \
                 ' ORDER BY artist DESC, album DESC, tracknum ASC'

        print 'media manager running query:',q
        for row in self.db.execute(q):
            _r = {}
            for i in self.ikeys:
                _r[i] = row[i]
            r.append(_r)
        
        return r

    def serve (self):
        self.connect()
        
        while True:
            req = self.queue.get()
            if req['_cmd'] == 'search':
                if 'query' in req:
                    q = req['query']
                else:
                    q = '*'
                try:
                    res = self.search(q)
                    req['_queue'].put(res)
                    if 'enqueue' in req and self.mgr_queue:
                        for i in res:
                            add = { '_class': 'player',
                                    '_ctx': req['enqueue'],
                                    '_cmd': 'enqueue',
                                    'file': i['filename'],
                                }
                            self.mgr_queue.put(add)
                except:
                    res = { '_error':
                            'database exception occurred in search' }
                    req['_queue'].put(res)
                    print sys.exc_info()

    def run (self):
        self.serve()

if __name__ == "__main__":
    m = main_manager()
    a = api_server(9876,m.queue)
    mm = media_manager('/mnt/data/music/tracks.db')
    m.add_media(mm)

    # for testing
    m.create_sink('192.168.1.15',9988)

    mm.start()
    a.start()
    m.serve()
