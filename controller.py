import json
import logging
import logging.handlers
import os
import Queue
import signal
import socket
import sqlite3
import sys
import threading
import time
import urllib
import uuid

HB_SYNC_LIMIT = 100

class sink:
    #
    # A class representing remote sink
    #
    # An address and port to connect are required, and optionally a
    # list of alternative names to refer to this sink (e.g. location,
    # common name, etc.).
    #
    # Once connected, this instance becomes the sole controller of the
    # remote endpoint.
    #
    def __init__ (self, address, port=9988, names=None):
        self.address = address
        self.port = port
        self.id = address+':'+str(port)
        self.aliases = []
        if names:
            if type(names) == str:
                self.aliases = [names]
            elif type(names) == list:
                self.aliases = names
        self.status = {
            'connected':False,
            'state':'initializing',
            'track':None,
            'time':-1,
            'vol':100,
            'contact':0,
            'age':0,
        }
        self.last_conn_attempt = 0
        self.default_timeout = 0.15

    def connect (self):
        #
        # Connect to remote sink. This function may be called during
        # any command so throttle the connection attempts by time
        # since last attempt
        #
        # Always do this in async manner, to check connection status
        # send a command, e.g. query_state()
        #
        if self.status['connected']:
            return
        if time.time() - self.last_conn_attempt < 1:
            return
        self.last_conn_attempt = time.time()
        self.sock = socket.socket()
        try:
            print 'attempting reconnect on sock %s,%d' %(self.address,self.port)
            self.sock.settimeout(0)
            self.sock.connect((self.address,self.port))
            return 1
        except:
            return 0

    def close (self):
        self.sock.close()

    def report (self):
        return {
            'address': '%s:%d' %(self.address,self.port),
            'id': self.id,
            'aliases': self.aliases,
        }

    def handle_socket_exception (self):
        #
        # Exception handler for remote sink socket. For timeout case,
        # do nothing. For other cases, trigger a reconnect attempt
        # (async)
        #
        e = sys.exc_info()
        if e[0] == socket.timeout:
            print 'got socket timeout on sink %s:%d' %(self.address,self.port)
        else:
            self.status['connected'] = False
            print 'socket error %s, attempting to reconnect'
            print e
            self.connect()

    def send_command (self, op='-', track='-', time='-', vol='-'):
        #
        # Send command to remote sink. By default the command contains
        # no data so the sink will respond with its current status.
        #
        cmd = [ op, track, str(time), str(vol) ]
        try:
            self.sock.send('\n'+' '.join(cmd))
            self.status['connected'] = True
            return 1
        except:
            self.handle_socket_exception()
            return 0

    def recv_state (self, timeout=None):
        #
        # Get some response from the remote sink. There may be several
        # items lined up in the queue depending on timeouts and user
        # commands so return the latest only
        #
        if not timeout:
            timeout = self.default_timeout
        try:
            self.sock.settimeout(timeout)
            resp = self.sock.recv(2048)
            #
            # If something closed the pipe, we may get null string
            # back. Must try to reconnect in this case so trigger
            # exception
            #
            if resp == '':
                raise Exception
            self.status['connected'] = True
            resp = resp.split('\n')
            if len(resp) < 2:
                return 0
            resp = resp[-2].split(' ')
            if len(resp) == 4:
                self.status['state']   = resp[0]
                self.status['track']   = resp[1]
                self.status['time']    = resp[2]
                self.status['vol']     = resp[3]
                self.status['contact'] = time.time()
            else:
                return 0
        except:
            self.handle_socket_exception()
            return 0

    def query_state (self):
        #
        # Query the state of remote endpoint. Do so synchronously, in
        # the sense that we first query the remote sink and then wait
        # for response before returning. Contrary to other send
        # commands which do not wait for response.
        #
        # First clear the line
        #
        try:
            self.sock.settimeout(0)
            self.sock.recv(2048)
        except:
            pass
        self.send_command()
        self.recv_state()
        return self.get_state()

    def get_state (self):
        self.status['age'] = time.time() - self.status['contact']
        return self.status

    def get_time (self):
        self.query_state()
        try:
            return int(self.status['time'])
        except:
            return -1

    def send_heartbeat (self, t):
        self.send_command(op = 'hb', time  = t)

    def pause (self):
        return self.send_command(op = 'pause')

    def stop (self):
        return self.send_command(op = 'stop')

    def track (self,filename):
        return self.send_command(track = filename)

    def seek (self, time):
        return self.send_command(op = 'seek', time  = time)

    def vol (self, volume):
        return self.send_command(vol  = volume)

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
        self.targetvol = 50
        self.current_track = None

    def add_sink (self, sink):
        sink.connect()
        print 'adding sink %s to zone %s' %(sink.id,self.name)
        if self.is_playing():
            t = self.heartbeat_time()
            sink.send_command(track=self.current_track,time=t)
        self.sinks.append(sink)

    def remove_sink (self, sink_id):
        for s in self.sinks:
            if s.id == sink_id or sink_id in s.aliases:
                s.stop()
                self.sinks.remove(s)
                return s
        return 0

    def report (self):
        return {
            'name': self.name,
            'aliases': self.aliases,
            'sinks': [ s.report() for s in self.sinks ],
        }

    def playnext (self):
        # print 'in playnext, len of playlist %d' %(len(self.playlist))
        if len(self.playlist) > 0:
            n = self.playlist.pop(0)
            print 'got track to play %s' %n
            [ i.track(n) for i in self.sinks ]
            self.current_track = n
            self.lastadd = time.time()
            print 'added track at time %d'%self.lastadd

    def is_playing (self):
        status = [ i.get_state()['state'] for i in self.sinks ]
        if 'playing' in status:
            return True
        return False

    def is_stopped (self):
        status = [ i.get_state()['state'] for i in self.sinks ]
        # print 'status array for zone %s:' %(self.aliases)
        # print status
        if 'playing' in status:
            return False
        if 'stopped' in status:
            return True
        return False

    def heartbeat_time (self):
        #
        # This controller keeps track of the start time of track, and
        # obtains the playtime of each player. Using all times
        # together and rejecting any players very far off we can send
        # a best estimate of what the time should be with two or more
        # sinks.
        #
        sink_times = []
        start_time = time.time()
        for i in self.sinks:
            _time = i.get_time()
            if _time > 0:
                sink_times.append(_time)

        if len(sink_times) == 0:
            return '0@%0.5f' %(time.time())

        mean = (sum(sink_times))/(1.0*len(sink_times))
        #
        # Now reject any time significantly different from the mean
        # time (not including our own)
        #
        final_times = []
        for i in sink_times:
            if abs(i-mean) < HB_SYNC_LIMIT:
                final_times.append(i)
        #
        # Finally, the heartbeat time is the remaining mean. If there
        # are none left, players are too far apart and we must choose
        # the furthest along
        #
        if len(final_times) == 0:
            t = max(sink_times)
        else:
            t = int(sum(final_times)/(1.0*len(final_times)))
        end_time = time.time()
        # Now we must adjust for time recv/calc time
        c_delta = int((start_time-end_time)*1000)
        t = '%d@%0.5f' %(t+c_delta,end_time)
        return t

    def heartbeat (self):
        #
        # A heartbeat is used to synchronize players. Calculate the
        # appropriate hearbeat time and send to each player in our
        # zone. The players themselves decide how to adjust their play
        #
        t = self.heartbeat_time()

        for i in self.sinks:
            i.send_heartbeat(t)

    def run (self):
        while True:
            try:
                cmd = self.queue.get(True, 1)
            except:
                cmd = None
                pass
            if cmd:
                if cmd == 'quit':
                    print 'quitting zone managr'
                    return
                if len(self.sinks) == 0:
                    res={'_err':'zone has no sinks'}
                    cmd['_cmd'] = ''
                if cmd['_cmd'] == 'seek':
                    [ i.seek(cmd['time']) for i in self.sinks ]
                    res={'okay':cmd['time']}
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
                    self.targetvol = cmd['vol']
                    [ i.vol(cmd['vol']) for i in self.sinks ]
                    res={'status':'okay'}
                if cmd['_cmd'] == 'pause':
                    [ i.pause() for i in self.sinks ]
                    res={'status':'okay'}
                if cmd['_cmd'] == 'play':
                    self.sinks[0].play()
                    res={'status':'okay'}
                if cmd['_cmd'] == 'stop':
                    self.sinks[0].stop()
                    res={'status':'okay'}
                if cmd['_cmd'] == 'status':
                    res = {}
                    res['vol'] = self.targetvol
                    res['sinks'] = [ i.get_state() for i in self.sinks ]
                    res['time'] = int(sum([ int(i['time']) for i in res['sinks'] ])/(1.0*len(res['sinks'])))
                    for i in res['sinks']:
                        i['skew'] = int(i['time'])-int(res['time'])
                    res['state'] = res['sinks'][0]['state']
                    res['playlist'] = self.playlist
                if '_queue' in cmd and cmd['_queue']:
                    cmd['_queue'].put(res)

            if len(self.sinks) > 0:
                if max([ time.time() - i.status['contact'] for i in self.sinks ]) > 0.3:
                    self.heartbeat()
                    # print 'time from last add: %d, stopped: %s' %(time.time() - self.lastadd,self.is_stopped())
                    if self.is_stopped() and time.time() - self.lastadd > 1:
                        # print 'playing next'
                        self.playnext()

class main_manager (threading.Thread):
    def __init__ (self):
        threading.Thread.__init__(self)        
        self.queue = Queue.Queue()
        self.sinks = []
        self.zones = []
        self.media = {}

    def create_sink (self, address, port, names=None):
        s = sink(address, port, names=names)
        m = sink_manager(s,aliases=['zone%d' %(len(self.zones)+1)])
        m.start()
        self.zones.append(m)

    def move_sink (self, sink_id, zone):
        #
        # first find the sink and remove from current zone
        #
        zone = self.get_zone(zone)
        if not zone:
            return 0
        for i in self.zones:
            sink = i.remove_sink(sink_id)
            #
            # If we found it, add to desired zone
            #
            if sink:
                zone.add_sink(sink)
                return 1

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
            
    def quit (self):
        for i in self.zones:
            print 'sending quit to zones'
            i.queue.put('quit')
        for i in self.media:
            print 'sending quit to media'
            self.media[i].queue.put('quit')

    def run (self):
        self.serve()
            
    def serve (self):
        while True:
            request = self.queue.get()
            routed = False
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
            elif request['_class'] == 'sink':
                if request['_cmd'] == 'move':
                    if 'dst' not in request:
                        request['_queue'].put({'_err':'dst required'})
                        continue
                    if self.move_sink(request['_ctx'],request['dst']):
                        request['_queue'].put({'status':'okay'})
                    else:
                        request['_queue'].put({'_err':'failed to move %s to %s' %(request['_ctx'],request['dst'])})
                if request['_cmd'] == 'add':
                    try:
                        if ':' in request['_ctx']:
                            (addr,port) = request['_ctx'].split(':')
                        else:
                            (addr,port) = (request['_ctx'],9988)
                        self.create_sink(addr,int(port))
                        request['_queue'].put({'okay':'added sink %s:%s' %(addr,port)})
                    except:
                        request['_queue'].put({'_err':'could not add sink'})

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
        self.request_keepalive = False

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
            # print 'handler recieved request: %s' %headers
            if headers == '':
                print 'closed connection'
                self.status = 1
                if self.queue:
                    self.conn = self.queue.get()
                    if self.conn == 'quit':
                        print 'quitting api handler'
                        return
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
        try:
            self.serve()
        except:
            for i in self.threads:
                i.conn.close()
                i.queue.put('quit')

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
            if req == 'quit':
                print 'quitting media manager'
                return
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
                            add = {
                                '_class': 'player',
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

def killself (signum, frame):
    m.quit()
    print 'sent all quits'
    os.kill(os.getpid(), signal.SIGKILL)
    sys.exit()

logger = logging.getLogger('zp')
logger.setLevel(logging.NOTSET)

if __name__ == "__main__":
    logger.addHandler(logging.StreamHandler())
    logger.addHandler(logging.handlers.SysLogHandler())
    logger.setLevel(logging.INFO)
    logger.info('hello from logger')

    m = main_manager()
    a = api_server(9876,m.queue)
    mm = media_manager('/mnt/data/music/tracks.db')
    m.add_media(mm)

    # for testing
    m.create_sink('192.168.1.15',9988,'livingroom')
    # m.create_sink('192.168.1.10',9988,'laptop')

    signal.signal(signal.SIGINT, killself)

    mm.start()
    m.start()
    a.serve()
