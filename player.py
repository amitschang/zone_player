import vlc
import socket
import os
import sys
import urllib
import time

MAX_VOL = 130
DEF_PORT = 9988

if len(sys.argv) > 1:
    port = int(sys.argv[1])
else:
    port = DEF_PORT

v=vlc.Instance()
p=v.media_player_new()

s=socket.socket()
s.bind(('0.0.0.0',port))
s.listen(1)
c=s.accept()

cur_track = '-'
target_vol = 50

while True:
    try:
        cmd=c[0].recv(1024)
        if cmd == '':
            raise Exception
    except:
        print 'connection terminated, waiting for new master'
        c=s.accept()
        cmd=c[0].recv(1024)
        print 'got new master'
    try:
        print 'received command:',cmd
        cmd = cmd.split('\n')[-1].strip().split(' ')
        #
        # Command is in parts:
        #
        # "op track time vol"
        #
        # Where any individual part may be simply "-" character, which
        # means unspecified
        #
        op    = cmd[0]
        track = cmd[1]
        t  = cmd[2]
        vol   = cmd[3]

        if vol != '-':
            try:
                target_vol = int(vol)
                p.audio_set_volume(target_vol)
            except:
                pass

        if track != '-':
            path = urllib.unquote(track)
            #
            # If the path is absolute or a url, or really not like our
            # expected mounted path format, just try to play that
            #
            if '/' not in path:
                path = '/mnt/'+path[0]+'/'+path[1]+'/'+path
                print 'preparing to play track %s' %path
            if path[0:7] == 'http://' or os.path.exists(path):
                print 'playing track'
                m=v.media_new(path)
                p.set_media(m)
                p.play()
                cur_track = track
                #
                # After we start play, ensure we set vol to desired
                # level (in cases where the player could not while in
                # another state)
                #
                p.audio_set_volume(target_vol)
            else:
                print 'track not valid...'
                cur_track = '-'

        if op == 'pause':
            p.pause()

        if op == 'stop':
            p.stop()    

        if op != 'hb' and t != '-':
            #
            # When given time that is not specified as heartbeat,
            # simply seek the player to that time. This could be seek
            # command or just adding player to group whose in middle
            # of track
            #
            try:
                t = int(t)
                p.set_time(t)
            except:
                pass

        #
        # Get the state of player. This will be sent in response of
        # same format as command where op is replaced by state.
        #
        # Do this before heartbeat adjustment since it will query this
        # (we may get heartbeats out of turn)
        #
        _state = p.get_state()
        state = 'stopped'
        if _state == vlc.State.Playing:
            state = 'playing'
        elif _state == vlc.State.Buffering:
            state = 'playing'
        elif _state == vlc.State.Opening:
            state = 'opening'
        elif _state == vlc.State.Paused:
            state = 'paused'

        if op == 'hb' and t != '-':
            #
            # Do the synchronization jump and/or rate adjustment
            #
            (t,r) = t.split('@')
            t_delta = abs(p.get_time() - int(t))
            r_delta = time.time() - float(r)
            print 'got time offset %d with ref clock offset %0.5f' %(t_delta,r_delta)
            if t_delta > 70 and state == 'playing':
                # p.set_time(t)
                pass
            continue

        #
        # Get other status info, 
        t = p.get_time()
        vol = p.audio_get_volume()
        res = '%s %s %d %d' %(state,cur_track,t,vol)
        print 'responding with %s' %res
        c[0].send(res+'\n')
    except:
        print 'caught exception',sys.exc_info()
        c[0].send('- - - -\n')
