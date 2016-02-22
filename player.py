import vlc
import socket
import os
import sys

v=vlc.Instance()
p=v.media_player_new()

s=socket.socket()
s.bind(('0.0.0.0',9988))
s.listen(1)
c=s.accept()
track='-'

while True:
    cmd=c[0].recv(1024)
    if cmd == '':
        c=s.accept()
        cmd=c[0].recv(1024)
    cmd = cmd.split('\n')[-1].strip()
    try:
        print 'got command %s from remote controller' %cmd
        res = 'ok'
        if cmd == 'pause':
            p.pause()
        if cmd == 'play':
            p.play()
        if cmd == 'stop':
            p.stop()
        if cmd[0:3] == 'vol':
            if p.audio_set_volume(int(cmd[4:])) == 0:
                res = 'ok'
            else:
                res = 'err'
        if cmd[0:5] == 'track':
            path = '/mnt/'+cmd[6]+'/'+cmd[7]+'/'+cmd[6:]
            if os.path.exists(path):
                track = cmd[6:]
                m=v.media_new(path)
                p.set_media(m)
                p.play()
            else:
                print 'cannot find file %s' %cmd[6:]
                res = 'err badpath'
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
        t = p.get_time()
        vol = p.audio_get_volume()
        res = '%s %s %s %s %d' %(res,state,track,t,vol)
        print 'responding with %s' %res
        c[0].send(res+'\n')
    except:
        print 'caught exception',sys.exc_info()
        c[0].send('err exception')
