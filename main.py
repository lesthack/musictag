from datetime import datetime
from tinytag import TinyTag
from hashlib import md5
#import tagpy
import eyed3
import sqlite3
import magic
import time
import re
import os
import sys

# Path base of music
path_base_home = '/home/lesthack/Music/'
mime_types = ['audio/mpeg', 'application/octet-stream', 'audio/mp4', 'audio/x-wav', 'audio/x-flac', 'application/ogg']
exclude_types = ['video/x-ms-asf', 'application/msword', 'application/xml', 'video/mp4', 'application/CDFV2-corrupt', 'application/x-rar', 'application/vnd.rn-realmedia', 'application/pdf', 'text/rtf', 'inode/x-empty', 'video/x-flv']
last_scan = None

medialib = sqlite3.connect('medialib.db')
medialib.text_factory = str

def init():
    cursor = medialib.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS track(
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            hash TEXT,
            filename TEXT, 
            path TEXT, 
            artist TEXT, 
            album TEXT, 
            album_artist TEXT, 
            title TEXT,
            genre TEXT,
            duration DECIMAL,
            has_cover BOOLEAN,
            created_at DATETIME,
            updated_at DATETIME
        );        
    ''')
    medialib.commit()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS params(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            param_key TEXT,
            param_value TEXT
        );
    ''')
    medialib.commit()

def get_param(param_key):
    try:
        cursor = medialib.cursor()
        cursor.execute('SELECT param_value FROM params WHERE param_key=?',[param_key])
        row = cursor.fetchone()
        if row:
            return row[0]
    except Exception as e:
        'Error: ',e
    return None

if last_scan is None:
    _last_scan_ = get_param('last_scan')
    if _last_scan_ is None:
        last_scan = datetime.now()
    else:
        last_scan = datetime.strptime(_last_scan_, '%Y-%m-%d %H:%M:%S')

def update_last_scan():
    cursor = medialib.cursor()
    last_scan = get_param('last_scan')
    if last_scan:
        cursor.execute('UPDATE params SET param_value=? WHERE param_key=?', [datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'last_scan'])
    else:
        cursor.execute('INSERT INTO params(param_key, param_value) VALUES(?, ?)', ['last_scan', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
    medialib.commit()

def reset():
    cursor = medialib.cursor()
    cursor.execute('DROP TABLE track')
    medialib.commit()

def clean():
    cursor = medialib.cursor()
    cursor.execute('DELETE FROM track')
    medialib.commit()

def check_tracks(top=10):
    cursor = medialib.cursor()
    
    if top >= 0: result = cursor.execute('SELECT id, path, filename FROM track LIMIT ? OFFSET 0', [top])
    else:       result = cursor.execute('SELECT id, path, filename FROM track')

    ids = []
    
    for row in result:
        path_track = os.path.join(row[1],row[2])
        if not os.path.exists(path_track):
            ids.append('{}'.format(row[0]))
            print 'Removing {}: {}'.format(row[0], path_track)
    if len(ids) > 0:
        cursor.execute('DELETE FROM track WHERE id IN ({})'.format( ','.join(ids) ))
        medialib.commit()

def select(query):
    cursor = medialib.cursor()
    cursor.execute(query)
    return [row for row in cursor]

def change_track(f, parameters):
    try:
        t = magic.from_file(f, mime=True)
        if t not in mime_types:
            return 'Unsupported metadata'
        audiofile = eyed3.load(f)
        if audiofile:
            audiofile.tag.artist = u'{}'.format(parameters['artist'])
            audiofile.tag.album = u'{}'.format(parameters['album'])
            audiofile.tag.title = u'{}'.format(parameters['title'])
            audiofile.tag.save()
            cursor = medialib.cursor()
            query = 'UPDATE track SET artist=?, album=?, title=? WHERE hash=?'
            cursor.execute(query, [parameters['artist'], parameters['album'], parameters['title'], parameters['hash']])
            medialib.commit()
            return 'OK'
        else:
            return 'Metada error'
    except Exception as e:
        print '\nError: ',e,f
    return 'FAIL'

def analyze(top=10, modify=False):
    ptt = re.compile('\d\d\s')
    cursor = medialib.cursor()
    result = [row for row in cursor.execute('SELECT replace(path,"/home/lesthack/Music/",""), filename, path, hash FROM track WHERE title is null ORDER BY path LIMIT ? OFFSET 0', [top])]
    total = len(result)
    i = 1
    print 'Total: ', total
    for row in result:
        print '[{}]'.format(i),
        i+=1
        info_path = os.path.split(row[0])
        if len(info_path)<=1:
            print 'Solo 1: ', path
        elif len(info_path)==2:
            prob_artist = info_path[0].strip()
            prob_album = info_path[1].strip()
            prob_title = ptt.sub('',str(' '.join(row[1].split('.')[:-1])).replace('-',' ').replace('_',' ').strip())
            if len(prob_artist)>0 and prob_artist != 'Unknown Artist' and len(prob_album) > 0  and prob_album != 'Unknown Album':
                print 'Artist: ', prob_artist, ' - Album: ', prob_album, ' - Title: ', prob_title, '\t',
                if modify:
                    print '[{}]'.format(change_track(os.path.join(row[2], row[1]), {'artist': prob_artist, 'album': prob_album, 'title': prob_title, 'hash': row[3]}))
                else:
                    print '[NO]'
            else:
                print 'Analyze best: {} - {}'.format(row[0],row[1])
        elif len(info_path)>2:
            print 'Mas de 2: ', path

def get_track(track_hash):
    query = 'SELECT * FROM track WHERE hash=?'
    cursor = medialib.cursor()
    cursor.execute(query, [track_hash])
    return cursor.fetchone()

def insert_track(parameters, cursor_inherited=None):
    try:
        names = parameters.keys()
        values = [parameters[key] for key in names]
        track_hash = parameters['hash']
        
        if cursor_inherited:
            cursor = cursor_inherited
        else:
            cursor = medialib.cursor()
        
        _track_ = get_track(track_hash)
        has_execute = False

        if _track_ is None:
            query = 'INSERT INTO track({names}) VALUES({values});'.format(names=','.join(names), values=','.join(['?' for key in names]))
            cursor.execute(query, values)
            has_execute = True
            print 'Track {} Inserted: {}'.format(parameters['hash'], os.path.join(parameters['path'], parameters['filename']))
        elif datetime.strptime(_track_[12][:19], '%Y-%m-%d %H:%M:%S') > parameters['updated_at']:
            del parameters['hash']
            query = 'UPDATE track SET {} WHERE hash=?'.format(', '.join(['{}=?'.format(p) for p in parameters]))
            cursor.execute(query, parameters.values()+[track_hash])
            has_execute = True
            print 'Track {} Updated: {}'.format(track_hash, os.path.join(parameters['path'], parameters['filename']))
        
        if not cursor_inherited and has_execute:
            medialib.commit()
    except Exception as e:
        print 'Error: ',e
        print '\t', parameters

def parse_track(f, just_update=False, cursor_inherited=None):
    t = magic.from_file(f, mime=True)
    file_path, file_name = os.path.split(f)
    created_at = datetime.fromtimestamp(os.path.getctime(f))
    updated_at = datetime.fromtimestamp(os.path.getmtime(f))
    if just_update and (updated_at < last_scan):
        #print '{} already exists on database. No apparent change.'.format(f)
        return False
    if t in mime_types and file_name not in ['.DS_Store','._.DS_Store']:
        parameters = {
            'filename': file_name,
            'path': file_path,
            'hash': md5(f).hexdigest(),
            'created_at': created_at,
            'updated_at': updated_at
        }
        try:
            audiotag = TinyTag.get(f, image=True)
            if audiotag:
                parameters['artist'] = (u'', u'{}'.format(audiotag.artist))[audiotag.artist!=None]
                parameters['album'] = (u'', u'{}'.format(audiotag.album))[audiotag.album!=None]
                parameters['album_artist'] = (u'', u'{}'.format(audiotag.albumartist))[audiotag.albumartist!=None]
                parameters['title'] = (u'', u'{}'.format(audiotag.title))[audiotag.title!=None]
                parameters['genre'] = (u'', u'{}'.format(audiotag.genre))[audiotag.genre!=None]
                parameters['duration'] = audiotag.duration
                #parameters['image'] = sqlite3.Binary(('', audiotag.get_image())[audiotag.get_image()!=None])
                parameters['has_cover'] = audiotag.get_image()!=None
        except Exception as e:
            print 'Error: ', e
            print '\t', t, f
        insert_track(parameters, cursor_inherited)
    return True

def scan(limit=1000, path_base=path_base_home, just_update=False):
    cursor = medialib.cursor()
    for f in [os.path.join(dp, f) for dp, dn, fn in os.walk(path_base) for f in fn]:
        parse_track(f, just_update, cursor)
        # Limitando
        if limit > -1:
            if limit == 0:
                break
            limit-=1
    medialib.commit()
    update_last_scan()

def fast_update_scan():
    check_tracks(-1)
    scan(-1, path_base_home, True)

def update_scan():
    check_tracks(-1)
    scan(-1, path_base_home, False)

#init()
#scan(limit=100)
#medialib.close()
