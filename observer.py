# observer.py

import os
import logging
import logging.handlers
import asyncio
import datetime
import configparser
from contextlib import suppress 
from watchdog.observers import Observer
from watchdog.events import (
    # FileSystemEventHandler,
    # RegexMatchingEventHandler,
    PatternMatchingEventHandler,
)

from utils import (
    hmsetRedisRec,
    hrenameRedisRec,
    deleteRedisRec,
)

class SourcePathNotExists(Exception):
    pass


class MyPaternEventHandler(PatternMatchingEventHandler):
    def __init__(self, loop, *args, **kwargs):
        self._loop = loop
        super().__init__(*args, **kwargs)
        
    def on_any_event(self, event):
        asyncio.run_coroutine_threadsafe(watch_handler(event), self._loop)


async def watch_handler(event):
    if event.event_type != 'modified' and not event.is_directory:
        rec_data = {}
        file_data = {}
        rec_data['file_key'] = os.path.basename(event.src_path)
        
        dt = datetime.datetime.now()
        file_data['sdTime'] = dt.strftime("%Y%m%d+%H-%M-%S")
        file_data['event_type'] = event.event_type
        src_path = event.src_path
        file_data['src_path'] = src_path
        file_data['dest_path'] = event.dest_path if event.event_type == 'moved' else ''
        rec_data['file_data'] = file_data
        
        if not event.is_directory:
            if event.event_type == 'created':
                file_size = -1
                # checks when file full downloaded
                while file_size != os.pathgetsize(event.src_path):
                    file_size = os.path.getsize(event.src_path)
                    await asyncio.sleep(0.01)
                logging.debug('Created file {event.src_path}')
                hmsetRedisRec(rec_data, db=1)
            elif event.key[0] == 'moved':
                hrenameRedisRec(rec_data, db=1)
            elif event.key[0] == 'deleted':
                deleteRedisRec(rec_data, db=1)
                
                
def main():
    print('Start Observer')
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'configs/observer_conf.ini'))
    
    loglevel = config['MAIN']['LOGLEVEL'].lower()
    rootdir = config['MAIN']['ROOT_FOLDER']
    
    if not os.path.exists(rootdir):
        raise SourcePathNotExists(f'Source path does not exists: {rootdir}')
    
    # setup logging
    DEBUG_FORMAT = f'%(asctime)s | {os.path.basename(__file__)} | %(levelname)-7s : %(funcName) : %(message)'
    MESSAGE_ONLY = f'%(asctime)s | {os.path.basename(__file__)} | %(levelname)-7s : %(message)'
    logdirpath = os.path.join(os.path.dirname(__file__), 'Logs')
    if not os.path.exists(logdirpath):
        os.makedirs(logdirpath)
    logfilename = os.path.join(
        logdirpath,
        f'log_Observer_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    )
    logging.basicConfig(
        level=logging.DEBUG if (loglevel) == 'debug' else logging.INFO,
        filename=logfilename,
        format=DEBUG_FORMAT if (loglevel) == 'debug' else MESSAGE_ONLY,
        datefmt='%Y-%m-%d %H:%M:%S',
        force=True,
        encoding='utf-8',
    )
    handler = logging.handlers.RotatingFileHandler(
        logfilename,
        mode='w',
        maxBytes=314_572_800, # 300 Mb
        backupCount=7,
        encoding='utf-8',
    )
    handler.setFormatter(DEBUG_FORMAT if (loglevel) == 'debug' else MESSAGE_ONLY)
    handler.setLevel(logging.DEBUG if (loglevel) == 'debug' else logging.INFO)
    logging.getLogger(os.path.basename(__file__)).addHandler(handler)
    
    loop = asyncio.new_event_loop()
    
    event_handler = MyPaternEventHandler(
        loop,
        patterns=['*.json'],
        ignore_directories=True,
        case_sensitive=False,
    )
    
    observer = Observer()
    observer.schedule(event_handler, rootdir, recursive=True)
    observer.start()
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        observer.stop()
        try:
            for task in asyncio.all_tasks():
                task.cancel()
                with suppress(asyncio.CancelledError):
                    loop.run_until_complete(task)
        except RuntimeError:
            pass
    finally:
        loop.close()
        observer.stop()
        observer.join()
        
        
if __name__ == '__main__':
    main()
        
    