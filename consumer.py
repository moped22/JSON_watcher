# consumer.py

import os
import logging
import logging.handlers
import configparser
import time
import shutil
import datetime

from utils import (
    FileDescription,
    deleteRedisKey,
    hgetallRedisRec,
    getallRedisKeys,
    moveToTemp,
    inserRecDB,
)
from utils.file_handler import moveToTemp
from observer import SourcePathNotExists
from db.models import inserRecDB


def main():
    print('Start Consumer')
    
    # load credentials
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
        f'log_Consumer_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
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

    while True:
        # load credentials
        config.read(
            os.path.join(os.path.dirname(__file__), 'configs/consumer_conf.ini')
        )
        destroot = config['MAIN']['DESTINATION_FOLDER']
        rkeys = getallRedisKeys()
        if not rkeys:
            time.sleep(5)
            continue
        for dct in map(hgetallRedisRec, rkeys):
            filedescription = FileDescription(dct['src_path'])
            if not filedescription:
                continue
            
            # move file to destination folder
            dst_path = os.path.join(
                destroot, 
                f"""{datetime.datetime.now().strftime('%Y%m%d_%H-%M-%S')}_{
                        os.path.basename(dct['src_path'])}"""
            )
            if not os.path.exists(os.path.dirname(dst_path)):
                os.makedirs(os.path.dirname(dst_path))
            shutil.move(dct['src_path'], dst_path)
            
            # if copied succesfully - delete record from redis
            if os.path.exists(dst_path):
                deleteRedisKey(os.path.basename(dct['src_path']))
                logging.info(
                    f'successdully copied {dct["src_path"]} to {dst_path}')
            else:
                logging.warning(
                    f'Error: can not move {dct["src_path"]} to  destination folder. File will be skipped'
                )
                continue
            
            # write file_info to DB
            try:
                print('Test status: OK - for file {dst_path}')  # inserRecDB(filedescription)
            except Exception as err:
                logging.error(
                    f'Error occurred when write to DB: {err}\n{dst_path} will be moved to temp'
                )
                moveToTemp(src=dst_path, to=destroot)
                continue
            
            
if __name__ == '__main__':
    main()