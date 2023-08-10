# models.py

import configparser
import os
import logging
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pony.orm
from pony.orm import (
    Database,
    PrimaryKey,
    Required,
    Optional,
    Set,
    db_session,    
)


pony.options.CUT_TRACEBACK = False
db = Database()
db_connect = False


class Files(db.Entity):
    id = PrimaryKey(int, sql_type='bigint', auto=True),
    filename = Required(str, sql_type='text')
    path = Required(str, sql_type='text')
    inserted_at = Required(datetime, default=datetime.now())
    updated_at = Optional(datetime)


def connect_db(db_settings):
    attempt = 3
    while attemp > 0:
        try:
            db.bind(**db_settings)
            logging.debug('Successfully connected to DB')
            return True
        except Exception as err:
            logging.debug(f'Unsuccessful connection attemp: {err}')
            attemp -= 1
            time.sleep(5)
            
@db_session            
def inserRecDB(filedescription):
    # db.Files(
    #     filename=filedescription.filename,
    #     path=filedescription.destFilePath
    #     )
    # db.commit()
    pass
                        

# load connection params
config = configparser.ConfigParser()
config_root = Path(os.path.dirname(__file__)).parent
config.read(os.path.join(config_root, 'configs/consumer_conf.ini'))
db_settings = dict(config['DATABASE'].items())
db_settings['port'] = int(db_settings['port'])
db_connect = connect_db(db_settings)
    
try:
    db.generate_mapping(create_tables=True)
except Exception as err:
    db_connect = False
    logging.error(f'Error mapping DB: {err}')
    
    
    
