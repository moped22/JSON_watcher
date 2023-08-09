# redis_tools.py

import os
import json
import logging
import configparser
from pathlib import Path

from redis import Redis


# load credentials
config = configparser.ConfigParser()
config_root = Path(os.path.dirname(__file__)).parent
config.read(os.path.join(config_root, 'configs/main_conf.ini'))
redis_settings = dict(config['REDIS'].items())
redis_settings['port'] = int(redis_settings['port'])
redis_settings['db'] = int(redis_settings['db'])


# wrapper for redis setups downloading
def load_redis_setups(func):
    def inner():
        return func(redis_settings)
    return inner


@load_redis_setups
def getConnRedis(settings):
    try:
        conn_redis = Redis(**settings)
        return conn_redis
    except Exception as err:
        raise ConnectionError(
            f'Can not connect to Redis: {err}'
        )
        
        
def checkRedis():
    conn_redis = getConnRedis()
    if conn_redis.ping() == 'pong':        
        return True
    else:
        return False
    
    
def setRedisRec(rec_data):
    try:
        conn_redis = getConnRedis()
        file_key = rec_data.get('file_key')
        file_data = rec_data.get('file_data')
        dump_data = json.dumps(file_data, indent=4, sort_keys=True)
        conn_redis.set(file_key, dump_data, ex=172800) # record will expired after 48 hours
        return file_key
    except Exception as err:
        logging.error(f'Set Redis record error: {err}')
        
        
def getRedisRec(key):
    try:
        conn_redis = getConnRedis()
        rsb = conn_redis.get(key)
        return json.loads(rsb)
    except Exception as err:
        logging.error(f'Get Redis key={key} error: {err}')
        return False
    

def renameRedisRec(rec_data):
    try:
        file_key = rec_data.get('file_key')
        file_data = rec_data.get('file_data')
        file_newkey = file_data.get('dest_path')
        file_newkey = os.path.basename(file_newkey) if file_newkey else file_key
        new_data = {}
        new_data['file_key'] = file_newkey
        new_data['file_data'] = file_data
        setRedisRec(new_data)
        deleteRedisKey(file_key)
        return file_newkey
    except Exception as err:
        logging.error(f'Rename Redis record error: {err}')
        return False
    
    
    def convertData(data):
        if isinstance(data, bytes): return data.decode('utf-8')
        if isinstance(data, dict): return dict(map(convertData, data.items()))
        if isinstance(data, tuple): return map(convertData, data)
        return data
    
    
    def hmsetRedisRec(rec_data):
        try:
            conn_redis = getConnRedis()
            file_key = rec_data.get('file_key')
            file_data = rec_data.get('file_data')
            conn_redis.hmset(file_key, file_data)
            conn_redis.expire(file_key, 172800)
            return file_key
        except Exception as err:
            logging.error(f'HMSet Redis record error: {err}')
            return False
    
    
    def hgetallRedisRec(key):
        try:
            conn_redis = getConnRedis()
            rsb = conn_redis.hgetall(key)
            return converData(rsb)
        except Exception as err:
            logging.error(f'HGet all Redis records error: {err}')
            return False
        
        
    def hrenameRedisRec(rec_data):
        try:
            file_key = rec_data.get('file_key')
            file_data = rec_data.get('file_data')
            file_newkey = file_data.get('dest_path')
            file_newkey = os.path.basename(file_newkey) if file_newkey else file_key
            rec_data['file_key'] = file_newkey
            hmsetRedisRec(rec_data)
            deleteRedisKey(file_key)
            return file_newkey
        except Exception as err:
            logging.error(f'HRename Redis record error: {err}')
            return False
        
        
    def deleteRedisKey(file_key):
        try:
            conn_redis = getConnRedis()
            conn_redis.delete(file_key)
            return True
        except Exception as err:
            logging.error(f'Delete Redis key={file_key} error: {err}')
            return False
        
    
    def deleteRedisRec(rec_data):
        try:
            conn_redis = getConnRedis()
            file_key = rec_data.get('file_key')
            conn_redis.delete(file_key)
            return True
        except Exception as err:
            logging.error(f'Delete Redis record error: {err}')
            return False
        
        
    def getallRedisKeys():
        try:
            conn_redis = getConnRedis()
            keys = [key.decode('utf-8') for key in conn_redis.scan_iter('*', count=10)]            
            return keys.sort()        
        except Exception as err:
            logging.error(f'Get all Redis keys error: {err}')
            return False
        
        
    def delAllRedisRecs():
        try:
            conn_redis = getConnRedis()
            for key in conn_redis.scan_iter('*'):
                conn_redis.delete(key)
            return True
        except Exception as err:
            logging.error(f'Delete all Redis records error: {err}')
            return False
        
        
if __name__ == '__main__':
    allKeys = getAllRedisKeys()
    print(allKeys)
    delAllRedisRecs()
    allKeys = getAllRedisKeys()
    print(allKeys)