# main.py

import os
import logging
import configparser
from concurrent.futures import ProcessPoolExecutor

import consumer
import observer
from observer import SourcePathNotExists


def observer_main():
    observer.main()
    
    
def consumer_main():
    consumer.main()
    
    
if __name__ == '__main__':
    print('Start program')
    
    # load credentials
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), 'configs/main_conf.ini'))
    
    NUM_CPUs = int(config['MAIN']['CPU_QUANTITY'])
    
    # run two process in parallel
    tasks = [consumer_main, observer_main,]    
    try:
        with ProcessPoolExecutor(max_workers=NUM_CPUs) as executor:
            executor.submit(observer_main)
            executor.submit(consumer_main)
    except KeyboardInterrupt:
        logging.info('Program exit')
