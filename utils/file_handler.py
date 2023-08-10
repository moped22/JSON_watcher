# file_handler.py

import os
import shutil
from pathlib import Path


class FileDescription:
    def __init__(self, file_path: str):
        self.destFilePath = file_path
        self.filename = os.path.basename(file_path)
        

def moveToTemp(src: str, to: str):    
    to_temp_path = os.path.join(to, 'temp')
    if not os.path.isdir(to_temp_path):
        os.makedirs(to_temp_path)
    shutil.move(src, to_temp_path)
    