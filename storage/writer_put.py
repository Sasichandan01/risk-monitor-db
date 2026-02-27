# storage/writer_put.py
from storage.base_writer import BaseWriter


class PutWriter(BaseWriter):
    """Writer for PUT (PE) options"""
    
    def __init__(self, connection_string):
        super().__init__(connection_string, 'PE')