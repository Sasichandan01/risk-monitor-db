# storage/writer_call.py
from storage.base_writer import BaseWriter


class CallWriter(BaseWriter):
    """Writer for CALL (CE) options"""
    
    def __init__(self, connection_string):
        super().__init__(connection_string, 'CE')