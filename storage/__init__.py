# storage/__init__.py
from storage.writer_call import CallWriter
from storage.writer_put import PutWriter

__all__ = ['CallWriter', 'PutWriter']