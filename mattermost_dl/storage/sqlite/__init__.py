'''
    The SQLite storage backend: a normalized, full-text-searchable single-file
    archive written directly during download.
'''

from .backend import SqliteBackend, SqliteChannelArchive

__all__ = ['SqliteBackend', 'SqliteChannelArchive']
