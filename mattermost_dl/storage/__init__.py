'''
    Pluggable storage backends.

    A backend is fed raw Mattermost API reply dicts and owns the persisted format.
    `makeBackend` selects one from configuration: the directory/JSON format or the
    normalized SQLite database.
'''

from .base import ChannelArchive, DownloadServices, StorageBackend
from .directory_json.backend import DirectoryJsonBackend
from .sqlite.backend import SqliteBackend

DIRECTORY_JSON = 'directory-json'
SQLITE = 'sqlite'

__all__ = [
    'ChannelArchive',
    'DownloadServices',
    'StorageBackend',
    'DirectoryJsonBackend',
    'SqliteBackend',
    'DIRECTORY_JSON',
    'SQLITE',
    'makeBackend',
]


def makeBackend(config, services, progress) -> StorageBackend:
    '''
        Build the storage backend selected by `config.outputFormat`.

        `services` supplies the driver capabilities the backend needs (entity
        resolution + asset fetching); `progress` is the run's progress reporter.
    '''
    fmt = getattr(config, 'outputFormat', DIRECTORY_JSON)
    if fmt == DIRECTORY_JSON:
        return DirectoryJsonBackend(config, services, progress)
    if fmt == SQLITE:
        return SqliteBackend(config, services, progress)
    raise ValueError(f"Unknown storage format '{fmt}'.")
