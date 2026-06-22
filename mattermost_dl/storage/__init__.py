'''
    Pluggable storage backends.

    A backend is fed raw Mattermost API reply dicts and owns the persisted format.
    `makeBackend` selects one from configuration; only the directory/JSON backend
    exists today.
'''

from .base import ChannelArchive, DownloadServices, StorageBackend
from .directory_json.backend import DirectoryJsonBackend

DIRECTORY_JSON = 'directory-json'

__all__ = [
    'ChannelArchive',
    'DownloadServices',
    'StorageBackend',
    'DirectoryJsonBackend',
    'DIRECTORY_JSON',
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
    raise ValueError(f"Unknown storage format '{fmt}'.")
