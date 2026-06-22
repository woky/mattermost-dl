'''
    Cross-cutting value types shared across the whole program.

    These are deliberately storage-independent: the download pipeline (driver,
    saver orchestration, config, recovery) passes raw Mattermost API dicts plus
    these small utilities. They are NOT "business objects" -- the entity model
    and the on-disk format live privately inside a storage backend.
'''

from dataclasses import dataclass
from datetime import datetime
from functools import total_ordering
from typing import NewType, Optional, Union

__all__ = [
    'StoreError',
    'EntityLocator',
    'Id',
    'Time',
    'PostHints',
]


class StoreError(Exception):
    '''Failed to load from the storage of downloaded content.'''
    pass


class EntityLocator:
    def __init__(self, info: dict):
        ok = False
        for key in info:
            if key in ('id', 'name', 'internalName'):
                if ok:
                    raise ValueError('EntityLocator with multiple (possibly conflicting) identificators.')
                ok = True
        else:
            if not ok:
                raise ValueError('EntityLocator has no identificator.')
        if 'id' in info:
            self.id: Id = info['id']
        if 'name' in info:
            self.name: str = info['name']
        if 'internalName' in info:
            self.internalName: str = info['internalName']
    def __repr__(self) -> str:
        return f'EntityLocator({self.__dict__})'


@total_ordering
class Time:
    def __init__(self, time: Union[int, str]):
        self._time: int
        # time is unix timestamp in miliseconds
        if isinstance(time, int):
            self._time = time
        else:
            assert isinstance(time, str)
            self._time = int(datetime.fromisoformat(time).timestamp() * 1000)

    # Returns unix timestamp in miliseconds
    @property
    def timestamp(self) -> int:
        return self._time
    def __eq__(self, other: 'Time'):
        return self._time == other._time
    # Defining __eq__ makes instances unhashable; Python 3.11+ then rejects
    # Time() used as a dataclass field default. Hash by value.
    def __hash__(self):
        return hash(self._time)
    def __lt__(self, other: 'Time'):
        return self._time < other._time
    # Needed to silence linter
    def __gt__(self, other: 'Time'):
        return self._time > other._time

    def __str__(self):
        fmt = datetime.fromtimestamp(self._time/1000).isoformat()
        fractionStart = fmt.rfind('.')
        if fractionStart != -1:
            fmt = fmt[:fractionStart]
        return fmt
    def __repr__(self):
        return f"'{datetime.fromtimestamp(self._time/1000).isoformat()}'"

    def toStore(self) -> int:
        return self.timestamp


Id = NewType('Id', str)


@dataclass
class PostHints:
    '''
        Ordering context for a post emitted by the download walk: how many posts
        have been processed so far, plus the ids of the chronologically adjacent
        posts (None at a channel boundary). Used to drive maxCount stops and to
        record neighbour ids in a storage backend's metadata.
    '''
    processedCount: int = 0
    # Id of post directly chronologically preceding current one. None if first in channel.
    postIdBefore: Optional[Id] = None
    # Id of post directly chronologically succeeding current one. None if last in channel.
    postIdAfter: Optional[Id] = None
