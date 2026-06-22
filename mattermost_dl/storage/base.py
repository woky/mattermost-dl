'''
    Storage backend abstraction.

    A storage backend is *fed raw Mattermost API reply dicts* and owns everything
    about how a download is persisted (the on-disk/db format, resume buffering,
    metadata, asset files). The download pipeline (driver + saver) stays
    storage-independent: it never sees the backend's internal representation.

    Two layers:
      - `StorageBackend`  -- one per run; run-level setup/teardown + a factory for
        per-channel archives.
      - `ChannelArchive`  -- one per channel download; owns that channel's
        persistence lifecycle.

    The backend depends on the driver only through the small `DownloadServices`
    duck-typed interface below (no HTTP or business-object types leak across it).
'''

from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from typing import BinaryIO, Generator, Iterable, List, Optional, Tuple

from ..types import Id, PostHints, Time


class DownloadServices:
    '''
        The capabilities a backend needs from the driver, to resolve referenced
        entities and fetch asset bytes while committing a channel. The real
        `MattermostDriver` satisfies this by duck typing; it is documented here so
        backends depend on a narrow surface rather than the whole driver.
    '''
    def getUserById(self, id: Id) -> dict: ...          # raw user reply
    def getEmojiById(self, id: Id) -> dict: ...         # raw emoji reply (KeyError if gone)
    def getFileUrl(self, fileId: Id) -> str: ...
    def getEmojiUrl(self, emojiId: Id) -> str: ...
    def getAvatarUrl(self, userId: Id) -> str: ...
    def getRaw(self, url: str): ...                     # response with .headers / .content
    def storeUrlInto(self, url: str, fp: BinaryIO) -> None: ...


@dataclass
class ResumeState:
    '''
        Outcome of reconciling an interrupted download buffer before a fetch
        cycle. `resume` means a buffer was found and the fetch should continue
        below `resumeCursor` (the buffer's oldest post id) rather than restart;
        `priorCount` is how many posts the buffer already holds (for progress).
    '''
    resume: bool = False
    resumeCursor: Optional[Id] = None
    priorCount: int = 0


class StagingWriter(ABC):
    '''Appends raw posts (newest->oldest) to a channel's staging buffer.'''
    @abstractmethod
    def add(self, rawPost: dict) -> None:
        '''Persist one raw API post into the staging buffer.'''


class ChannelArchive(ABC):
    '''
        Per-channel persistence handle. Lifecycle, as driven by the saver:

            archive.committedExists() / isInterrupted()   # recovery gate
            archive.discard() | archive.backup(arbiter)    # recovery mechanics
            state = archive.reconcileBuffer()              # resume?
            newest = archive.committedNewestPost()         # incremental boundary
            with archive.stagingWriter(resume) as w:       # fetch newest->oldest
                ... w.add(rawPost) ...
            archive.commit(incremental=..., localNewestId=...)
    '''

    @abstractmethod
    def committedExists(self) -> bool:
        '''True if a non-empty committed archive already exists.'''

    @abstractmethod
    def isInterrupted(self) -> bool:
        '''True if a resumable (non-empty) interrupted buffer is present.'''

    @abstractmethod
    def discard(self) -> None:
        '''Delete the committed archive (and its metadata).'''

    @abstractmethod
    def backup(self, arbiter) -> bool:
        '''
            Move the committed archive aside as a backup. Returns False if the
            download should be skipped (the arbiter declined to overwrite an
            existing backup), True otherwise.
        '''

    @abstractmethod
    def committedNewestPost(self) -> Optional[Tuple[Id, Time]]:
        '''(id, createTime) of the newest committed post, or None if none.'''

    @abstractmethod
    def reconcileBuffer(self) -> ResumeState:
        '''Reconcile any interrupted buffer against the committed archive.'''

    @abstractmethod
    def stagingWriter(self, resume: bool) -> 'Generator[StagingWriter, None, None]':
        '''Context manager yielding a `StagingWriter` for the fetch cycle.'''

    @abstractmethod
    def commit(self, *, incremental: bool, localNewestId: Optional[Id]) -> None:
        '''
            Durably commit the staged posts, build channel metadata, fetch any
            referenced assets and clear the buffer.
        '''


class StorageBackend(ABC):
    '''Run-scoped storage target; a factory for per-channel archives.'''

    def __enter__(self) -> 'StorageBackend':
        self.open()
        return self

    def __exit__(self, *exc) -> bool:
        self.close()
        return False

    def open(self) -> None:
        '''Prepare the storage target (e.g. create the output directory).'''

    def close(self) -> None:
        '''Release the storage target.'''

    @abstractmethod
    def channelArchive(self, key: str, channel: dict, team: Optional[dict],
                       options, seedUsers: Iterable[dict] = ()) -> ChannelArchive:
        '''
            A `ChannelArchive` for one channel.

            `key`        stable, human-readable channel identity (output stem).
            `channel`    raw channel API reply.
            `team`       raw team API reply, or None for team-less channels.
            `options`    that channel's `ChannelOptions`.
            `seedUsers`  raw user replies known to participate up front (e.g. the
                         two parties of a direct channel).
        '''
