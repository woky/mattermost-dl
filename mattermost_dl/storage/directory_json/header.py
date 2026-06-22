'''
    Contains the history storage format and related utilites

    The format currently looks like
    channelname.meta.json
        - contains json equivalent of ChannelHeader
    channelname.data.json
        - contains newline separated sequence of compact json serializations of Post
        - posts are always ordered oldest->newest by timestamp and are continuous
          (no posts missing within the covered interval)
    channelname.data.json.tmp
        - transient download buffer holding posts newest->oldest as they are fetched
        - on completion it is reversed and appended into channelname.data.json, then deleted
        - its presence means a prior download was interrupted and can be resumed
'''

from ...common import *

from .entities import *
from ...config import ChannelOptions
from ...jsonvalidation import validate as validateJson, formatValidationErrors
from ... import jsonvalidation

from collections.abc import Iterable
import json
import jsonschema
# HACK: Pyright linter doesn't recognize special meaning of ClassVar from .common in dataclasses
from typing import ClassVar


def _iterLinesBackward(f: BinaryIO, size: int, chunkSize: int = 65536
        ) -> Generator[Tuple[bytes, int], None, None]:
    '''
        Yields `(lineBytes, startOffset)` for each newline-terminated line in the
        byte range `[0, size)` of `f`, newest (last) line first. `lineBytes`
        excludes the terminating newline; `startOffset` is the line's first byte.

        A trailing partial line (bytes after the final newline, e.g. left by a
        crash mid-write) is skipped. The file is read backward in bounded chunks,
        so only the lines actually consumed are held in memory.
    '''
    pos = size
    buf = b''  # always equals f[pos : pos + len(buf)]
    sawTerminator = False  # have we passed the final newline (and dropped any partial)?
    while pos > 0:
        readSize = min(chunkSize, pos)
        pos -= readSize
        f.seek(pos)
        buf = f.read(readSize) + buf
        while True:
            nl = buf.rfind(b'\n')
            if nl == -1:
                break
            segment = buf[nl + 1:]
            segStart = pos + nl + 1
            buf = buf[:nl]
            if not sawTerminator:
                # Bytes after the very last newline are an incomplete trailing line.
                sawTerminator = True
                continue
            if segment:
                yield segment, segStart
    if buf and sawTerminator:
        # Leftover is the first line in the file (no preceding newline).
        yield buf, 0


def iterPostsBackward(filename: Path) -> Generator[bytes, None, None]:
    '''
        Yields the raw stored post lines (without trailing newline) of a
        newline-delimited post file in reverse file order.

        For a newest->oldest buffer this yields posts oldest->newest, the order
        in which they are appended into the oldest->newest data file on commit.
    '''
    with open(filename, 'rb') as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        if size == 0:
            return
        for line, _ in _iterLinesBackward(f, size):
            yield line


def trimDataFileNewerThan(filename: Path, boundaryTime: Time) -> None:
    '''
        Crash-safety reconcile for the oldest->newest data file.

        Removes from the end of `filename` every complete line whose createTime is
        `>= boundaryTime`, plus any trailing partial line. Used before re-committing
        a resumed download buffer: every buffered post is strictly newer than the
        committed archive, so trimming the tail down to `boundaryTime` (the buffer's
        oldest post) undoes a partial or complete prior append, making the re-commit
        append each buffered post exactly once.
    '''
    if not filename.is_file():
        return
    with open(filename, 'r+b') as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        if size == 0:
            return
        keepLen = 0
        for line, startOffset in _iterLinesBackward(f, size):
            try:
                createTime = json.loads(line)['createTime']
            except (json.JSONDecodeError, KeyError, TypeError):
                continue  # unparseable tail line -> drop it too
            if createTime < boundaryTime.timestamp:
                keepLen = startOffset + len(line) + 1
                break
        if keepLen < size:
            f.truncate(keepLen)


def countStoredPosts(filename: Path) -> int:
    '''
        Number of complete (newline-terminated) post lines in a stored post file.
        A trailing partial line (no newline) is not counted. Cheap: counts newline
        bytes without parsing.
    '''
    if not filename.is_file():
        return 0
    count = 0
    with open(filename, 'rb') as f:
        while True:
            chunk = f.read(1 << 20)
            if not chunk:
                break
            count += chunk.count(b'\n')
    return count


def readLastStoredPost(filename: Path) -> Optional[Tuple[Id, Time, int]]:
    '''
        Reads the last complete post line of a newline-delimited post file.

        Returns `(postId, createTime, validByteLength)` or `None` if there is no
        complete post (missing/empty file or only a truncated partial line).
        `validByteLength` is the file length up to and including the terminating
        newline of that last complete post; any bytes beyond it are an incomplete
        write that can be trimmed.
    '''
    if not filename.is_file():
        return None
    with open(filename, 'rb') as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        if size == 0:
            return None
        for line, startOffset in _iterLinesBackward(f, size):
            try:
                info = json.loads(line)
                return Id(info['id']), Time(info['createTime']), startOffset + len(line) + 1
            except (json.JSONDecodeError, KeyError, TypeError):
                return None
    return None


@dataclass
class PostStorage(JsonMessage):
    '''
        Posts are always stored oldest->newest and continuous (no gaps in the
        covered time interval). Note that if count == 0, other fields do not have
        to hold meaningful values.
    '''

    # Number of posts
    count: int = 0
    byteSize: int = 0
    # If the first post is not completely first, here is post that we known to be before it (respecting ordering)
    postIdBeforeFirst: Optional[Id] = None
    # Create time of first post in the storage or some time point before it, if there are no posts up to that
    beginTime: Time = Time(0)
    firstPostId: Id = Id('')
    # Create time of last post in the storage or some time point after it, if there are no posts up to that
    endTime: Time = Time(0)
    lastPostId: Id = Id('')
    # If the post is not latest, this one shall be after it (respecting ordering)
    postIdAfterLast: Optional[Id] = None

    @staticmethod
    def fromOptions(options: ChannelOptions) -> 'PostStorage':
        '''
            Constructs fresh, partially initialized storage suitable
            for incremental filling by `addSortedPost`, followed
            by correcting the byteSize.

            Storage is always oldest->newest and continuous: downloads run
            newest->oldest into a transient buffer and are reversed into the data
            file on commit, where this storage is built in ascending order.
        '''
        storage = PostStorage(misc={})
        if options.postsAfterTime is not None:
            storage.beginTime = options.postsAfterTime
        return storage


    def addSortedPost(self, p: Post, postOrderHints: PostHints):
        '''
            Records one post into the storage metadata. Posts must be fed in
            ascending (oldest->newest) order, as produced by the commit pass.
        '''
        if self.count == 0:
            self.firstPostId = p.id
            if self.beginTime == Time(0):
                self.beginTime = p.createTime
            self.postIdBeforeFirst = postOrderHints.postIdBefore
        self.lastPostId = p.id
        self.endTime = p.createTime
        self.postIdAfterLast = postOrderHints.postIdAfter

        self.count += 1


    def update(self, other: 'PostStorage'):
        '''
            Updates old post storage with freshly downloaded content.
            Does not represent concatenation of arbitrary storages.
        '''
        if other.count > 0:
            assert self.lastPostId == other.postIdBeforeFirst
            self.count += other.count
            self.byteSize = other.byteSize
            self.lastPostId = other.lastPostId
            self.endTime = other.endTime
            self.postIdAfterLast = other.postIdAfterLast

    @classmethod
    def memberFromStore(cls, memberName: str, jsonMemberValue: Any) -> Any:
        if memberName in ('firstPostId', 'lastPostId', 'postIdBeforeFirst', 'postIdAfterLast'):
            return jsonMemberValue
        return NotImplemented


@dataclass
class ChannelHeader:
    _schemaValidator: ClassVar[jsonschema.Draft7Validator]

    channel: Channel
    team: Optional[Team] = None  # Missing if channel is not scoped under team
    # Missing if channel has no messages, so `storage.count > 0` shall hold
    # (as long as header is not currently getting filled)
    storage: Optional[PostStorage] = None
    # Users that appeared in conversations
    usedUsers: Set[User] = dataclassfield(default_factory=set)
    # Emojis that appeared in conversations
    usedEmojis: Set[Emoji] = dataclassfield(default_factory=set)

    @classmethod
    def fromStore(cls, info: Any):
        '''
            Loading previously saved header.
        '''
        def onWarning(w):
            if isinstance(w, jsonvalidation.UnsupportedVersion):
                logging.warning(
                    f'Loading channel from future version {w.found}, currently supported version is 1. It may not be loadable and some data may be lost.')
            else:
                logging.warning(f"Channel header encountered warning '{w}', it may not be loadable correctly.")
        def onError(e):
            if isinstance(e, jsonvalidation.BadObject):
                logging.error(f"Failed to load channel header, loaded json object has unsupported type {e.recieved}.")
            else:
                assert isinstance(e, Iterable)
                logging.error("Channel header didn't match expected schema. " + formatValidationErrors(e))
            raise StoreError
        info = validateJson(info, cls._schemaValidator,
                            acceptedVersion='1', onWarning=onWarning, onError=onError)

        self = cast(ChannelHeader, ClassMock())
        self.channel = Channel.fromStore(info['channel'])
        if 'users' in info:
            self.usedUsers = set()
            for userInfo in info['users']:
                self.usedUsers.add(User.fromStore(userInfo))
        if 'team' in info:
            self.team = Team.fromStore(info['team'])
        if 'storage' in info:
            storage = PostStorage.fromStore(info['storage'])
            if storage.count != 0:
                self.storage = storage
        if 'emojis' in info:
            self.usedEmojis = set()
            for emojiInfo in info['emojis']:
                self.usedEmojis.add(Emoji.fromStore(emojiInfo))
        return cls(**self.__dict__)

    def update(self, other: 'ChannelHeader'):
        self.channel = other.channel
        if other.team is not None:
            self.team = other.team
        if other.storage is not None:
            if self.storage is not None:
                self.storage.update(other.storage)
            else:
                self.storage = copy(other.storage)
        self.usedUsers = other.usedUsers | self.usedUsers
        self.usedEmojis = other.usedEmojis | self.usedEmojis

    def toStore(self) -> dict:
        content: Dict[str, Any] = {
            'version': '1'
        }
        if self.team:
            content.update(team=self.team.toStore(includeChannels=False))
        content.update(channel=self.channel.toStore())
        if self.storage is not None and self.storage.count > 0:
            content.update(storage=self.storage.toStore())
        if self.usedUsers:
            content.update(users=[u.toStore() for u in self.usedUsers])
        if self.usedEmojis:
            content.update(emojis=[e.toStore() for e in self.usedEmojis])

        return content

    @staticmethod
    def loadSchemaValidator() -> jsonschema.Draft7Validator:
        with open(sourceDirectory(__file__)/'header.schema.json') as schemaFile:
            return jsonschema.Draft7Validator(json.load(schemaFile))

ChannelHeader._schemaValidator = ChannelHeader.loadSchemaValidator()
