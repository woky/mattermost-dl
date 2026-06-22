'''
    The directory-of-JSON storage backend: the project's original on-disk format,
    now isolated behind the `StorageBackend` interface.

    Per channel it writes:
      <key>.data.json       posts oldest->newest, one compact JSON object per line
      <key>.meta.json       channel/team/users/emojis + storage bookkeeping
      <key>.data.json.tmp   transient newest->oldest download buffer (resume marker)

    It is fed raw Mattermost API post dicts and converts them, via the private
    entity model (`Post.fromMattermost(...).toStore()`), into the camelCase format.
'''

import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from copy import deepcopy
from mimetypes import guess_extension
from pathlib import Path
from typing import (Any, Callable, Collection, Dict, Generator, Iterable, List,
                    Optional, Tuple, TypeVar, cast)

from ...config import ChannelOptions, ConfigFile
from ...types import Id, PostHints, Time
from ..base import (ChannelArchive, DownloadServices, ResumeState, StagingWriter,
                    StorageBackend)
from .entities import Channel, Emoji, FileAttachment, Post, Team, User
from .header import (ChannelHeader, PostStorage, countStoredPosts,
                    iterPostsBackward, readLastStoredPost, trimDataFileNewerThan)


def _jsonDump(obj, fp):
    '''Serialize, letting nested entities serialize themselves via toStore.'''
    def fallback(o):
        if hasattr(o, 'toStore'):
            return o.toStore()
        return str(o)
    json.dump(obj, fp, default=fallback, ensure_ascii=False)


class DirectoryJsonBackend(StorageBackend):
    def __init__(self, config: ConfigFile, services: DownloadServices, progress):
        self.config = config
        self.services = services
        # The one progress reporter for the run; asset downloads render tasks on it.
        self.progress = progress
        self.outputDirectory: Path = config.outputDirectory

    def open(self) -> None:
        if not self.outputDirectory.is_dir():
            self.outputDirectory.mkdir()

    def channelArchive(self, key: str, channel: dict, team: Optional[dict],
                       options: ChannelOptions, seedUsers: Iterable[dict] = ()) -> ChannelArchive:
        return DirectoryJsonChannelArchive(self, key, channel, team, options, list(seedUsers))

    # ----- assets -------------------------------------------------------------
    # Asset layout (the emojis/, avatars/, <key>--files/ subdirs and the file
    # names recorded into the header) is part of this format, so it lives here.

    def storeFile(self, url: str, filename: str, directoryName: Path,
                  suffix: Optional[str] = None, redownload: bool = False) -> str:
        if '/' in filename:
            logging.warning(f'Refusing to store file with name "{filename}"')
            raise ValueError

        httpResponse = self.services.getRaw(url)
        if suffix is None:
            if 'content-type' in httpResponse.headers:
                contentType = httpResponse.headers['content-type']
                suffixIdx = contentType.find(';')
                if suffixIdx != -1:
                    contentType = contentType[:suffixIdx]
                suffix = guess_extension(contentType)
                if suffix is None:
                    crudeParse = re.match(r'^[^/]+/(\S+)$', contentType)
                    if crudeParse is not None:
                        suffix = '.'+crudeParse[1]
                    else:
                        logging.warning(f"Can't guess extension from content type '{contentType}', leaving empty.")
                        suffix = ''
            else:
                suffix = ''
        assert isinstance(suffix, str)
        fullFilename = directoryName / (filename + suffix)
        if fullFilename.exists() and not redownload:
            return filename + suffix
        with open(fullFilename, 'wb') as output:
            self.services.storeUrlInto(url, output)
        return filename + suffix

    FileEntity = TypeVar('FileEntity')
    def processFiles(self, entities: Collection['DirectoryJsonBackend.FileEntity'],
            directoryName: str, entitiesName: str,
            getFilenameFromEntity: Callable[['DirectoryJsonBackend.FileEntity'], str],
            shouldDownload: Callable[['DirectoryJsonBackend.FileEntity'], bool],
            getUrlFromEntity: Callable[['DirectoryJsonBackend.FileEntity'], str],
            storeFilename: Callable[['DirectoryJsonBackend.FileEntity', str], None],
            getSuffixHint=(lambda e: None), redownload: bool = False):

        if len(entities) == 0:
            return

        dirName: Path = self.outputDirectory / directoryName
        hasFolder = dirName.is_dir()
        if hasFolder:
            files: Dict[str, str] = {Path(name).stem: name for name in os.listdir(dirName)}
        else:
            files = {}

        # First pass (no network): resolve cache hits and collect what actually
        # needs downloading, so the folder is created only when there's something
        # to fetch and the parallel phase works from a fixed list.
        toDownload: list = []
        for entity in entities:
            filename = getFilenameFromEntity(entity)
            if filename in files:
                storeFilename(entity, files[filename])
            elif shouldDownload(entity):
                toDownload.append(entity)

        if toDownload and not hasFolder:
            dirName.mkdir()
            hasFolder = True

        def download(entity):
            return entity, self.storeFile(
                url=getUrlFromEntity(entity), filename=getFilenameFromEntity(entity),
                directoryName=dirName, suffix=getSuffixHint(entity), redownload=redownload)

        # Each file is an independent GET writing to a distinct path; the driver's
        # governor is the real cap on in-flight requests. Results are applied on
        # this thread so the progress task has a single writer. max_workers=1
        # preserves the original sequential walk.
        workers = max(1, self.config.throttlingMaxConcurrency)
        with self.progress.task(directoryName, unit=entitiesName) as task:
            completed = 0
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [pool.submit(download, entity) for entity in toDownload]
                for future in as_completed(futures):
                    entity, storedName = future.result()
                    storeFilename(entity, storedName)
                    completed += 1
                    task.update(completed, len(toDownload))
        logging.info(f"Processed all {entitiesName}.")

    def processAttachments(self, directoryName: str, channelOpts: ChannelOptions,
                           attachments: Collection[FileAttachment], redownload: bool = False):
        if not channelOpts.downloadAttachments:
            return
        def shouldDownload(attachment: FileAttachment) -> bool:
            return ((channelOpts.downloadAttachmentSizeLimit == 0 or attachment.byteSize <= channelOpts.downloadAttachmentSizeLimit)
                and (len(channelOpts.downloadAttachmentTypes) == 0 or attachment.mimeType in channelOpts.downloadAttachmentTypes))
        def getSuffixHint(attachment: FileAttachment) -> Optional[str]:
            suffix = Path(attachment.name).suffix
            return suffix if suffix != '' else None

        self.processFiles(attachments, directoryName, 'files',
            getFilenameFromEntity=lambda attachment: str(attachment.id),
            shouldDownload=shouldDownload,
            getUrlFromEntity=lambda attachment: self.services.getFileUrl(attachment.id),
            storeFilename=lambda attachment, filename: None,
            getSuffixHint=getSuffixHint,
            redownload=redownload)

    def processEmoji(self, directoryName: str, emojis: Collection[Emoji], redownload: bool = False):
        self.processFiles(emojis, directoryName, 'emojis',
            getFilenameFromEntity=lambda e: e.name,
            shouldDownload=lambda _: True,
            getUrlFromEntity=lambda e: self.services.getEmojiUrl(e.id),
            storeFilename=lambda e, filename: setattr(e, 'imageFileName', filename),
            redownload=redownload)

    def processAvatars(self, directoryName: str, users: Collection[User], redownload: bool = False):
        self.processFiles(users, directoryName, 'user avatars',
            getFilenameFromEntity=lambda u: u.name,
            shouldDownload=lambda _: True,
            getUrlFromEntity=lambda u: self.services.getAvatarUrl(u.id),
            storeFilename=lambda u, filename: setattr(u, 'avatarFileName', filename),
            redownload=redownload)

    def downloadEmojiDatabase(self, rawEmojis: Iterable[dict]):
        '''Download the whole emoji database's images (config.downloadEmojis).'''
        emojis = [Emoji.fromMattermost(deepcopy(e)) for e in rawEmojis]
        self.processEmoji('emojis', emojis)


class _DirectoryStagingWriter(StagingWriter):
    '''Serializes raw posts into the newest->oldest staging buffer.'''
    def __init__(self, archive: 'DirectoryJsonChannelArchive', output):
        self._archive = archive
        self._output = output

    def add(self, rawPost: dict) -> None:
        options = self._archive.options
        takeEmojis = options.emojiMetadata or options.downloadEmoji
        post = Post.fromMattermost(rawPost)
        if post.emojis:
            if takeEmojis:
                post.emojis = [cast(Emoji, emoji).id for emoji in post.emojis]
            else:
                post.emojis = []
        _jsonDump(post.toStore(), self._output)
        self._output.write('\n')


class DirectoryJsonChannelArchive(ChannelArchive):
    def __init__(self, backend: DirectoryJsonBackend, key: str, channel: dict,
                 team: Optional[dict], options: ChannelOptions, seedUsers: List[dict]):
        self.backend = backend
        self.key = key
        self.options = options
        self._channelRaw = channel  # kept un-consumed for storage-independent callers
        # Convert the seed metadata to entities once (fromMattermost consumes its
        # input, and commit may run twice for the resume catch-up), keeping the
        # caller's raw dicts intact.
        self._channelBo = Channel.fromMattermost(deepcopy(channel))
        self._teamBo = Team.fromMattermost(deepcopy(team)) if team is not None else None
        self._seedUserBos = [User.fromMattermost(deepcopy(u)) for u in seedUsers]
        self._userBos: Dict[Id, User] = {u.id: u for u in self._seedUserBos}

        outdir = backend.outputDirectory
        self.headerFilename = outdir / (key + '.meta.json')
        self.dataFilename = outdir / (key + '.data.json')
        self.tmpFilename = outdir / (key + '.data.json.tmp')

    # ----- download boundary --------------------------------------------------
    def committedNewestPost(self) -> Optional[Tuple[Id, Time]]:
        info = readLastStoredPost(self.dataFilename)
        if info is None:
            return None
        postId, createTime, _ = info
        return postId, createTime

    def reconcileBuffer(self) -> ResumeState:
        tmp = self.tmpFilename
        # A zero-length buffer carries no resumable state; treat it as absent.
        if tmp.is_file() and tmp.stat().st_size == 0:
            tmp.unlink()
            return ResumeState()
        if not (tmp.is_file() and tmp.stat().st_size > 0):
            return ResumeState()
        tmpInfo = readLastStoredPost(tmp)
        if tmpInfo is None:
            tmp.unlink()  # no complete buffered post; start over
            return ResumeState()
        oldestId, oldestTime, tmpValidLen = tmpInfo
        if tmpValidLen != tmp.stat().st_size:
            os.truncate(tmp, tmpValidLen)  # drop a half-written trailing line
        # Every buffered post is strictly newer than the committed archive, so undo
        # any partial prior commit before re-appending.
        trimDataFileNewerThan(self.dataFilename, oldestTime)
        return ResumeState(resume=True, resumeCursor=oldestId, priorCount=countStoredPosts(tmp))

    @contextmanager
    def stagingWriter(self, resume: bool) -> 'Generator[StagingWriter, None, None]':
        with open(self.tmpFilename, 'a' if resume else 'w', encoding='utf8') as output:
            yield _DirectoryStagingWriter(self, output)

    # ----- commit -------------------------------------------------------------
    def _userBo(self, userId: Id) -> User:
        if userId not in self._userBos:
            self._userBos[userId] = User.fromMattermost(deepcopy(self.backend.services.getUserById(userId)))
        return self._userBos[userId]

    def _freshHeader(self) -> ChannelHeader:
        header = ChannelHeader(channel=self._channelBo, team=self._teamBo)
        header.usedUsers = set(self._seedUserBos)
        header.usedEmojis = set()
        return header

    def _loadChannelHeader(self) -> Optional[ChannelHeader]:
        if not self.headerFilename.is_file():
            return None
        try:
            with open(self.headerFilename, 'r', encoding='utf8') as headerFile:
                return ChannelHeader.fromStore(json.load(headerFile))
        except Exception:
            logging.warning(f"Unable to load existing metadata file '{self.headerFilename}'.")
            return None

    def commit(self, *, incremental: bool, localNewestId: Optional[Id]) -> None:
        class PostRef:
            '''Minimal post view for storage metadata (id + createTime only).'''
            __slots__ = ('id', 'createTime')
            def __init__(self, id: Id, createTime: Time):
                self.id = id
                self.createTime = createTime

        options = self.options
        header = self._freshHeader()
        storage = PostStorage.fromOptions(options)
        header.storage = storage
        attachments: List[FileAttachment] = []
        takeEmojis: bool = options.emojiMetadata or options.downloadEmoji

        with open(self.dataFilename, 'ab') as output:
            # Posts arrive oldest->newest here. Buffer one so each is written
            # carrying its successor's id as the "post after" ordering hint.
            prevPostId: Optional[Id] = localNewestId
            pending: Optional[Tuple[dict, bytes]] = None

            def flush(successor: Optional[dict]):
                nonlocal pending, prevPostId
                if pending is None:
                    return
                pendingPost, pendingLine = pending
                output.write(pendingLine + b'\n')
                hints = PostHints(
                    postIdBefore=prevPostId,
                    postIdAfter=(Id(successor['id']) if successor is not None else None))
                storage.addSortedPost(
                    cast(Post, PostRef(Id(pendingPost['id']), Time(pendingPost['createTime']))),
                    hints)
                header.usedUsers.add(self._userBo(pendingPost['userId']))
                if options.downloadAttachments:
                    for attachmentInfo in pendingPost.get('attachments', []):
                        attachments.append(FileAttachment.fromStore(attachmentInfo))
                if takeEmojis:
                    for emojiId in pendingPost.get('emojis', []):
                        try:
                            header.usedEmojis.add(Emoji.fromMattermost(deepcopy(self.backend.services.getEmojiById(emojiId))))
                        except KeyError:
                            logging.warning(f"Emoji '{emojiId}' referenced by a stored post is no longer available.")
                prevPostId = Id(pendingPost['id'])
                pending = None

            for line in iterPostsBackward(self.tmpFilename):
                successor = json.loads(line)
                flush(successor)
                pending = (successor, line)
            flush(None)

            output.flush()
            storage.byteSize = os.fstat(output.fileno()).st_size

        self._processChannelAuxiliaries(header, attachments)

        # Carry forward metadata accumulated by previous downloads, if appending.
        if incremental:
            oldHeader = self._loadChannelHeader()
            if oldHeader is not None:
                try:
                    oldHeader.update(header)
                    header = oldHeader
                except AssertionError:
                    logging.warning("Existing channel metadata was incompatible with the appended posts;"
                        " rewriting metadata for the appended range only.")

        headerContent = header.toStore()
        with open(self.headerFilename, 'w', encoding='utf8') as headerFile:
            _jsonDump(headerContent, headerFile)

        if self.tmpFilename.is_file():
            self.tmpFilename.unlink()

    def _processChannelAuxiliaries(self, header: ChannelHeader, usedAttachments: List[FileAttachment]):
        options = self.options
        if options.downloadEmoji and not self.backend.config.downloadAllEmojis:
            self.backend.processEmoji('emojis', emojis=header.usedEmojis)
        if options.downloadAttachments and len(usedAttachments) > 0:
            self.backend.processAttachments(self.key + '--files', channelOpts=options, attachments=usedAttachments)
        if options.downloadAvatars:
            self.backend.processAvatars('avatars', users=header.usedUsers)
