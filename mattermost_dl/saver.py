'''
    Contains high level logic of history downloading
'''

from .common import *

from .bo import *
from .config import ChannelOptions, ConfigFile, GroupChannelSpec, LogVerbosity, TeamSpec
from .driver import MattermostDriver
from . import progress
from .recovery import RReuse, RecoveryArbiter, RBackup, RDelete, RSkipDownload
from .store import (ChannelHeader, PostStorage, countStoredPosts,
    iterPostsBackward, readLastStoredPost, trimDataFileNewerThan)

import json
from mimetypes import guess_extension

@dataclass
class ChannelRequest:
    config: ChannelOptions
    metadata: Channel

    def __hash__(self) -> int:
        return hash(self.metadata.id)

class SavingFailed(Exception):
    '''
        Invocation of Saver failed due to known external problem, such as failing to log in.
        Unlike logical errors, dumping stack trace to users is not necessary.

        Should stringify into problem description and if caused by internal exception that
        may provide additional info, such subexception may be chained into it.
    '''
    pass


class Saver:
    '''
        Main class responsible for orchestrating the downloading process.
        Should start from __call__ method.
    '''
    def __init__(self, configfile: ConfigFile, driver: Optional[MattermostDriver] = None,
            recoveryArbiter: Optional[RecoveryArbiter] = None
            ):
        if driver is None:
            driver = MattermostDriver(configfile)
        if recoveryArbiter is None:
            recoveryArbiter = RecoveryArbiter(configfile)
        self.configfile = configfile
        self.driver: MattermostDriver = driver
        self.recoveryArbiter: RecoveryArbiter = recoveryArbiter
        self.user: User # Conveniency, fetched on call

    def jsonDumpToFile(self, obj, fp):
        def fallback(obj):
            if hasattr(obj, 'toStore'):
                return obj.toStore()
            return str(obj)

        json.dump(obj, fp, default=fallback, ensure_ascii=False)

    def getUserByLocator(self, locator: EntityLocator) -> User:
        if hasattr(locator, 'id'):
            return self.driver.getUserById(locator.id)
        elif hasattr(locator, 'name'):
            return self.driver.getUserByName(locator.name)
        elif hasattr(locator, 'internalName'):
            return self.driver.getUserByName(locator.internalName)
        else:
            raise ValueError

    def matchGroupChannel(self, channel: Channel, locator: Union[Id, FrozenSet[EntityLocator]]) -> bool:
        if isinstance(locator, str):
            return channel.id == locator
        else:
            assert isinstance(locator, frozenset)
            if channel.members is None:
                self.driver.loadChannelMembers(channel)
                assert channel.members is not None
            users = set(self.getUserByLocator(userLocator)
                for userLocator in locator
            )
            users.add(self.user)
            return users == set(u for u in channel.members)

    def getWantedUsers(self) -> List[Tuple[User, ChannelOptions]]:
        userIds = set()
        res = []
        for userSpec in self.configfile.explicitUsers:
            u = self.getUserByLocator(userSpec.locator)
            if u.id in userIds:
                logging.warning(f"Explicitly requesting direct messages for user {u.name} more than once.")
            else:
                userIds.add(u.id)
                res.append((u, userSpec.opts))
        return res

    def getWantedGlobalChannels(self) -> Tuple[Dict[User, ChannelRequest], Set[ChannelRequest]]:
        '''
            Collects a list of channels requested by configfile that aren't scoped under Team.
            Returns pair representing channel requests for users and groups respectively.
        '''
        wantedDirectChannels: Dict[User, ChannelRequest] = {}
        wantedGroupChannels: Set[ChannelRequest] = set()
        explicitDirectChannelNames = {self.driver.getDirectChannelNameByUserId(u.id): (u, opts) for u, opts in self.getWantedUsers()}
        matchedGroupChannels: Set[GroupChannelSpec] = set()
        for team in self.driver.getTeams().values():
            for channel in team.channels.values():
                if channel.type == ChannelType.Direct:
                    # If we don't have this channel already
                    if channel.id not in (ch.metadata.id for ch in wantedDirectChannels.values()):
                        if channel.internalName in explicitDirectChannelNames:
                            u, opts = explicitDirectChannelNames[channel.internalName]
                            wantedDirectChannels.update({u: ChannelRequest(config=opts, metadata=channel)})
                            del explicitDirectChannelNames[channel.internalName]
                        elif self.configfile.miscDirectChannels:
                            otherUser = self.driver.getUserById(self.driver.getUserIdFromDirectChannelName(channel.internalName))
                            wantedDirectChannels.update({otherUser: ChannelRequest(config=self.configfile.directChannelDefaults, metadata=channel)})
                elif channel.type == ChannelType.Group:
                    for wch in self.configfile.explicitGroups:
                        if self.matchGroupChannel(channel, wch.locator):
                            wantedGroupChannels.add(ChannelRequest(config=wch.opts, metadata=channel))
                            matchedGroupChannels.add(wch)
                            break
                    else:
                        if self.configfile.miscGroupChannels:
                            wantedGroupChannels.add(ChannelRequest(config=self.configfile.groupChannelDefaults, metadata=channel))

        # Have not found all channels?
        for user, _ in explicitDirectChannelNames.values():
            logging.warning(f'Found no direct channel with {user.name}.')
        for wch in self.configfile.explicitGroups:
            if wch not in matchedGroupChannels:
                logging.warning(f'Found no group channel via locator {wch.locator}.')
        return wantedDirectChannels, wantedGroupChannels

    def getWantedPerTeamChannels(self) -> Dict[Team, List[ChannelRequest]]:
        if self.configfile.miscTeams is False and len(self.configfile.explicitTeams) == 0:
            return {}

        res: Dict[Team, List[ChannelRequest]] = {}
        teams = self.driver.getTeams()

        def getChannelsForTeam(team: Team, wantedTeam: TeamSpec) -> List[ChannelRequest]:
            channels = []
            explicitPublicLocators = {ch.locator for ch in wantedTeam.explicitPublicChannels}
            explicitPrivateLocators = {ch.locator for ch in wantedTeam.explicitPrivateChannels}

            for availableChannel in team.channels.values():
                if availableChannel.type == ChannelType.Open:
                    for wch in wantedTeam.explicitPublicChannels:
                        if availableChannel.match(wch.locator):
                            channels.append(ChannelRequest(config=wch.opts, metadata=availableChannel))
                            explicitPublicLocators.remove(wch.locator)
                            break
                    else:
                        if wantedTeam.miscPublicChannels:
                            channels.append(ChannelRequest(config=wantedTeam.publicChannelDefaults, metadata=availableChannel))
                elif availableChannel.type == ChannelType.Private:
                    for wch in wantedTeam.explicitPrivateChannels:
                        if availableChannel.match(wch.locator):
                            channels.append(ChannelRequest(config=wch.opts, metadata=availableChannel))
                            explicitPrivateLocators.remove(wch.locator)
                            break
                    else:
                        if wantedTeam.miscPrivateChannels:
                            channels.append(ChannelRequest(config=wantedTeam.privateChannelDefaults, metadata=availableChannel))
            for loc in explicitPublicLocators:
                logging.warning(f'Found no requested public channel on team {team.internalName} ({team.name}) via locator {loc}.')
            for loc in explicitPrivateLocators:
                logging.warning(f'Found no requested private channel on team {team.internalName} ({team.name}) via locator {loc}.')
            return channels

        explicitTeamLocators: Set[EntityLocator] = {t.locator for t in self.configfile.explicitTeams}
        for availableTeam in teams.values():
            for wantedTeam in self.configfile.explicitTeams:
                if availableTeam.match(wantedTeam.locator):
                    res[availableTeam] = getChannelsForTeam(availableTeam, wantedTeam)
                    explicitTeamLocators.remove(wantedTeam.locator)
                    break
            else:
                if self.configfile.miscTeams:
                    channels = []
                    for ch in availableTeam.channels.values():
                        if ch.type == ChannelType.Open:
                            channels.append(ChannelRequest(config=self.configfile.publicChannelDefaults, metadata=ch))
                        elif ch.type == ChannelType.Private:
                            channels.append(ChannelRequest(config=self.configfile.privateChannelDefaults, metadata=ch))
                    res[availableTeam] = channels
        for loc in explicitTeamLocators:
            logging.error(f'Team requested by {loc} was not found!')
        return res

    def storeFile(self, url: str, filename: str, directoryName: Path, suffix: Optional[str] = None, redownload: bool = False) -> str:
        if '/' in filename:
            logging.warning(f'Refusing to store file with name "{filename}"')
            raise ValueError

        httpResponse = self.driver.getRaw(url)
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
            self.driver.storeUrlInto(url, output)
        return filename + suffix

    FileEntity = TypeVar('FileEntity')
    def processFiles(self, entities: Collection[FileEntity], directoryName: str, entitiesName: str,
            getFilenameFromEntity: Callable[[FileEntity], str], shouldDownload: Callable[[FileEntity], bool],
            getUrlFromEntity: Callable[[FileEntity], str], storeFilename: Callable[[FileEntity, str], None],
            getSuffixHint = (lambda e: None), redownload: bool = False):

        # Note: getSuffixHint: Callable[[FileEntity], Optional[str]] can't be assigned due to type checker failure
        if len(entities) == 0:
            return

        dirName: Path = self.configfile.outputDirectory / directoryName
        hasFolder = dirName.is_dir()
        if hasFolder:
            files: Dict[str, str] = {Path(name).stem: name for name in os.listdir(dirName)}
        else:
            files = {}

        showProgressReport = self.showProgressReport()

        if showProgressReport:
            reporter = progress.ProgressReporter(sys.stderr, settings=self.configfile.reportProgress,
                header='Progress: ', footer=f'/{len(entities)} {entitiesName} (upper limit approximate)',
                contentPadding=6, contentAlignLeft=False, updateIntervalMs=self.configfile.progressInterval)
            reporter.open()
            reporter.update('0')
        else:
            # Reporter should be never accessed in this case, but we want clear type for linting
            reporter = cast(progress.ProgressReporter, UnboundLocalError)

        for i, entity in enumerate(entities):
            filename = getFilenameFromEntity(entity)
            if filename in files:
                storeFilename(entity, files[filename])
                continue
            if not shouldDownload(entity):
                continue
            url = getUrlFromEntity(entity)

            if not hasFolder:
                dirName.mkdir()
                hasFolder = True

            suffix = getSuffixHint(entity)
            storeFilename(entity, self.storeFile(
                url=url, filename=filename, directoryName=dirName,
                suffix=suffix, redownload=redownload))

            if showProgressReport:
                reporter.update(str(i+1))
        if showProgressReport:
            reporter.close()
        logging.info(f"Processed all {entitiesName}.")

    def processAttachments(self, directoryName: str, channelOpts: ChannelOptions, attachments: Collection[FileAttachment], redownload: bool = False):
        if not channelOpts.downloadAttachments:
            return
        def shouldDownload(attachment: FileAttachment) -> bool:
            return ((channelOpts.downloadAttachmentSizeLimit == 0 or attachment.byteSize <= channelOpts.downloadAttachmentSizeLimit)
                and (len(channelOpts.downloadAttachmentTypes) == 0 or attachment.mimeType in channelOpts.downloadAttachmentTypes))
        def storeFilename(attachment: FileAttachment, filename: str):
            pass
        def getSuffixHint(attachment: FileAttachment) -> Optional[str]:
            suffix = Path(attachment.name).suffix
            if suffix == '':
                return None
            else:
                return suffix

        self.processFiles(attachments, directoryName, 'files',
            getFilenameFromEntity=lambda attachment: str(attachment.id),
            shouldDownload=shouldDownload,
            getUrlFromEntity=lambda attachment: self.driver.getFileUrl(attachment),
            storeFilename=storeFilename,
            getSuffixHint=getSuffixHint,
            redownload=redownload
        )

    def processEmoji(self, directoryName: str, emojis: Collection[Emoji], redownload: bool = False):
        def storeFilename(emoji: Emoji, filename: str):
            emoji.imageFileName = filename
        self.processFiles(emojis, directoryName, 'emojis',
            getFilenameFromEntity=lambda e: e.name,
            shouldDownload=lambda _: True,
            getUrlFromEntity=lambda e: self.driver.getEmojiUrl(e),
            storeFilename=storeFilename,
            redownload=redownload
        )

    def processAvatars(self, directoryName: str, users: Collection[User], redownload: bool = False):
        def storeFilename(user: User, avatarFilename: str):
            user.avatarFileName = avatarFilename
        self.processFiles(users, directoryName, 'user avatars',
            getFilenameFromEntity=lambda u: u.name,
            shouldDownload=lambda _: True,
            getUrlFromEntity=lambda u: self.driver.getAvatarUrl(u),
            storeFilename=storeFilename,
            redownload=redownload
        )

    def enrichEmoji(self, emoji: Emoji):
        if self.configfile.verboseHumanFriendlyPosts:
            emoji.creatorName = self.driver.getUserById(emoji.creatorId).name

    def enrichPostReaction(self, reaction: PostReaction):
        if self.configfile.verboseHumanFriendlyPosts:
            reaction.userName = self.driver.getUserById(reaction.userId).name

    # Note: the post gets mutated, so we better not pass persistent copy
    def enrichPost(self, post: Post):
        if self.configfile.verboseHumanFriendlyPosts:
            post.userName = self.driver.getUserById(post.userId).name
        if len(post.reactions) != 0:
            for reaction in post.reactions:
                self.enrichPostReaction(reaction)

    def showProgressReport(self) -> bool:
        return (self.configfile.verbosity == LogVerbosity.Normal
            and self.configfile.reportProgress.mode != progress.VisualizationMode.DumbTerminal)

    def makeArchiveFilenames(self, stem: str) -> Tuple[Path, Path]:
        '''
            Helper that returns pair of filenames for header and data file of channel archive
        '''
        return (
            self.configfile.outputDirectory / (stem + '.meta.json'),
            self.configfile.outputDirectory / (stem + '.data.json')
        )

    def makeBufferFilename(self, stem: str) -> Path:
        '''
            Filename of the transient newest->oldest download buffer for a channel.
            Its presence signals an interrupted download that can be resumed.
        '''
        return self.configfile.outputDirectory / (stem + '.data.json.tmp')

    def getUnusedArchiveBackupFilenames(self, backupAlternatives: Generator[str, None, None]) -> Tuple[Path, Path]:
        while True:
            fname = next(backupAlternatives)
            headerFname, dataFname = self.makeArchiveFilenames(fname)
            if not headerFname.is_file() and not dataFname.is_file():
                return headerFname, dataFname

    def backupArchive(self, channel: Channel, channelOutfile: str,
            backupOutfile: str, backupAlternatives: Generator[str, None, None]
        ) -> Union[None, RSkipDownload]:
        '''
            Backups existing archive of selected filename by renaming it.
            Consults arbiter on overwrites of existing backups  - if the chosen action is to not lose anything and not creating redundant data,
            RSkipDownload is returned.

            Outfile parameters represent root of the channel's name, without suffixes.
            `backupAlternatives` shall yield alternate backup outfile.
        '''

        headerFname, dataFname = self.makeArchiveFilenames(channelOutfile)
        headerBackupFname, dataBackupFname = self.makeArchiveFilenames(backupOutfile)

        headerExist = headerFname.is_file()
        dataExist = dataFname.is_file()

        if not headerExist and not dataExist:
            return None

        # Backups already exist
        if headerBackupFname.is_file() or dataBackupFname.is_file():
            opts = self.recoveryArbiter.onExistingChannelBackup(channel, headerBackupFname, dataBackupFname)
            if isinstance(opts, RSkipDownload):
                return opts
            elif isinstance(opts, RDelete):
                if headerBackupFname.is_file():
                    headerBackupFname.unlink()
                if dataBackupFname.is_file():
                    dataBackupFname.unlink()
            else:
                assert opts == RBackup()
                headerAltBackupFname, dataAltBackupFname = self.getUnusedArchiveBackupFilenames(backupAlternatives)
                if headerBackupFname.is_file():
                    headerBackupFname.rename(headerAltBackupFname)
                if dataBackupFname.is_file():
                    dataBackupFname.rename(dataAltBackupFname)

        if headerExist:
            headerFname.rename(headerBackupFname)
        if dataExist:
            dataFname.rename(dataBackupFname)

        return None

    def loadChannelHeader(self, headerFilename: Path) -> Optional[ChannelHeader]:
        '''
            Loads an existing channel metadata header, or None if it is missing or
            unreadable. Used only to carry forward accumulated metadata (used users,
            emojis, post storage stats) into a new commit; it is never consulted to
            decide what to download.
        '''
        if not headerFilename.is_file():
            return None
        try:
            with open(headerFilename, 'r', encoding='utf8') as headerFile:
                return ChannelHeader.fromStore(json.load(headerFile))
        except Exception:
            logging.warning(exceptionFormatter(f"Unable to load existing metadata file '{headerFilename}'."))
            return None

    def processChannelAuxiliaries(self, channelOutfile: str, header: ChannelHeader, options: ChannelOptions, usedAttachments: List[FileAttachment]):
        '''Fetches additional data beside posts for given channel.'''
        if options.emojiMetadata:
            for emoji in header.usedEmojis:
                self.enrichEmoji(emoji)
        if options.downloadEmoji and not self.configfile.downloadAllEmojis:
            self.processEmoji('emojis', emojis=header.usedEmojis)

        if options.downloadAttachments and len(usedAttachments) > 0:
            self.processAttachments(channelOutfile+'--files', channelOpts=options, attachments=usedAttachments)

        if options.downloadAvatars:
            self.processAvatars('avatars', users=header.usedUsers)


    def processChannel(self, channelOutfile: str, header: ChannelHeader, channelRequest: ChannelRequest):
        '''
            Downloads a channel into <channelOutfile>.data.json (oldest->newest) and
            writes metadata into <channelOutfile>.meta.json.

            The download is driven purely by the files on disk (never by the header):
              - no .data.json, no buffer  -> fresh first download (stops at channel start)
              - no .data.json, buffer     -> resume an interrupted first download
              - .data.json, no buffer     -> incremental update (stops at local newest)
              - .data.json, buffer        -> resume an interrupted incremental update
            See store.py for the buffer/data-file format.
        '''
        channel, options = channelRequest.metadata, channelRequest.config

        headerFilename, dataFilename = self.makeArchiveFilenames(channelOutfile)
        tmpFilename = self.makeBufferFilename(channelOutfile)

        if options.postLimit == 0 or options.postSessionLimit == 0:
            return # Early exit - nothing downloaded, no need to touch header

        def backupAltNames() -> Generator[str, None, None]:
            '''Yields alternative filenames for archive backups.'''
            i = 1
            while True:
                yield f'{channelOutfile}--backup~{i}'
                i += 1

        # A zero-length buffer carries no resumable state; treat it as absent.
        if tmpFilename.is_file() and tmpFilename.stat().st_size == 0:
            tmpFilename.unlink()

        # With no interrupted buffer but an existing committed archive, the user's
        # reuse policy decides whether to append, back up, delete or skip. An
        # interrupted buffer is always simply resumed (the policy already applied
        # when that interrupted run started).
        if not tmpFilename.is_file() and dataFilename.is_file():
            decision = self.recoveryArbiter.onArchiveReuse(
                self.loadChannelHeader(headerFilename), options, reusable=True)
            if isinstance(decision, RSkipDownload):
                return
            elif isinstance(decision, RDelete):
                self._deleteArchive(headerFilename, dataFilename)
            elif isinstance(decision, RBackup):
                if self.backupArchive(channel, channelOutfile, channelOutfile+'--backup', backupAltNames()) == RSkipDownload():
                    return
            else:
                assert isinstance(decision, RReuse)

        resumed = self._runDownloadCycle(channel, options, channelOutfile, header,
            headerFilename, dataFilename, tmpFilename)
        if resumed:
            # The resumed buffer's newest post was frozen at the channel-newest of
            # the interrupted run; run one fresh incremental to capture anything that
            # has arrived above it since.
            self._runDownloadCycle(channel, options, channelOutfile, header,
                headerFilename, dataFilename, tmpFilename)

    def _deleteArchive(self, headerFilename: Path, dataFilename: Path):
        if headerFilename.is_file():
            headerFilename.unlink()
        if dataFilename.is_file():
            dataFilename.unlink()

    def _freshHeader(self, template: ChannelHeader) -> ChannelHeader:
        '''A working header seeded with the template's channel/team/participants.'''
        header = ChannelHeader(channel=template.channel, team=template.team)
        header.usedUsers = set(template.usedUsers)
        header.usedEmojis = set(template.usedEmojis)
        return header

    def _buildDriverParams(self, options: ChannelOptions, *, resume: bool, resumeCursor: Optional[Id],
            incremental: bool, localNewestId: Optional[Id], localNewestTime: Optional[Time]
        ) -> Dict[str, Any]:
        '''
            Translates channel options plus the on-disk state into MattermostDriver
            seek/stop parameters for a newest->oldest walk.
        '''
        params: Dict[str, Any] = {}

        if options.postLimit > 0 or options.postSessionLimit > 0:
            if options.postLimit == -1:
                params['maxCount'] = options.postSessionLimit
            elif options.postSessionLimit == -1:
                params['maxCount'] = options.postLimit
            else:
                params['maxCount'] = min(options.postLimit, options.postSessionLimit)

        # Where the walk starts (newest side): a resume seek wins over user filters.
        if resume:
            params['beforePost'] = resumeCursor
        elif options.postsBeforeId:
            params['beforePost'] = options.postsBeforeId
        elif options.postsBeforeTime:
            params['beforeTime'] = options.postsBeforeTime

        # Where the walk stops (oldest side): an incremental stops at the local
        # newest post (by id, and by its time should that post be deleted
        # server-side); a first download stops at any user lower bound, else the
        # natural channel start.
        if incremental:
            params['afterPost'] = localNewestId
            params['afterTime'] = localNewestTime
        elif options.postsAfterId:
            params['afterPost'] = options.postsAfterId
        elif options.postsAfterTime:
            params['afterTime'] = options.postsAfterTime

        return params

    def _runDownloadCycle(self, channel: Channel, options: ChannelOptions, channelOutfile: str,
            template: ChannelHeader, headerFilename: Path, dataFilename: Path, tmpFilename: Path) -> bool:
        '''
            One pass of the state machine: reconcile any interrupted buffer, fetch
            newest->oldest into the buffer, then commit it into the data file.

            Returns True if this pass resumed an interrupted buffer, so the caller
            should run one more fresh incremental to catch up.
        '''
        resume = False
        resumeCursor: Optional[Id] = None
        priorBufferCount = 0
        if tmpFilename.is_file() and tmpFilename.stat().st_size > 0:
            tmpInfo = readLastStoredPost(tmpFilename)
            if tmpInfo is None:
                tmpFilename.unlink()  # no complete buffered post; start over
            else:
                oldestId, oldestTime, tmpValidLen = tmpInfo
                if tmpValidLen != tmpFilename.stat().st_size:
                    os.truncate(tmpFilename, tmpValidLen)  # drop a half-written trailing line
                # Every buffered post is strictly newer than the committed archive,
                # so undo any partial prior commit before re-appending.
                trimDataFileNewerThan(dataFilename, oldestTime)
                resume = True
                resumeCursor = oldestId
                # Posts already in the buffer count toward progress, so a resumed
                # download continues the count instead of appearing to restart at 0.
                priorBufferCount = countStoredPosts(tmpFilename)

        incremental = dataFilename.is_file() and dataFilename.stat().st_size > 0
        localNewestId: Optional[Id] = None
        localNewestTime: Optional[Time] = None
        if incremental:
            localNewest = readLastStoredPost(dataFilename)
            if localNewest is None:
                incremental = False  # data file had no complete post
            else:
                localNewestId, localNewestTime, _ = localNewest

        dlParams = self._buildDriverParams(options, resume=resume, resumeCursor=resumeCursor,
            incremental=incremental, localNewestId=localNewestId, localNewestTime=localNewestTime)

        self._fetchIntoBuffer(channel, options, tmpFilename, resume=resume,
            dlParams=dlParams, priorCount=priorBufferCount)
        self._commitBuffer(channel, options, channelOutfile, template,
            headerFilename, dataFilename, tmpFilename,
            incremental=incremental, localNewestId=localNewestId)
        return resume

    def _fetchIntoBuffer(self, channel: Channel, options: ChannelOptions, tmpFilename: Path, *,
            resume: bool, dlParams: Dict[str, Any], priorCount: int = 0):
        '''
            Streams posts newest->oldest into the buffer (append when resuming, else
            fresh). Each post is enriched and serialized; metadata is not collected
            here but during commit, which keeps it robust to resumption. On
            interruption the partially written buffer is left on disk for resuming.

            `priorCount` is how many posts a resumed buffer already holds, so the
            progress count continues from there instead of restarting at 0.
        '''
        showProgressReport = self.showProgressReport()
        takeEmojis: bool = options.emojiMetadata or options.downloadEmoji
        bufferedCount = priorCount  # total posts in the buffer (prior + this run)
        sessionCount = 0            # posts fetched during this run only

        with open(tmpFilename, 'a' if resume else 'w', encoding='utf8') as output:
            if showProgressReport:
                estimatedPostLimit: int = channel.messageCount
                if options.postLimit != -1:
                    estimatedPostLimit = min(estimatedPostLimit, options.postLimit)
                if options.postSessionLimit != -1:
                    estimatedPostLimit = min(estimatedPostLimit, options.postSessionLimit)

                progressReporter = progress.ProgressReporter(sys.stderr, settings=self.configfile.reportProgress,
                    contentPadding=10, contentAlignLeft=False,
                    header='Progress: ', footer=f'/{estimatedPostLimit} posts (upper limit approximate)',
                    updateIntervalMs=self.configfile.progressInterval)
            else:
                # Reporter should be never accessed in this case, but we want clear type for linting
                progressReporter = cast(progress.ProgressReporter, UnboundLocalError)
            # The reporter is opened lazily on the first fetched post: a run with
            # nothing new to download (e.g. an up-to-date channel) then shows no
            # progress line at all, instead of a confusing frozen "0/N".
            progressOpened = False

            def perPost(p: Post, hints: MattermostDriver.PostHints):
                nonlocal bufferedCount, sessionCount, progressOpened
                self.enrichPost(p)
                if p.emojis:
                    if takeEmojis:
                        p.emojis = [cast(Emoji, emoji).id for emoji in p.emojis]
                    else:
                        p.emojis = []
                self.jsonDumpToFile(p.toStore(), output)
                output.write('\n')
                bufferedCount += 1
                sessionCount += 1
                if showProgressReport:
                    if not progressOpened:
                        progressReporter.open()
                        progressOpened = True
                    progressReporter.update(str(bufferedCount))

            if showProgressReport:
                skippedPostCount = 0
                skippedLeadingMsg = False
                def onSkippedPost():
                    nonlocal skippedLeadingMsg, skippedPostCount
                    if skippedPostCount % 99 == 0:
                        if skippedLeadingMsg:
                            print('.', end='', file=sys.stderr, flush=True)
                        else:
                            print(' ...skipping posts not matching condition...', end='', file=sys.stderr, flush=True)
                            skippedLeadingMsg = True
                    skippedPostCount += 1
            else:
                def onSkippedPost():
                    pass

            postProcessRes = self.driver.processPosts(processor=perPost, channel=channel, **dlParams, onSkippedPost=onSkippedPost)

            if showProgressReport and progressOpened:
                progressReporter.close()
            if postProcessRes == MattermostDriver.ProcessPostResult.NothingRequested:
                logging.info('Nothing to download.')
            elif sessionCount == 0:
                logging.info('No new posts; channel already up to date.')
            elif postProcessRes == MattermostDriver.ProcessPostResult.NoMorePosts:
                logging.info(f'Downloaded {sessionCount} posts (reached channel start).')
            elif postProcessRes == MattermostDriver.ProcessPostResult.MaxCountReached:
                logging.info(f'Downloaded {sessionCount} posts (post limit reached).')
            else:
                assert postProcessRes == MattermostDriver.ProcessPostResult.ConditionReached
                logging.info(f'Downloaded {sessionCount} posts (reached requested boundary).')

    def _commitBuffer(self, channel: Channel, options: ChannelOptions, channelOutfile: str,
            template: ChannelHeader, headerFilename: Path, dataFilename: Path, tmpFilename: Path, *,
            incremental: bool, localNewestId: Optional[Id]):
        '''
            Reverses the newest->oldest buffer into the oldest->newest data file,
            building post storage metadata and collecting used users/emojis/
            attachments, then fetches auxiliaries, writes the metadata header and
            deletes the buffer. When appending, accumulated metadata from previous
            downloads is merged in.
        '''
        class PostRef:
            '''Minimal post view for storage metadata (id + createTime only).'''
            __slots__ = ('id', 'createTime')
            def __init__(self, id: Id, createTime: Time):
                self.id = id
                self.createTime = createTime

        header = self._freshHeader(template)
        storage = PostStorage.fromOptions(options)
        header.storage = storage
        attachments: List[FileAttachment] = []
        takeEmojis: bool = options.emojiMetadata or options.downloadEmoji

        with open(dataFilename, 'ab') as output:
            # Posts arrive oldest->newest here. We buffer one post so each is written
            # carrying its successor's id as the "post after" ordering hint.
            prevPostId: Optional[Id] = localNewestId
            pending: Optional[Tuple[dict, bytes]] = None

            def flush(successor: Optional[dict]):
                nonlocal pending, prevPostId
                if pending is None:
                    return
                pendingPost, pendingLine = pending
                output.write(pendingLine + b'\n')
                hints = MattermostDriver.PostHints(
                    postIdBefore=prevPostId,
                    postIdAfter=(Id(successor['id']) if successor is not None else None))
                storage.addSortedPost(
                    cast(Post, PostRef(Id(pendingPost['id']), Time(pendingPost['createTime']))),
                    hints)
                header.usedUsers.add(self.driver.getUserById(pendingPost['userId']))
                if options.downloadAttachments:
                    for attachmentInfo in pendingPost.get('attachments', []):
                        attachments.append(FileAttachment.fromStore(attachmentInfo))
                if takeEmojis:
                    for emojiId in pendingPost.get('emojis', []):
                        try:
                            header.usedEmojis.add(self.driver.getEmojiById(emojiId))
                        except KeyError:
                            logging.warning(f"Emoji '{emojiId}' referenced by a stored post is no longer available.")
                prevPostId = Id(pendingPost['id'])
                pending = None

            for line in iterPostsBackward(tmpFilename):
                successor = json.loads(line)
                flush(successor)
                pending = (successor, line)
            flush(None)

            output.flush()
            storage.byteSize = os.fstat(output.fileno()).st_size

        self.processChannelAuxiliaries(channelOutfile, header, options, attachments)

        # Carry forward metadata accumulated by previous downloads, if appending.
        if incremental:
            oldHeader = self.loadChannelHeader(headerFilename)
            if oldHeader is not None:
                try:
                    oldHeader.update(header)
                    header = oldHeader
                except AssertionError:
                    logging.warning("Existing channel metadata was incompatible with the appended posts;"
                        " rewriting metadata for the appended range only.")

        headerContent = header.toStore()
        with open(headerFilename, 'w', encoding='utf8') as headerFile:
            self.jsonDumpToFile(headerContent, headerFile)

        if tmpFilename.is_file():
            tmpFilename.unlink()

    def processDirectChannel(self, otherUser: User, channelRequest: ChannelRequest):
        # channel, options = channelRequest.metadata, channelRequest.config
        logging.info(f"Processing conversation with {otherUser.name} ...")

        directChannelOutfile = f'd.{self.user.name}--{otherUser.name}'
        header = ChannelHeader(channel=channelRequest.metadata)
        header.usedUsers = {self.user, otherUser}

        self.processChannel(channelOutfile=directChannelOutfile, header=header, channelRequest=channelRequest)

    def processGroupChannel(self, channelRequest: ChannelRequest):
        channel, options = channelRequest.metadata, channelRequest.config
        if channel.members is None:
            self.driver.loadChannelMembers(channel)
            assert channel.members is not None
        userlist = '-'.join(sorted(u.name for u in channel.members))
        if userlist == '':
            logging.warning(f'No users for group channel {channel.id}, using id as name!')
            userlist = str(channel.id)
        logging.info(f"Processing group chat {userlist} ...")
        channelOutfile = f'g.{userlist}'

        header = ChannelHeader(channel=channel)

        self.processChannel(channelOutfile=channelOutfile, header=header, channelRequest=channelRequest)

    def processTeamChannel(self, team: Team, channelRequest: ChannelRequest):
        '''
            Processes public and private channels
        '''
        channel, options = channelRequest.metadata, channelRequest.config

        private = channelRequest.metadata.type == ChannelType.Private

        logging.info(f'Processing {"private" if private else "public"} channel {team.internalName}/{channel.internalName} ...')
        channelOutfile = f'{"p" if private else "o"}.{team.internalName}--{channel.internalName}'

        header = ChannelHeader(channel=channel, team=team)

        self.processChannel(channelOutfile=channelOutfile, header=header, channelRequest=channelRequest)

    def __call__(self):
        '''
            Entrypoint of the Saver logic. Throws SavingFailed on known errors.
        '''
        if not self.configfile.outputDirectory.is_dir():
            self.configfile.outputDirectory.mkdir()
        m = self.driver

        logging.info(f'Logging in as {self.configfile.username}.')
        try:
            if self.configfile.token == '':
                m.login()
            self.user = m.loadLocalUser()
        except Exception as e:
            raise SavingFailed("Failed to log in. Check your credentials.") from e

        try:
            logging.info('Collecting metadata about available teams ...')
            teams = m.getTeams()
            if len(teams) == 0:
                raise SavingFailed(f'User {self.configfile.username} is not member of any teams!')

            logging.info('Collecting metadata about available channels ...')
            for team in teams.values():
                m.loadChannels(teamId=team.id)

            if self.configfile.downloadAllEmojis:
                logging.info('Downloading emoji database ...')
                emojis = self.driver.getEmojiList()
                for emoji in emojis:
                    self.enrichEmoji(emoji)
                self.processEmoji('emojis', emojis)

            logging.info('Selecting channels to download ...')
            directChannels, groupChannels = self.getWantedGlobalChannels()
            teamChannels = self.getWantedPerTeamChannels()

            logging.info('Processing channels ...')
            for user, channel in directChannels.items():
                self.processDirectChannel(user, channel)
            for channel in groupChannels:
                self.processGroupChannel(channel)
            for team, perTeamChannels in teamChannels.items():
                for channel in perTeamChannels:
                    self.processTeamChannel(team, channel)
        except KeyboardInterrupt:
            logging.info('Downloading interrupted.')
            return

        logging.info('Download process completed succesfully.')
