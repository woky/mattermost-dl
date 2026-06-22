'''
    Contains high level logic of history downloading.

    This orchestration layer is storage-independent: it works on raw Mattermost
    API reply dicts and hands them to a pluggable storage backend, which owns the
    persisted format. It never sees the backend's internal representation.
'''

from .common import *

from .config import ChannelOptions, ConfigFile, GroupChannelSpec, LogVerbosity, TeamSpec
from .driver import (MattermostDriver, CHANNEL_OPEN, CHANNEL_PRIVATE,
    CHANNEL_GROUP, CHANNEL_DIRECT)
from . import progress
from .storage import makeBackend

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed


@dataclass
class ChannelRequest:
    config: ChannelOptions
    metadata: dict  # raw channel API reply

    def __hash__(self) -> int:
        return hash(self.metadata['id'])


def _matchChannel(channel: dict, locator) -> bool:
    if hasattr(locator, 'id'):
        return channel['id'] == locator.id
    elif hasattr(locator, 'internalName'):
        return channel['name'] == locator.internalName
    else:
        return channel.get('display_name') == locator.name


def _matchTeam(team: dict, locator) -> bool:
    if hasattr(locator, 'id'):
        return team['id'] == locator.id
    elif hasattr(locator, 'internalName'):
        return team['name'] == locator.internalName
    else:
        return team.get('display_name') == locator.name


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
    def __init__(self, configfile: ConfigFile, driver: Optional[MattermostDriver] = None):
        if driver is None:
            driver = MattermostDriver(configfile)
        self.configfile = configfile
        self.driver: MattermostDriver = driver
        self.user: dict  # Conveniency, fetched on call (raw user reply)
        # Set to ask in-flight channel workers to wind down (e.g. on Ctrl-C); each
        # leaves its resumable .tmp buffer behind. Only consulted when channels are
        # downloaded concurrently; the sequential path relies on KeyboardInterrupt.
        self.stopEvent = threading.Event()
        # Single progress path: a live multi-line block on a tty (one line per active
        # channel), plain periodic lines off a tty, nothing when disabled.
        self.progress = self._makeProgressManager()
        # The storage backend is fed raw API dicts and owns the persisted format;
        # the driver supplies the entity-resolution and asset-fetching it needs.
        self.backend = makeBackend(configfile, self.driver, self.progress)

    def getUserByLocator(self, locator: EntityLocator) -> dict:
        if hasattr(locator, 'id'):
            return self.driver.getUserById(locator.id)
        elif hasattr(locator, 'name'):
            return self.driver.getUserByName(locator.name)
        elif hasattr(locator, 'internalName'):
            return self.driver.getUserByName(locator.internalName)
        else:
            raise ValueError

    def matchGroupChannel(self, channel: dict, locator: Union[Id, FrozenSet[EntityLocator]]) -> bool:
        if isinstance(locator, str):
            return channel['id'] == locator
        else:
            assert isinstance(locator, frozenset)
            members = self.driver.loadChannelMembers(channel['id'])
            wantedIds = {self.getUserByLocator(userLocator)['id'] for userLocator in locator}
            wantedIds.add(self.user['id'])
            return wantedIds == {m['id'] for m in members}

    def getWantedUsers(self) -> List[Tuple[dict, ChannelOptions]]:
        userIds = set()
        res = []
        for userSpec in self.configfile.explicitUsers:
            u = self.getUserByLocator(userSpec.locator)
            if u['id'] in userIds:
                logging.warning(f"Explicitly requesting direct messages for user {u['username']} more than once.")
            else:
                userIds.add(u['id'])
                res.append((u, userSpec.opts))
        return res

    def getWantedGlobalChannels(self) -> Tuple[Dict[Id, Tuple[dict, ChannelRequest]], Set[ChannelRequest]]:
        '''
            Collects a list of channels requested by configfile that aren't scoped under Team.
            Returns pair representing channel requests for users (keyed by the other
            user's id) and groups respectively.
        '''
        wantedDirectChannels: Dict[Id, Tuple[dict, ChannelRequest]] = {}
        wantedGroupChannels: Set[ChannelRequest] = set()
        explicitDirectChannelNames = {self.driver.getDirectChannelNameByUserId(u['id']): (u, opts) for u, opts in self.getWantedUsers()}
        matchedGroupChannels: Set[GroupChannelSpec] = set()
        for teamId in self.driver.getTeams():
            for channel in self.driver.getChannels(teamId).values():
                ctype = channel['type']
                if ctype == CHANNEL_DIRECT:
                    # If we don't have this channel already
                    if channel['id'] not in (req.metadata['id'] for _, req in wantedDirectChannels.values()):
                        internalName = channel['name']
                        if internalName in explicitDirectChannelNames:
                            u, opts = explicitDirectChannelNames[internalName]
                            wantedDirectChannels[u['id']] = (u, ChannelRequest(config=opts, metadata=channel))
                            del explicitDirectChannelNames[internalName]
                        elif self.configfile.miscDirectChannels:
                            otherUser = self.driver.getUserById(self.driver.getUserIdFromDirectChannelName(internalName))
                            wantedDirectChannels[otherUser['id']] = (otherUser, ChannelRequest(config=self.configfile.directChannelDefaults, metadata=channel))
                elif ctype == CHANNEL_GROUP:
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
            logging.warning(f"Found no direct channel with {user['username']}.")
        for wch in self.configfile.explicitGroups:
            if wch not in matchedGroupChannels:
                logging.warning(f'Found no group channel via locator {wch.locator}.')
        return wantedDirectChannels, wantedGroupChannels

    def getWantedPerTeamChannels(self) -> Dict[Id, Tuple[dict, List[ChannelRequest]]]:
        if self.configfile.miscTeams is False and len(self.configfile.explicitTeams) == 0:
            return {}

        res: Dict[Id, Tuple[dict, List[ChannelRequest]]] = {}
        teams = self.driver.getTeams()

        def getChannelsForTeam(teamId: Id, team: dict, wantedTeam: TeamSpec) -> List[ChannelRequest]:
            channels = []
            explicitPublicLocators = {ch.locator for ch in wantedTeam.explicitPublicChannels}
            explicitPrivateLocators = {ch.locator for ch in wantedTeam.explicitPrivateChannels}

            for availableChannel in self.driver.getChannels(teamId).values():
                ctype = availableChannel['type']
                if ctype == CHANNEL_OPEN:
                    for wch in wantedTeam.explicitPublicChannels:
                        if _matchChannel(availableChannel, wch.locator):
                            channels.append(ChannelRequest(config=wch.opts, metadata=availableChannel))
                            explicitPublicLocators.remove(wch.locator)
                            break
                    else:
                        if wantedTeam.miscPublicChannels:
                            channels.append(ChannelRequest(config=wantedTeam.publicChannelDefaults, metadata=availableChannel))
                elif ctype == CHANNEL_PRIVATE:
                    for wch in wantedTeam.explicitPrivateChannels:
                        if _matchChannel(availableChannel, wch.locator):
                            channels.append(ChannelRequest(config=wch.opts, metadata=availableChannel))
                            explicitPrivateLocators.remove(wch.locator)
                            break
                    else:
                        if wantedTeam.miscPrivateChannels:
                            channels.append(ChannelRequest(config=wantedTeam.privateChannelDefaults, metadata=availableChannel))
            for loc in explicitPublicLocators:
                logging.warning(f"Found no requested public channel on team {team['name']} ({team['display_name']}) via locator {loc}.")
            for loc in explicitPrivateLocators:
                logging.warning(f"Found no requested private channel on team {team['name']} ({team['display_name']}) via locator {loc}.")
            return channels

        explicitTeamLocators: Set[EntityLocator] = {t.locator for t in self.configfile.explicitTeams}
        for teamId, availableTeam in teams.items():
            for wantedTeam in self.configfile.explicitTeams:
                if _matchTeam(availableTeam, wantedTeam.locator):
                    res[teamId] = (availableTeam, getChannelsForTeam(teamId, availableTeam, wantedTeam))
                    explicitTeamLocators.remove(wantedTeam.locator)
                    break
            else:
                if self.configfile.miscTeams:
                    channels = []
                    for ch in self.driver.getChannels(teamId).values():
                        if ch['type'] == CHANNEL_OPEN:
                            channels.append(ChannelRequest(config=self.configfile.publicChannelDefaults, metadata=ch))
                        elif ch['type'] == CHANNEL_PRIVATE:
                            channels.append(ChannelRequest(config=self.configfile.privateChannelDefaults, metadata=ch))
                    res[teamId] = (availableTeam, channels)
        for loc in explicitTeamLocators:
            logging.error(f'Team requested by {loc} was not found!')
        return res

    def _makeProgressManager(self) -> progress.ProgressManager:
        '''
            The one progress reporter for the whole run. Disabled (a no-op) for quiet
            runs or when progress is switched off in config; otherwise the manager
            renders a live multi-line block on a tty and plain periodic lines off one.
        '''
        enabled = (self.configfile.verbosity == LogVerbosity.Normal
            and self.configfile.reportProgress.mode != progress.VisualizationMode.DumbTerminal)
        return progress.ProgressManager(sys.stderr, settings=self.configfile.reportProgress,
            updateIntervalMs=self.configfile.progressInterval, enabled=enabled)

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

    def processChannel(self, channelOutfile: str, channelRequest: ChannelRequest, *,
            team: Optional[dict] = None, seedUsers: Iterable[dict] = (),
            label: Optional[str] = None):
        '''
            Downloads a channel into the storage backend.

            The download is driven purely by the persisted state (never by stored
            metadata):
              - no committed posts, no buffer  -> fresh first download
              - no committed posts, buffer     -> resume an interrupted first download
              - committed posts, no buffer     -> incremental update (stops at local newest)
              - committed posts, buffer        -> resume an interrupted incremental update
        '''
        channel, options = channelRequest.metadata, channelRequest.config

        if options.postLimit == 0 or options.postSessionLimit == 0:
            return  # Early exit - nothing downloaded

        archive = self.backend.channelArchive(channelOutfile, channel, team, options, seedUsers)

        resumed = self._runDownloadCycle(archive, channel, options, label=label)
        if resumed:
            # The resumed buffer's newest post was frozen at the channel-newest of
            # the interrupted run; run one fresh incremental to capture anything that
            # has arrived above it since.
            self._runDownloadCycle(archive, channel, options, label=label)

    def _runDownloadCycle(self, archive, channel: dict, options: ChannelOptions, *,
            label: Optional[str] = None) -> bool:
        '''
            One pass of the state machine: reconcile any interrupted buffer, fetch
            newest->oldest into the buffer, then commit it.

            Returns True if this pass resumed an interrupted buffer, so the caller
            should run one more fresh incremental to catch up.
        '''
        resumeState = archive.reconcileBuffer()

        newest = archive.committedNewestPost()
        incremental = newest is not None
        localNewestId = newest[0] if newest is not None else None
        localNewestTime = newest[1] if newest is not None else None

        dlParams = self._buildDriverParams(options, resume=resumeState.resume,
            resumeCursor=resumeState.resumeCursor, incremental=incremental,
            localNewestId=localNewestId, localNewestTime=localNewestTime)

        self._fetchIntoBuffer(archive, channel, options, resume=resumeState.resume,
            dlParams=dlParams, priorCount=resumeState.priorCount, label=label)
        archive.commit(incremental=incremental, localNewestId=localNewestId)
        return resumeState.resume

    def _fetchIntoBuffer(self, archive, channel: dict, options: ChannelOptions, *,
            resume: bool, dlParams: Dict[str, Any], priorCount: int = 0,
            label: Optional[str] = None):
        '''
            Streams raw posts newest->oldest into the backend's staging buffer
            (append when resuming, else fresh). On interruption the partial buffer
            is left for resuming.

            `priorCount` is how many posts a resumed buffer already holds, so the
            progress count continues from there instead of restarting at 0.

            `label` is the human-readable name shown on the progress line; it falls
            back to the channel's internal name. Direct and group channels pass a
            friendly label because their internal name is a user-id blob.
        '''
        bufferedCount = priorCount  # total posts in the buffer (prior + this run)
        sessionCount = 0            # posts fetched during this run only

        # Approximate upper bound used as the progress line's denominator.
        estimatedPostLimit: int = channel['total_msg_count']
        if options.postLimit != -1:
            estimatedPostLimit = min(estimatedPostLimit, options.postLimit)
        if options.postSessionLimit != -1:
            estimatedPostLimit = min(estimatedPostLimit, options.postSessionLimit)

        # The task line is drawn lazily on its first update, so an up-to-date channel
        # (nothing fetched) shows no progress line at all.
        with archive.stagingWriter(resume=resume) as writer, \
                self.progress.task(label or channel['name']) as task:
            def perPost(rawPost: dict, hints: PostHints):
                nonlocal bufferedCount, sessionCount
                # Cooperative stop: bail out the same way a Ctrl-C would, leaving the
                # partial buffer on disk so the next run resumes it. (At most one more
                # page is fetched after a stop is requested.)
                if self.stopEvent.is_set():
                    raise KeyboardInterrupt
                writer.add(rawPost)
                bufferedCount += 1
                sessionCount += 1
                task.update(bufferedCount, estimatedPostLimit)

            def onSkippedPost():
                if self.stopEvent.is_set():
                    raise KeyboardInterrupt
                task.update(note='skipping posts outside the requested range')

            postProcessRes = self.driver.processPosts(processor=perPost, channel=channel, **dlParams, onSkippedPost=onSkippedPost)

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

    def processDirectChannel(self, otherUser: dict, channelRequest: ChannelRequest):
        logging.info(f"Processing conversation with {otherUser['username']} ...")
        directChannelOutfile = f"d.{self.user['username']}--{otherUser['username']}"
        self.processChannel(directChannelOutfile, channelRequest, seedUsers=[self.user, otherUser],
            label=f"@{otherUser['username']}")

    def processGroupChannel(self, channelRequest: ChannelRequest):
        channel = channelRequest.metadata
        members = self.driver.loadChannelMembers(channel['id'])
        userlist = '-'.join(sorted(u['username'] for u in members))
        if userlist == '':
            logging.warning(f"No users for group channel {channel['id']}, using id as name!")
            userlist = str(channel['id'])
        logging.info(f"Processing group chat {userlist} ...")
        channelOutfile = f'g.{userlist}'
        self.processChannel(channelOutfile, channelRequest, label=userlist)

    def processTeamChannel(self, team: dict, channelRequest: ChannelRequest):
        '''
            Processes public and private channels
        '''
        channel = channelRequest.metadata
        private = channel['type'] == CHANNEL_PRIVATE
        logging.info(f"Processing {'private' if private else 'public'} channel {team['name']}/{channel['name']} ...")
        channelOutfile = f"{'p' if private else 'o'}.{team['name']}--{channel['name']}"
        self.processChannel(channelOutfile, channelRequest, team=team)

    def __call__(self):
        '''
            Entrypoint of the Saver logic. Throws SavingFailed on known errors.
        '''
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
            for teamId in teams:
                m.loadChannels(teamId=teamId)

            with self.backend:
                if self.configfile.downloadAllEmojis:
                    logging.info('Downloading emoji database ...')
                    self.backend.downloadEmojiDatabase(self.driver.getEmojiList())

                logging.info('Selecting channels to download ...')
                directChannels, groupChannels = self.getWantedGlobalChannels()
                teamChannels = self.getWantedPerTeamChannels()

                logging.info('Processing channels ...')
                # Each channel is independent of the others, so they can download
                # concurrently. Bind loop vars per task.
                tasks: List[Callable[[], None]] = []
                for otherUser, channel in directChannels.values():
                    tasks.append(lambda u=otherUser, c=channel: self.processDirectChannel(u, c))
                for channel in groupChannels:
                    tasks.append(lambda c=channel: self.processGroupChannel(c))
                for team, perTeamChannels in teamChannels.values():
                    for channelReq in perTeamChannels:
                        tasks.append(lambda t=team, c=channelReq: self.processTeamChannel(t, c))
                # The progress manager owns the live region for the download phase and
                # routes every log line above it, so the two never clobber each other.
                with self.progress, self.progress.captureLogging():
                    self._processChannels(tasks)
        except KeyboardInterrupt:
            logging.info('Downloading interrupted.')
            self.stopEvent.set()
            return

        logging.info('Download process completed succesfully.')

    def _runChannelTask(self, task: Callable[[], None]):
        '''
            Runs one channel download in a worker, treating a cooperative stop
            (KeyboardInterrupt raised once stopEvent is set) as a clean return so the
            partial .tmp buffer is left for resuming and the pool isn't torn down.
        '''
        if self.stopEvent.is_set():
            return
        try:
            task()
        except KeyboardInterrupt:
            self.stopEvent.set()

    def _processChannels(self, tasks: List[Callable[[], None]]):
        '''
            Downloads the channels. At the default ceiling of 1 this is the original
            strictly-sequential walk (and a real Ctrl-C propagates straight to the
            caller). Above 1, channels run in a thread pool bounded by the same
            ceiling; the driver's governor is the real cap on in-flight requests.
        '''
        workers = max(1, self.configfile.throttlingMaxConcurrency)
        if workers == 1:
            for task in tasks:
                task()
            return

        firstError: Optional[BaseException] = None
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(self._runChannelTask, task) for task in tasks]
            try:
                for future in as_completed(futures):
                    exc = future.exception()
                    if exc is not None and firstError is None:
                        # A genuine failure (not a cooperative stop): wind the others
                        # down too, then re-raise once the pool has drained.
                        firstError = exc
                        self.stopEvent.set()
            except KeyboardInterrupt:
                # Ctrl-C reached the main thread while waiting; ask workers to stop
                # (each leaves a resumable buffer) and let the pool drain.
                self.stopEvent.set()
                raise
        if firstError is not None:
            raise firstError
