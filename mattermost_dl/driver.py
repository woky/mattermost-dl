'''
    Implements Mattermost driver, object that logically
    represent (connection to) Mattermost server
'''

from .common import *
from .bo import *
from .config import ConfigFile

import json
import requests
import threading
import time
from collections import deque
from time import sleep
from datetime import datetime
from email.utils import parsedate_to_datetime

@dataclass
class Cache:
    users: Dict[Id, User] = dataclassfield(default_factory=dict)
    teams: Dict[Id, Team] = dataclassfield(default_factory=dict)
    emojis: Dict[Id, Emoji] = dataclassfield(default_factory=dict)


class AdaptiveConcurrency:
    '''
        Bounds the number of simultaneous HTTP requests and adapts it to observed
        network health (AIMD, the same control TCP uses for congestion).

        `ceiling` is the user-configured maximum. The effective limit `live` shrinks
        multiplicatively toward `floor` (=1) when transient failures *cluster*, and
        grows additively back toward the ceiling on sustained success. A single
        isolated failure is treated as a flake and does not change concurrency.

        On a failure cluster a shared `pauseUntil` instant is published; every worker
        honors it before issuing its next request, so N workers don't independently
        rediscover the same outage and retry-storm into a struggling server.

        Orthogonally, a shared token bucket caps the *aggregate* request rate (req/s)
        across all workers, independent of how many run at once -- the proactive
        politeness control that adaptive concurrency (reactive) does not provide.

        The whole thing is one Condition-guarded object shared by all worker threads.
    '''
    FAILURE_WINDOW = 15.0      # s; failures this close together count as a cluster
    CLUSTER_THRESHOLD = 2      # failures within the window that trigger a backoff
    GROWTH_THRESHOLD = 5       # consecutive successes before reclaiming one permit
    DECREASE_COOLDOWN = 1.0    # s; coalesce a simultaneous failure burst into one cut

    def __init__(self, ceiling: int, rate: float = 0.0,
            clock: Callable[[], float] = time.monotonic):
        self.ceiling = max(1, ceiling)
        self.floor = 1
        self.live = self.ceiling
        self.inFlight = 0
        self.pauseUntil = 0.0
        self._successStreak = 0
        self._failures: "deque[float]" = deque()
        self._lastDecrease = float('-inf')
        self._clock = clock
        self._cond = threading.Condition()
        # Shared token bucket bounding aggregate requests/second. rate <= 0 disables
        # it. It starts full, so a configured rate still allows a short initial burst
        # (capped by the concurrency ceiling) before settling to the steady rate.
        self.rate = max(0.0, rate)
        self.capacity = max(1.0, self.rate)
        self._tokens = self.capacity
        self._lastRefill = clock()

    def acquire(self) -> None:
        '''Block until a permit, the shared cooldown, and a rate token all allow it.'''
        with self._cond:
            while True:
                now = self._clock()
                if now < self.pauseUntil:
                    self._cond.wait(timeout=self.pauseUntil - now)
                    continue
                if self.inFlight >= self.live:
                    self._cond.wait(timeout=1.0)
                    continue
                tokenWait = self._tokenWaitLocked(now)
                if tokenWait > 0:
                    self._cond.wait(timeout=tokenWait)
                    continue
                self._consumeTokenLocked()
                self.inFlight += 1
                return

    def release(self) -> None:
        with self._cond:
            if self.inFlight > 0:
                self.inFlight -= 1
            self._cond.notify_all()

    def _tokenWaitLocked(self, now: float) -> float:
        '''
            Refill the bucket for elapsed time and report how many seconds until a
            token is available (0 if one is now, or if rate limiting is disabled).
        '''
        if self.rate <= 0:
            return 0.0
        elapsed = now - self._lastRefill
        self._lastRefill = now
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        if self._tokens >= 1.0:
            return 0.0
        return (1.0 - self._tokens) / self.rate

    def _consumeTokenLocked(self) -> None:
        if self.rate > 0:
            self._tokens -= 1.0

    def __enter__(self) -> 'AdaptiveConcurrency':
        '''Hold one request permit for the duration of a `with` block.'''
        self.acquire()
        return self

    def __exit__(self, *exc) -> bool:
        self.release()
        return False

    def onSuccess(self) -> None:
        '''A completed HTTP exchange: the network is healthy, grow back slowly.'''
        with self._cond:
            self._successStreak += 1
            if self.live < self.ceiling and self._successStreak >= self.GROWTH_THRESHOLD:
                self.live = min(self.ceiling, self.live + 1)
                self._successStreak = 0
                self._cond.notify_all()

    def onTransientFailure(self, backoffSeconds: float = 0.0) -> None:
        '''
            A timeout / dropped connection / 429. Cuts concurrency and pauses all
            workers only when failures cluster; a lone failure just ages out.
        '''
        with self._cond:
            now = self._clock()
            self._successStreak = 0
            self._failures.append(now)
            while self._failures and now - self._failures[0] > self.FAILURE_WINDOW:
                self._failures.popleft()
            if len(self._failures) < self.CLUSTER_THRESHOLD:
                return
            # Multiplicative decrease, but at most once per cooldown so a burst of
            # concurrently-failing requests collapses `live` once, not N times.
            if now - self._lastDecrease >= self.DECREASE_COOLDOWN:
                self.live = max(self.floor, self.live // 2)
                self._lastDecrease = now
            self.pauseUntil = max(self.pauseUntil, now + max(0.0, backoffSeconds))
            self._cond.notify_all()

class MattermostDriver:
    API_PART = '/api/v4/'

    # Resilience against transient network failures (dropped connections,
    # read timeouts) during long bulk downloads: retry with exponential backoff
    # instead of aborting the whole run.
    HTTP_MAX_RETRIES = 5
    HTTP_RETRY_BASE_DELAY = 1.0   # seconds; doubled each attempt
    HTTP_RETRY_MAX_DELAY = 60.0   # cap on a single wait
    HTTP_CONNECT_TIMEOUT = 15.0   # seconds to establish a connection

    def __init__(self, config: ConfigFile):
        self.configfile: ConfigFile = config
        self.authorizationToken: Optional[str] = config.token if config.token else None
        # Information we get along the way
        self.context: Dict[str, Any] = {}
        self.cache = Cache()
        # Guards read-modify-write of the cache dicts when channels are downloaded
        # concurrently (enrichment looks users up from many threads). Reentrant
        # because some cache lookups call into others.
        self.cacheLock = threading.RLock()
        self.session = requests.Session()
        # Single source of truth for request pacing, shared by every worker thread:
        # caps both how many requests are in flight at once and the aggregate rate.
        # At the default ceiling of 1 and rate 0 it is a no-op passthrough, preserving
        # the original strictly-sequential, unthrottled behavior.
        self.concurrency = AdaptiveConcurrency(config.throttlingMaxConcurrency,
            rate=config.throttlingMaxRequestsPerSecond)

    def onBadHttpResponse(self, request: str, result: requests.Response) -> NoReturn:
        message = None
        messageExtra = None
        try:
            jsn = result.json()
            message = jsn['message']
            messageExtra = jsn['detailed_error']
        except Exception:
            pass
        logmessage = f"Request '{request}' failed with status code {result.status_code}.\nHTTP status: {result.reason}"
        if message:
            logmessage += "\nError message: " + message
        if messageExtra:
            logmessage += "\nError details: " + messageExtra
        logging.error(logmessage)
        result.raise_for_status()
        raise AssertionError # Never

    def getRaw(self, apiCommand: str, params: dict = {}) -> requests.Response:
        '''
            Common json returning request of GET variety.
            Arguments shall be already encoded in command
        '''
        headers = {}
        if self.authorizationToken:
            headers.update({'Authorization': 'Bearer '+self.authorizationToken})
        url = self.configfile.hostname + self.API_PART + apiCommand
        # Resilience: retry transient network failures with exponential backoff,
        # and honor server-side rate limiting (HTTP 429 + Retry-After) instead of
        # aborting. Other bad HTTP statuses are handled by onBadHttpResponse.
        # The concurrency governor caps simultaneous in-flight requests and adapts
        # that cap down when failures cluster (and back up when they stop); a
        # permit is held for the whole call, retries and backoff included.
        with self.concurrency:
            for attempt in range(self.HTTP_MAX_RETRIES + 1):
                backoff = min(self.HTTP_RETRY_MAX_DELAY,
                              self.HTTP_RETRY_BASE_DELAY * 2 ** attempt)
                try:
                    r = self.session.get(url, headers=headers, params=params,
                                          timeout=(self.HTTP_CONNECT_TIMEOUT,
                                                   self.configfile.throttlingRequestTimeout))
                except (requests.exceptions.ConnectionError,
                        requests.exceptions.Timeout,
                        requests.exceptions.ChunkedEncodingError) as e:
                    self.concurrency.onTransientFailure(backoff)
                    if attempt == self.HTTP_MAX_RETRIES:
                        logging.error(f"Request '{apiCommand}' failed after "
                                      f"{attempt + 1} attempts: {e}")
                        raise
                    logging.warning(f"Request '{apiCommand}' hit a transient network "
                                    f"error ({e}); retrying in {backoff:.0f}s "
                                    f"(attempt {attempt + 1}/{self.HTTP_MAX_RETRIES}).")
                    sleep(backoff)
                    continue
                # Server is telling us to slow down: wait as instructed and retry.
                if r.status_code == 429 and attempt < self.HTTP_MAX_RETRIES:
                    wait = self._retryAfterSeconds(r, default=backoff)
                    self.concurrency.onTransientFailure(wait)
                    logging.warning(f"Request '{apiCommand}' was rate limited (429); "
                                    f"waiting {wait:.0f}s before retry "
                                    f"(attempt {attempt + 1}/{self.HTTP_MAX_RETRIES}).")
                    sleep(wait)
                    continue
                # A completed HTTP exchange (even a 4xx/5xx): the network is healthy.
                self.concurrency.onSuccess()
                break
        if r.status_code != 200:
            self.onBadHttpResponse(apiCommand, r)
        return r

    @staticmethod
    def _retryAfterSeconds(response: requests.Response, default: float) -> float:
        '''
            How long to wait per a 429 response's Retry-After header, accepting
            either delta-seconds or an HTTP-date. Falls back to `default`.
        '''
        value = response.headers.get('Retry-After')
        if not value:
            return default
        try:
            return max(0.0, float(value))
        except ValueError:
            pass
        try:
            when = parsedate_to_datetime(value)
            return max(0.0, (when - datetime.now(when.tzinfo)).total_seconds())
        except (TypeError, ValueError):
            return default

    def get(self, apiCommand: str, params: dict = {}) -> Union[dict, list]:
        '''
            Common json returning request of GET variety.
            Arguments shall be already encoded in command
        '''
        apiCommand = apiCommand.format(**self.context)
        r = self.getRaw(apiCommand, params)
        r = r.json()
        # We're guaranteeing certain types on output
        if not isinstance(r, (dict, list)):
            raise TypeError
        return r

    def storeUrlInto(self, url: str, fp: BinaryIO):
        response = self.getRaw(url)
        fp.write(response.content)

    def postRaw(self, apiCommand: str, data: Union[bytes, str]) -> requests.Response:
        '''
            Common json passing returning request of POST variety.
        '''
        headers = {}
        if self.authorizationToken:
            headers.update({'Token': self.authorizationToken})
        r = self.session.post(self.configfile.hostname + self.API_PART + apiCommand, data, headers=headers)
        if r.status_code != 200:
            self.onBadHttpResponse(apiCommand, r)
        return r

    def post(self, apiCommand: str, data: dict) -> dict:
        '''
            Common json passing returning request of POST variety.
        '''
        apiCommand = apiCommand.format(**self.context)
        r = self.postRaw(apiCommand, data=json.dumps(data))
        return r.json()

    def login(self):
        r = self.postRaw('users/login', json.dumps({
            'login_id': self.configfile.username,
            'password': self.configfile.password
        }))

        self.authorizationToken = r.headers['Token']

    def getUserById(self, id: Id) -> User:
        with self.cacheLock:
            if id in self.cache.users:
                return self.cache.users[id]

        userInfo = self.get('users/'+id)
        assert isinstance(userInfo, dict)
        u = User.fromMattermost(userInfo)
        with self.cacheLock:
            self.cache.users.update({u.id: u})
        return u

    def getUserByName(self, userName: str) -> User:
        with self.cacheLock:
            for user in self.cache.users.values():
                if user.name == userName:
                    return user

        userInfo = self.get('users/username/'+userName)
        assert isinstance(userInfo, dict)
        u = User.fromMattermost(userInfo)
        with self.cacheLock:
            self.cache.users.update({u.id: u})
        return u

    def loadLocalUser(self) -> User:
        u = self.getUserByName(self.configfile.username)
        self.context['userId'] = u.id
        return u

    def getTeams(self) -> Dict[Id, Team]:
        with self.cacheLock:
            if len(self.cache.teams) != 0:
                return self.cache.teams
        teamInfos = self.get('users/{userId}/teams')
        assert isinstance(teamInfos, list)
        with self.cacheLock:
            for teamInfo in teamInfos:
                t = Team.fromMattermost(teamInfo)
                self.cache.teams.update({t.id: t})
            return self.cache.teams

    def getTeamById(self, teamId: Id) -> Team:
        return self.getTeams()[teamId]
    def getTeamByName(self, name: str) -> Team:
        teams = self.getTeams()
        for team in teams.values():
            if team.name == name:
                return team
        raise KeyError
    def getTeamByIntenalName(self, name: str) -> Team:
        teams = self.getTeams()
        for team in teams.values():
            if team.internalName == name:
                return team
        raise KeyError

    def loadChannels(self, teamId: Id = None):
        if not teamId:
            teamId = Id(self.context['teamId'])
        channelInfos = self.get(f'users/{{userId}}/teams/{teamId}/channels')
        t = self.cache.teams[teamId]
        assert isinstance(channelInfos, list)
        for chInfo in channelInfos:
            ch = Channel.fromMattermost(chInfo)
            t.channels.update({ch.id: ch})

    def getChannelById(self, channelId: Id, teamId: Id = None) -> Channel:
        if teamId is None:
            teamId = self.context['teamId']
            assert teamId is not None
        return self.cache.teams[teamId].channels[channelId]
    def getChannelByName(self, name: str, teamId: Id = None) -> Channel:
        if teamId is None:
            teamId = self.context['teamId']
            assert teamId is not None
        for channel in self.cache.teams[teamId].channels.values():
            if channel.name == name:
                return channel
        raise KeyError

    def getDirectChannelNameByUserId(self, otherUserId: Id):
        localUserId = self.context['userId']
        if localUserId < otherUserId:
            return f'{localUserId}__{otherUserId}'
        else:
            return f'{otherUserId}__{localUserId}'
    def getDirectChannelNameByUserName(self, otherUserName: str):
        return self.getDirectChannelNameByUserId(self.getUserByName(otherUserName).id)

    def getDirectChannelByUserName(self, otherUserName: str, teamId = None) -> Channel:
        if not teamId:
            teamId = self.context['teamId']
        channelName = self.getDirectChannelNameByUserName(otherUserName)
        for channel in self.cache.teams[teamId].channels.values():
            if channel.type == ChannelType.Direct and channel.internalName == channelName:
                return channel
        raise KeyError

    def getUserIdFromDirectChannelName(self, channelName: str) -> Id:
        '''
            Gets userId of the nonlocal user in direct (private) channel.
        '''
        left, right = channelName.split('__')
        if left == self.context['userId']:
            return Id(right)
        else:
            return Id(left)

    def loadChannelMembers(self, channel: Channel):
        if channel.members is not None:
            return

        res = []

        page = 0
        params = {
            'per_page': 100
        }
        while True:
            params.update({'page': page})
            memberWindow = self.get(f'channels/{channel.id}/members', params)
            assert isinstance(memberWindow, list)
            for m in memberWindow:
                res.append(self.getUserById(m['user_id']))

            if len(memberWindow) == 0 or len(memberWindow) < 100:
                break

            page += 1

        channel.members = res

    def getPostById(self, postId: Id) -> Post:
        postInfo = self.get(f'/posts/{postId}')
        assert isinstance(postInfo, dict)
        return Post.fromMattermost(postInfo)

    @dataclass
    class PostHints:
        processedCount: int = 0
        # Id of post directly chronologically preceding current one. None if the post is first in channel
        postIdBefore: Optional[Id] = None
        # Id of post directly chronologically succeeding current one. None if the post is last in channel
        postIdAfter: Optional[Id] = None

    class ProcessPostResult(Enum):
        NothingRequested = enumerator()
        NoMorePosts = enumerator()
        MaxCountReached = enumerator()
        ConditionReached = enumerator()

    def processPosts(self, processor: Callable[[Post, 'MattermostDriver.PostHints'], None],
            channel: Optional[Channel] = None, *,
            beforePost: Optional[Id] = None, afterPost: Optional[Id] = None,
            beforeTime: Optional[Time] = None, afterTime: Optional[Time] = None,
            bufferSize: int = 0, maxCount: int = 0, offset: int = 0,
            onSkippedPost: Callable[[], None] = (lambda: None)
            ) -> 'MattermostDriver.ProcessPostResult':
        '''
            Main function to load all channel's posts.
            Loading happens lazily in batches, each post is passed to external callable.

            Download always runs newest->oldest, the Mattermost-native direction:
                - `before`/`beforePost` is the only server-side cursor: seed it (resume
                  or user lower bound), then after each page move it to the earliest
                  fetched post and keep paging back.
                - The lower bound (afterPost/afterTime) is enforced *client-side*, by
                  stopping the walk when it is reached. It must NOT be sent to the
                  server as `after=`: Mattermost does not intersect `after` with the
                  moving `before` cursor (it honours `after` and ignores `before`), so
                  the page never advances and the same newest posts repeat forever.
                - apply offset
                - skip posts newer than beforeTime, then start processing
                - continue collecting until the channel start, maxCount, afterPost or
                  afterTime is reached
        '''
        if not bufferSize:
            bufferSize = self.configfile.throttlingPageSize
        if channel:
            channelId = channel.id
        else:
            channelId = Id(self.context['channelId'])
            channel = self.getChannelById(channelId)

        params: Dict[str, Any] = {
            'per_page': bufferSize
        }
        # NB: afterPost is deliberately NOT sent as `after=`; it is only a client-side
        # stop (see the loop below). Sending it alongside the moving `before` cursor
        # makes the server ignore the cursor and re-return the same page endlessly.
        if beforePost:
            params.update(before=beforePost)

        # The processed window is [afterTime, beforeTime): posts with create_at
        # >= afterTime (lower stop) and < beforeTime (upper skip). It is empty only
        # when afterTime >= beforeTime, in which case short-circuit instead of
        # paging through the whole channel skipping everything.
        if afterTime and beforeTime and afterTime >= beforeTime:
            return self.ProcessPostResult.NothingRequested

        page: int = offset // bufferSize
        # How many messages on page shall be ignored (in the download direction)
        pageOffset: int = offset % bufferSize

        postHints = self.PostHints()
        while True:
            if page != 0:
                params.update(page=page)
            postWindow = self.get(f'channels/{channelId}/posts', params=params)
            assert isinstance(postWindow, dict)

            stopReason: Optional[MattermostDriver.ProcessPostResult] = None

            for windowIndex, postId in enumerate(postWindow['order'][pageOffset:]):
                p = postWindow['posts'][postId]
                postHints.postIdBefore = postWindow['order'][windowIndex + 1] if windowIndex + 1 < len(postWindow['order']) else postWindow['prev_post_id'] if postWindow['prev_post_id'] != '' else None
                postHints.postIdAfter = postWindow['order'][windowIndex - 1] if windowIndex - 1 >= 0 else postWindow['next_post_id'] if postWindow['next_post_id'] != '' else None
                if ((afterPost and p['id'] == afterPost)
                    or (afterTime and p['create_at'] < afterTime.timestamp)):
                    stopReason = self.ProcessPostResult.ConditionReached
                    break
                if maxCount and postHints.processedCount == maxCount:
                    stopReason = self.ProcessPostResult.MaxCountReached
                    break
                if beforeTime and p['create_at'] >= beforeTime.timestamp:
                    onSkippedPost()
                    continue
                processor(Post.fromMattermost(p), postHints)
                postHints.processedCount += 1

            # No messages recieved?
            if len(postWindow['order']) == 0:
                return self.ProcessPostResult.NoMorePosts

            if stopReason is not None:
                return stopReason
            if maxCount and postHints.processedCount >= maxCount:
                return self.ProcessPostResult.MaxCountReached

            if postWindow['prev_post_id'] == '':
                return self.ProcessPostResult.NoMorePosts
            params.update(before = postWindow['order'][-1])

            if page != 0:
                page = 0
                del params['page']
            if pageOffset != 0:
                pageOffset = 0

    def getPosts(self, channel: Channel = None, *args, **kwargs) -> List[Post]:
        result = []
        def process(p: Post, hints: 'MattermostDriver.PostHints'):
            result.append(p)
        self.processPosts(channel=channel, processor=process, *args, **kwargs)
        return result

    def processEmojiList(self, processor: Callable[[Emoji], None], bufferSize: int = 0, maxCount: int = 0):
        if not bufferSize:
            bufferSize = self.configfile.throttlingPageSize
        params = {
            'per_page': bufferSize
        }

        recieved = 0
        page = 0
        while True:
            if maxCount and maxCount - recieved < bufferSize:
                params.update({"per_page": maxCount - recieved})
            params.update({"page": page})
            emojiWindow = self.get('emoji', params)
            assert isinstance(emojiWindow, list)
            for emojiInfo in emojiWindow:
                e = Emoji.fromMattermost(emojiInfo)
                with self.cacheLock:
                    self.cache.emojis.update({e.id: e})
                processor(e)
            recieved += len(emojiWindow)
            if len(emojiWindow) < bufferSize or (maxCount and recieved >= maxCount):
                break
            page += 1

    def getEmojiList(self, *args, **kwargs) -> List[Emoji]:
        result = []
        def process(p: Emoji):
            result.append(p)
        self.processEmojiList(processor=process, *args, **kwargs)
        return result

    def getEmojiById(self, emojiId: Id) -> Emoji:
        if len(self.cache.emojis) == 0:
            self.getEmojiList()
        with self.cacheLock:
            if emojiId in self.cache.emojis:
                return self.cache.emojis[emojiId]
            else:
                raise KeyError

    def getEmojiByName(self, emojiName: str) -> Emoji:
        if len(self.cache.emojis) == 0:
            self.getEmojiList()
        with self.cacheLock:
            for emoji in self.cache.emojis.values():
                if emoji.name == emojiName:
                    return emoji
        raise KeyError

    def getEmojiUrl(self, emoji: Emoji) -> str:
        return f'emoji/{emoji.id}/image'

    def getFileUrl(self, file: FileAttachment, publicUrl = False) -> str:
        # Note: public access links may be unimplemented by server
        if publicUrl:
            return f'{self.configfile.hostname}{self.API_PART}files/{file.id}/link'
        else:
            return f'files/{file.id}'

    def getAvatarUrl(self, user: User) -> str:
        return f'users/{user.id}/image'
