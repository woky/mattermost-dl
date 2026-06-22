'''
    Shared test fixtures: an in-memory Mattermost posts API and small builders.
'''

import copy
import json
import threading
from pathlib import Path

from mattermost_dl import progress
from mattermost_dl.config import ConfigFile
from mattermost_dl.driver import MattermostDriver
from mattermost_dl.saver import Saver


def mmPost(id, create_at, user_id='u1', message='m', **extra):
    '''A Mattermost-API-shaped post dict, as returned by the posts endpoint.'''
    post = {
        'id': id, 'user_id': user_id, 'create_at': create_at, 'update_at': create_at,
        'edit_at': 0, 'delete_at': 0, 'message': message, 'props': {}, 'type': '',
        'metadata': {}, 'root_id': '', 'parent_id': '',
    }
    post.update(extra)
    return post


class FakePostsDriver(MattermostDriver):
    '''
        MattermostDriver whose HTTP `get` is replaced by an in-memory channel,
        faithfully honoring the per_page / page / before / after semantics the
        real newest->oldest walk relies on. `requestLog` records every query so a
        test can assert what was (and was not) re-fetched.
    '''
    def __init__(self, config, postsOldestToNewest, interruptAfterGets=None):
        super().__init__(config)
        self.allPosts = list(postsOldestToNewest)
        self.byId = {p['id']: p for p in self.allPosts}
        self.requestLog = []
        # Guards requestLog so concurrent channel workers can share one driver.
        self._logLock = threading.Lock()
        # Raise KeyboardInterrupt once this many `get` calls have been served,
        # to emulate a Ctrl-C mid-download.
        self.interruptAfterGets = interruptAfterGets

    def get(self, command, params=None):
        params = dict(params or {})
        with self._logLock:
            self.requestLog.append(params)
            served = len(self.requestLog)
        if self.interruptAfterGets is not None and served > self.interruptAfterGets:
            raise KeyboardInterrupt
        perPage = params.get('per_page', 60)
        page = params.get('page', 0)
        before = params.get('before')
        after = params.get('after')

        cands = list(reversed(self.allPosts))  # newest -> oldest
        # `before`/`after` are no-ops if the referenced post is unknown (e.g. it was
        # deleted server-side), matching the real API and exercising the time stops.
        if before is not None and before in self.byId:
            boundary = self.byId[before]['create_at']
            cands = [p for p in cands if p['create_at'] < boundary]
        if after is not None and after in self.byId:
            boundary = self.byId[after]['create_at']
            cands = [p for p in cands if p['create_at'] > boundary]

        start = page * perPage
        window = cands[start:start + perPage]
        order = [p['id'] for p in window]
        # Return deep copies: Post.fromMattermost consumes (pops keys from) the dict.
        posts = {p['id']: copy.deepcopy(p) for p in window}
        prev_post_id = cands[start + perPage]['id'] if len(cands) > start + perPage else ''
        next_post_id = cands[start - 1]['id'] if 0 < start < len(cands) + 1 and start - 1 < len(cands) else ''
        return {'order': order, 'posts': posts,
                'prev_post_id': prev_post_id, 'next_post_id': next_post_id}

    def getUserById(self, id):
        return {
            'id': id, 'username': f'user-{id}', 'nickname': '', 'first_name': '',
            'last_name': '', 'create_at': 0, 'update_at': 0, 'delete_at': 0,
            'position': '', 'roles': 'system_user',
        }

    def getEmojiById(self, id):
        return {
            'id': id, 'creator_id': 'c', 'name': f'emoji-{id}',
            'create_at': 0, 'update_at': 0, 'delete_at': 0,
        }


def makeConfig(outdir, pageSize=60):
    config = ConfigFile()
    config.outputDirectory = Path(outdir)
    config.throttlingPageSize = pageSize
    # Disable the progress reporter so tests stay quiet and deterministic.
    config.reportProgress = progress.ProgressSettings(
        mode=progress.VisualizationMode.DumbTerminal, forceMode=True)
    return config


def makeChannel(id='chan', messageCount=100):
    '''A Mattermost-API-shaped open channel dict, as the driver yields it.'''
    return {
        'id': id, 'name': id, 'display_name': id, 'type': 'O',
        'create_at': 0, 'update_at': 0, 'delete_at': 0,
        'header': '', 'purpose': '', 'last_post_at': 0,
        'total_msg_count': messageCount, 'creator_id': '',
    }


def makeSaver(config, driver):
    return Saver(config, driver=driver)


def readStoredIds(dataFile):
    '''Ids stored in a data/buffer file, in file order.'''
    return [json.loads(line)['id'] for line in Path(dataFile).read_text().splitlines() if line]
