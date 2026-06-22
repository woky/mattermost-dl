'''
    Tests for the newest->oldest MattermostDriver.processPosts walk: pagination, the
    server-side `before` cursor, the client-side afterPost/afterTime lower-bound stops
    and the maxCount / beforeTime conditions.
'''

import tempfile
import unittest

from mattermost_dl.types import Time
from mattermost_dl.config import ConfigFile
from mattermost_dl.driver import MattermostDriver

from .helpers import FakePostsDriver, makeChannel, makeConfig, mmPost


class ProcessPostsTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        # Posts p1..p7 oldest -> newest, create_at 10..70.
        self.posts = [mmPost(f'p{i}', i * 10) for i in range(1, 8)]
        # Small page size to exercise multi-page backward paging.
        self.config = makeConfig(self._dir.name, pageSize=3)
        self.driver = FakePostsDriver(self.config, self.posts)
        self.channel = makeChannel(messageCount=len(self.posts))

    def tearDown(self):
        self._dir.cleanup()

    def collect(self, **kwargs):
        out = []
        result = self.driver.processPosts(processor=lambda p, h: out.append(p["id"]),
                                          channel=self.channel, **kwargs)
        return out, result

    def test_full_walk_newest_to_oldest(self):
        ids, result = self.collect()
        self.assertEqual(ids, ['p7', 'p6', 'p5', 'p4', 'p3', 'p2', 'p1'])
        self.assertEqual(result, MattermostDriver.ProcessPostResult.NoMorePosts)
        # Multiple pages were needed (pageSize 3, 7 posts).
        self.assertGreater(len(self.driver.requestLog), 1)

    def test_max_count(self):
        ids, result = self.collect(maxCount=3)
        self.assertEqual(ids, ['p7', 'p6', 'p5'])
        self.assertEqual(result, MattermostDriver.ProcessPostResult.MaxCountReached)

    def test_after_post_stops_walk(self):
        # afterPost=p4 -> walk newest->oldest and stop when p4 is reached (client-side,
        # never sent to the server as after=).
        ids, _ = self.collect(afterPost='p4')
        self.assertEqual(ids, ['p7', 'p6', 'p5'])
        self.assertTrue(all('after' not in req for req in self.driver.requestLog),
                        self.driver.requestLog)

    def test_after_time_backstops_deleted_after_post(self):
        # afterPost id is unknown (post deleted server-side); afterTime must stop us.
        ids, result = self.collect(afterPost='deleted', afterTime=Time(40))
        self.assertEqual(ids, ['p7', 'p6', 'p5', 'p4'])  # p3 (create 30 < 40) stops
        self.assertEqual(result, MattermostDriver.ProcessPostResult.ConditionReached)

    def test_before_post_resume_cursor(self):
        ids, _ = self.collect(beforePost='p5')
        self.assertEqual(ids, ['p4', 'p3', 'p2', 'p1'])
        # The very first request seeks straight to the cursor; p7/p6/p5 never fetched.
        self.assertEqual(self.driver.requestLog[0].get('before'), 'p5')

    def test_before_time_skips_newer(self):
        ids, _ = self.collect(beforeTime=Time(40))
        self.assertEqual(ids, ['p3', 'p2', 'p1'])

    def test_time_window_both_bounds(self):
        # Window [afterTime=25, beforeTime=55): create_at in {30,40,50} -> p3,p4,p5.
        ids, _ = self.collect(afterTime=Time(25), beforeTime=Time(55))
        self.assertEqual(ids, ['p5', 'p4', 'p3'])

    def test_empty_time_window_requests_nothing(self):
        # afterTime >= beforeTime is an empty range -> short-circuit, no fetch.
        ids, result = self.collect(afterTime=Time(55), beforeTime=Time(25))
        self.assertEqual(ids, [])
        self.assertEqual(result, MattermostDriver.ProcessPostResult.NothingRequested)

    def test_empty_channel(self):
        driver = FakePostsDriver(self.config, [])
        out = []
        result = driver.processPosts(processor=lambda p, h: out.append(p["id"]),
                                     channel=makeChannel(messageCount=0))
        self.assertEqual(out, [])
        self.assertEqual(result, MattermostDriver.ProcessPostResult.NoMorePosts)


class AfterWinsPostsDriver(FakePostsDriver):
    '''
        Models the observed real Mattermost behavior that the shared fake does not:
        when a request carries both `after` and `before`, the server honors `after`
        and IGNORES the `before` cursor. The old code sent both during an incremental
        walk, so the cursor never advanced and the same newest page repeated forever.
        A request cap turns that regression into a fast failure instead of a hang.
    '''
    MAX_REQUESTS = 500

    def get(self, command, params=None):
        if len(self.requestLog) >= self.MAX_REQUESTS:
            raise AssertionError('processPosts did not terminate: after/before loop regressed')
        params = dict(params or {})
        if params.get('after') is not None:
            params.pop('before', None)  # server ignores the cursor when after= is present
        return super().get(command, params)


class IncrementalAfterPostRegressionTests(unittest.TestCase):
    '''Guards the fix for the infinite incremental re-fetch loop.'''
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.config = makeConfig(self._dir.name, pageSize=3)
        self.posts = [mmPost(f'p{i}', i * 10) for i in range(1, 8)]  # p1..p7

    def tearDown(self):
        self._dir.cleanup()

    def test_incremental_with_few_new_posts_terminates_without_duplicates(self):
        # data has p1..p4; channel now p1..p7. Incremental lower bound afterPost=p4.
        # Against an after-wins server this looped forever emitting p7,p6,p5 repeatedly.
        driver = AfterWinsPostsDriver(self.config, self.posts)
        out = []
        result = driver.processPosts(processor=lambda p, h: out.append(p["id"]),
                                     channel=makeChannel(messageCount=7), afterPost='p4')
        self.assertEqual(out, ['p7', 'p6', 'p5'])           # only the genuinely new posts
        self.assertEqual(len(out), len(set(out)), 'no duplicates')
        self.assertEqual(result, MattermostDriver.ProcessPostResult.ConditionReached)

    def test_resumed_incremental_with_before_and_after_terminates(self):
        # Both bounds present (resume cursor beforePost + incremental afterPost) -- the
        # exact combination that triggered the loop.
        driver = AfterWinsPostsDriver(self.config, self.posts)
        out = []
        driver.processPosts(processor=lambda p, h: out.append(p["id"]),
                            channel=makeChannel(messageCount=7),
                            beforePost='p6', afterPost='p2')
        self.assertEqual(out, ['p5', 'p4', 'p3'])  # between p6 (excl) and p2 (stop)
        self.assertEqual(len(out), len(set(out)), 'no duplicates')


class PageSizeTests(unittest.TestCase):
    def test_default_page_size_is_api_max(self):
        self.assertEqual(ConfigFile().throttlingPageSize, 200)

    def test_page_size_is_clamped_to_api_max(self):
        self.assertEqual(ConfigFile.fromJson({'throttling': {'pageSize': 5000}}).throttlingPageSize, 200)

    def test_processPosts_requests_configured_page_size(self):
        with tempfile.TemporaryDirectory() as outdir:
            config = makeConfig(outdir, pageSize=200)
            driver = FakePostsDriver(config, [mmPost(f'p{i}', i) for i in range(1, 10)])
            driver.processPosts(processor=lambda p, h: None, channel=makeChannel(messageCount=9))
            self.assertTrue(driver.requestLog)
            self.assertTrue(all(req['per_page'] == 200 for req in driver.requestLog),
                            driver.requestLog)


if __name__ == '__main__':
    unittest.main()
