'''
    Tests for the newest->oldest MattermostDriver.processPosts walk: pagination,
    the server-side before/after cursors and the maxCount / afterTime / beforeTime
    stop conditions.
'''

import tempfile
import unittest

from mattermost_dl.bo import Time
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
        result = self.driver.processPosts(processor=lambda p, h: out.append(p.id),
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

    def test_after_post_server_side(self):
        # after=p4 -> only posts newer than p4.
        ids, _ = self.collect(afterPost='p4')
        self.assertEqual(ids, ['p7', 'p6', 'p5'])

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
        result = driver.processPosts(processor=lambda p, h: out.append(p.id),
                                     channel=makeChannel(messageCount=0))
        self.assertEqual(out, [])
        self.assertEqual(result, MattermostDriver.ProcessPostResult.NoMorePosts)


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
