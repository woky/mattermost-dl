'''
    Integration tests for the file-driven download state machine: fresh download,
    incremental append, resume of an interrupted run, the crash-safety reconcile
    and the post-resume catch-up. Uses the in-memory FakePostsDriver so the real
    processPosts / commit code runs end to end, driving the saver orchestration
    against the directory/JSON storage backend.
'''

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from mattermost_dl import progress
from mattermost_dl.config import ChannelOptions
from mattermost_dl.saver import ChannelRequest
from mattermost_dl.storage.directory_json.entities import Post
from mattermost_dl.types import Time

from .helpers import (FakePostsDriver, makeChannel, makeConfig, makeSaver,
                      mmPost, readStoredIds)

OUTFILE = 'o.team--chan'


def storedLine(id, create_at):
    '''A data/buffer line exactly as the backend would serialize a fetched post.'''
    post = Post.fromMattermost(mmPost(id, create_at))
    return json.dumps(post.toStore(), ensure_ascii=False)


class CycleTestBase(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.dir = Path(self._dir.name)
        # Full channel p1..p7 oldest -> newest, create_at 10..70.
        self.allPosts = [mmPost(f'p{i}', i * 10) for i in range(1, 8)]

    def tearDown(self):
        self._dir.cleanup()

    def saverFor(self, posts, pageSize=60):
        config = makeConfig(self.dir, pageSize=pageSize)
        driver = FakePostsDriver(config, posts)
        return makeSaver(config, driver), driver

    def paths(self, saver=None):
        return (self.dir / (OUTFILE + '.meta.json'),
                self.dir / (OUTFILE + '.data.json'),
                self.dir / (OUTFILE + '.data.json.tmp'))

    def runCycle(self, saver, channel, options=None):
        options = options if options is not None else ChannelOptions()
        archive = saver.backend.channelArchive(OUTFILE, channel, None, options, [])
        return saver._runDownloadCycle(archive, channel, options)

    def request(self, channel, options=None):
        return ChannelRequest(config=options if options is not None else ChannelOptions(),
                              metadata=channel)

    def loadStorage(self, headerFile):
        return json.loads(Path(headerFile).read_text())['storage']


class FreshDownloadTests(CycleTestBase):
    def test_fresh_writes_ascending_and_metadata(self):
        saver, driver = self.saverFor(self.allPosts, pageSize=3)
        channel = makeChannel(messageCount=7)
        header, data, tmp = self.paths()

        resumed = self.runCycle(saver, channel)

        self.assertFalse(resumed)
        self.assertEqual(readStoredIds(data), ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'])
        self.assertFalse(tmp.exists(), 'buffer must be removed on success')

        storage = self.loadStorage(header)
        self.assertNotIn('organization', storage, 'ordering concept must be gone')
        self.assertEqual(storage['count'], 7)
        self.assertEqual(storage['firstPostId'], 'p1')
        self.assertEqual(storage['lastPostId'], 'p7')
        self.assertEqual(storage['byteSize'], data.stat().st_size)
        # First and last posts are at the channel extremes -> no neighbours recorded.
        self.assertNotIn('postIdBeforeFirst', storage)
        self.assertNotIn('postIdAfterLast', storage)

    def test_max_count_keeps_newest_n(self):
        saver, driver = self.saverFor(self.allPosts, pageSize=3)
        channel = makeChannel(messageCount=7)
        header, data, tmp = self.paths()
        options = ChannelOptions()
        options.postLimit = 3  # maximumPostCount

        self.runCycle(saver, channel, options)

        # Newest 3 posts, still stored oldest -> newest.
        self.assertEqual(readStoredIds(data), ['p5', 'p6', 'p7'])

    def test_interval_download_after_and_before_time(self):
        # README "Download things in interval": fresh download bounded on both sides.
        saver, driver = self.saverFor(self.allPosts, pageSize=3)
        channel = makeChannel(messageCount=7)
        header, data, tmp = self.paths()
        options = ChannelOptions()
        options.postsAfterTime = Time(25)   # exclude p1(10), p2(20)
        options.postsBeforeTime = Time(55)  # exclude p6(60), p7(70)

        self.runCycle(saver, channel, options)

        self.assertEqual(readStoredIds(data), ['p3', 'p4', 'p5'])
        self.assertFalse(tmp.exists())


class IncrementalTests(CycleTestBase):
    def test_incremental_appends_only_new(self):
        # Baseline: commit p1..p4.
        saver1, _ = self.saverFor(self.allPosts[:4], pageSize=60)
        self.runCycle(saver1, makeChannel(messageCount=4))
        header, data, tmp = self.paths()
        self.assertEqual(readStoredIds(data), ['p1', 'p2', 'p3', 'p4'])

        # Channel now has p1..p7; incremental run.
        saver2, driver2 = self.saverFor(self.allPosts, pageSize=60)
        resumed = self.runCycle(saver2, makeChannel(messageCount=7))

        self.assertFalse(resumed)
        self.assertEqual(readStoredIds(data), ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'])
        self.assertFalse(tmp.exists())
        # The lower bound is enforced client-side: `after` is never sent to the server
        # (sending it alongside the moving `before` cursor loops it forever), yet only
        # the new posts p5..p7 are appended.
        self.assertTrue(all('after' not in req for req in driver2.requestLog),
                        driver2.requestLog)

        storage = self.loadStorage(header)
        self.assertEqual(storage['count'], 7)
        self.assertEqual(storage['firstPostId'], 'p1')
        self.assertEqual(storage['lastPostId'], 'p7')
        self.assertEqual(storage['byteSize'], data.stat().st_size)

    def test_incremental_decision_does_not_need_meta_header(self):
        # Baseline commit p1..p4, then delete the metadata header entirely.
        saver1, _ = self.saverFor(self.allPosts[:4], pageSize=60)
        self.runCycle(saver1, makeChannel(messageCount=4))
        header, data, tmp = self.paths()
        header.unlink()
        self.assertEqual(readStoredIds(data), ['p1', 'p2', 'p3', 'p4'])

        # Incremental still appends only the new posts -- the decision reads the
        # data file (local newest = p4), never the (now absent) header.
        saver2, driver2 = self.saverFor(self.allPosts, pageSize=60)
        self.runCycle(saver2, makeChannel(messageCount=7))

        self.assertEqual(readStoredIds(data), ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'])
        self.assertTrue(all('after' not in req for req in driver2.requestLog),
                        driver2.requestLog)
        self.assertTrue(header.exists(), 'a fresh header is written on commit')

    def test_incremental_no_new_posts_is_noop(self):
        saver1, _ = self.saverFor(self.allPosts, pageSize=60)
        self.runCycle(saver1, makeChannel(messageCount=7))
        header, data, tmp = self.paths()
        before = data.read_text()

        saver2, _ = self.saverFor(self.allPosts, pageSize=60)
        self.runCycle(saver2, makeChannel(messageCount=7))

        self.assertEqual(data.read_text(), before, 'no-op incremental must not change data')
        self.assertFalse(tmp.exists())
        self.assertEqual(self.loadStorage(header)['count'], 7)


class ResumeTests(CycleTestBase):
    def test_resume_first_download_does_not_refetch_buffer(self):
        # Simulate an interrupted first download: buffer holds newest posts
        # p7,p6,p5 (newest -> oldest); no committed data file yet.
        saver, driver = self.saverFor(self.allPosts, pageSize=60)
        header, data, tmp = self.paths()
        tmp.write_text(storedLine('p7', 70) + '\n' + storedLine('p6', 60) + '\n' + storedLine('p5', 50) + '\n')
        channel = makeChannel(messageCount=7)

        resumed = self.runCycle(saver, channel)

        self.assertTrue(resumed)
        self.assertEqual(readStoredIds(data), ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'])
        self.assertFalse(tmp.exists())
        # Resume seeks straight below the buffer's oldest post (p5); p5/p6/p7 are
        # taken from the buffer, never re-requested.
        self.assertTrue(all(req.get('before') == 'p5' for req in driver.requestLog))

    def test_resume_incremental_reconciles_partial_commit(self):
        # Baseline commit p1..p4.
        saver1, _ = self.saverFor(self.allPosts[:4], pageSize=60)
        self.runCycle(saver1, makeChannel(messageCount=4))
        header, data, tmp = self.paths()
        self.assertEqual(readStoredIds(data), ['p1', 'p2', 'p3', 'p4'])

        # Simulate an interrupted incremental that fetched p5,p6,p7 into the buffer
        # AND had already partially committed p5 onto the data file before crashing.
        with open(data, 'a', encoding='utf8') as f:
            f.write(storedLine('p5', 50) + '\n')
        tmp.write_text(storedLine('p7', 70) + '\n' + storedLine('p6', 60) + '\n' + storedLine('p5', 50) + '\n')

        saver2, _ = self.saverFor(self.allPosts, pageSize=60)
        resumed = self.runCycle(saver2, makeChannel(messageCount=7))

        self.assertTrue(resumed)
        # p5 appears exactly once: reconcile trimmed the partial append, commit re-added it.
        self.assertEqual(readStoredIds(data), ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'])
        self.assertFalse(tmp.exists())
        self.assertEqual(self.loadStorage(header)['count'], 7)


class ProcessChannelCatchupTests(CycleTestBase):
    def test_resume_then_catch_up_new_posts(self):
        # Interrupted first download captured only the two newest posts p7,p6,
        # but by the time we resume, p1..p7 exist. Resume commits p1..p7; the
        # catch-up incremental finds nothing newer than p7.
        saver, driver = self.saverFor(self.allPosts, pageSize=60)
        header, data, tmp = self.paths()
        tmp.write_text(storedLine('p7', 70) + '\n' + storedLine('p6', 60) + '\n')
        channel = makeChannel(messageCount=7)

        saver.processChannel(OUTFILE, self.request(channel))

        self.assertEqual(readStoredIds(data), ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'])
        self.assertFalse(tmp.exists())
        self.assertEqual(self.loadStorage(header)['count'], 7)
        # Resume's first request seeks below the buffer's oldest (p6).
        self.assertEqual(driver.requestLog[0].get('before'), 'p6')

    def test_catch_up_fetches_posts_arrived_after_interruption(self):
        # Interrupted download buffered p4,p3 (the channel only had p1..p4 then).
        # By resume time p5..p7 have arrived; the catch-up must capture them.
        saver, driver = self.saverFor(self.allPosts, pageSize=60)
        header, data, tmp = self.paths()
        tmp.write_text(storedLine('p4', 40) + '\n' + storedLine('p3', 30) + '\n')
        channel = makeChannel(messageCount=7)

        saver.processChannel(OUTFILE, self.request(channel))

        # Resume committed p1..p4, then the catch-up appended p5..p7.
        self.assertEqual(readStoredIds(data), ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'])
        self.assertFalse(tmp.exists())
        self.assertEqual(self.loadStorage(header)['count'], 7)


class ReusePolicyTests(CycleTestBase):
    def _commitBaseline(self):
        saver, _ = self.saverFor(self.allPosts[:4], pageSize=60)
        self.runCycle(saver, makeChannel(messageCount=4))
        return self.paths()

    def test_existing_archive_default_appends(self):
        header, data, tmp = self._commitBaseline()
        saver, _ = self.saverFor(self.allPosts, pageSize=60)
        channel = makeChannel(messageCount=7)
        saver.processChannel(OUTFILE, self.request(channel))
        self.assertEqual(readStoredIds(data), ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'])


class InterruptResumeTests(CycleTestBase):
    def test_interrupt_then_resume_completes_without_restart(self):
        # Interrupt after the first page (p7,p6) is fetched and buffered.
        config = makeConfig(self.dir, pageSize=2)
        driver1 = FakePostsDriver(config, self.allPosts, interruptAfterGets=1)
        saver1 = makeSaver(config, driver1)
        header, data, tmp = self.paths()
        channel = makeChannel(messageCount=7)

        with self.assertRaises(KeyboardInterrupt):
            saver1.processChannel(OUTFILE, self.request(channel))

        # Buffer persisted; nothing committed yet.
        self.assertFalse(data.exists())
        self.assertEqual(readStoredIds(tmp), ['p7', 'p6'])  # newest-first buffer

        # Resume with a clean driver: must seek below the buffer, not restart.
        driver2 = FakePostsDriver(makeConfig(self.dir, pageSize=2), self.allPosts)
        saver2 = makeSaver(driver2.configfile, driver2)
        saver2.processChannel(OUTFILE, self.request(channel))

        self.assertEqual(readStoredIds(data), ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'])
        self.assertFalse(tmp.exists())
        self.assertEqual(driver2.requestLog[0].get('before'), 'p6',
                         'resume must seek below the buffer oldest, not refetch from newest')


class ProgressUxTests(CycleTestBase):
    def runCycleCapturingStderr(self, pageSize):
        # Build the saver INSIDE the redirect so its progress manager targets the
        # captured stream (it binds sys.stderr at construction).
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            config = makeConfig(self.dir, pageSize=pageSize)
            config.progressInterval = 0  # render every update
            config.reportProgress = progress.ProgressSettings(
                mode=progress.VisualizationMode.AnsiEscapes, forceMode=True)
            saver = makeSaver(config, FakePostsDriver(config, self.allPosts))
            self.runCycle(saver, makeChannel(messageCount=7))
        return err.getvalue()

    def test_fresh_download_shows_live_progress(self):
        out = self.runCycleCapturingStderr(pageSize=3)
        self.assertIn('chan:', out)
        self.assertIn('/7 posts', out)

    def test_up_to_date_channel_shows_no_progress_line(self):
        # Commit p1..p7 quietly first (DumbTerminal config -> progress disabled).
        saver0, _ = self.saverFor(self.allPosts, pageSize=60)
        self.runCycle(saver0, makeChannel(messageCount=7))
        # Re-run as a no-op incremental with progress forced on: nothing new is
        # fetched, so the task never updates and no progress line is drawn.
        out = self.runCycleCapturingStderr(pageSize=60)
        self.assertNotIn('posts', out,
                         'an up-to-date channel must not print a frozen 0/N progress line')


if __name__ == '__main__':
    unittest.main()
