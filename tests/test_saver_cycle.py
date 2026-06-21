'''
    Integration tests for the file-driven download state machine in saver.py:
    fresh download, incremental append, resume of an interrupted run, the
    crash-safety reconcile and the post-resume catch-up. Uses the in-memory
    FakePostsDriver so the real processPosts / commit code runs end to end.
'''

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from mattermost_dl import progress
from mattermost_dl.bo import Post, Time
from mattermost_dl.config import ChannelOptions
from mattermost_dl.recovery import RBackup, RDelete, RSkipDownload
from mattermost_dl.saver import ChannelRequest
from mattermost_dl.store import ChannelHeader

from .helpers import (FakePostsDriver, makeChannel, makeConfig, makeSaver,
                      mmPost, readStoredIds)

OUTFILE = 'o.team--chan'


def storedLine(id, create_at):
    '''A data/buffer line exactly as the saver would serialize a fetched post.'''
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

    def paths(self, saver):
        header, data = saver.makeArchiveFilenames(OUTFILE)
        return header, data, saver.makeBufferFilename(OUTFILE)

    def runCycle(self, saver, channel):
        header, data, tmp = self.paths(saver)
        template = ChannelHeader(channel=channel)
        return saver._runDownloadCycle(channel, ChannelOptions(), OUTFILE, template, header, data, tmp)

    def loadStorage(self, headerFile):
        return json.loads(Path(headerFile).read_text())['storage']


class FreshDownloadTests(CycleTestBase):
    def test_fresh_writes_ascending_and_metadata(self):
        saver, driver = self.saverFor(self.allPosts, pageSize=3)
        channel = makeChannel(messageCount=7)
        header, data, tmp = self.paths(saver)

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
        config = makeConfig(self.dir, pageSize=3)
        driver = FakePostsDriver(config, self.allPosts)
        saver = makeSaver(config, driver)
        channel = makeChannel(messageCount=7)
        header, data, tmp = self.paths(saver)
        options = ChannelOptions()
        options.postLimit = 3  # maximumPostCount
        template = ChannelHeader(channel=channel)

        saver._runDownloadCycle(channel, options, OUTFILE, template, header, data, tmp)

        # Newest 3 posts, still stored oldest -> newest.
        self.assertEqual(readStoredIds(data), ['p5', 'p6', 'p7'])


    def test_interval_download_after_and_before_time(self):
        # README "Download things in interval": fresh download bounded on both sides.
        config = makeConfig(self.dir, pageSize=3)
        driver = FakePostsDriver(config, self.allPosts)
        saver = makeSaver(config, driver)
        channel = makeChannel(messageCount=7)
        header, data, tmp = self.paths(saver)
        options = ChannelOptions()
        options.postsAfterTime = Time(25)   # exclude p1(10), p2(20)
        options.postsBeforeTime = Time(55)  # exclude p6(60), p7(70)
        template = ChannelHeader(channel=channel)

        saver._runDownloadCycle(channel, options, OUTFILE, template, header, data, tmp)

        self.assertEqual(readStoredIds(data), ['p3', 'p4', 'p5'])
        self.assertFalse(tmp.exists())


class IncrementalTests(CycleTestBase):
    def test_incremental_appends_only_new(self):
        # Baseline: commit p1..p4.
        saver1, _ = self.saverFor(self.allPosts[:4], pageSize=60)
        channel = makeChannel(messageCount=4)
        self.runCycle(saver1, channel)
        header, data, tmp = self.paths(saver1)
        self.assertEqual(readStoredIds(data), ['p1', 'p2', 'p3', 'p4'])

        # Channel now has p1..p7; incremental run.
        saver2, driver2 = self.saverFor(self.allPosts, pageSize=60)
        channel = makeChannel(messageCount=7)
        resumed = self.runCycle(saver2, channel)

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
        channel = makeChannel(messageCount=4)
        self.runCycle(saver1, channel)
        header, data, tmp = self.paths(saver1)
        header.unlink()
        self.assertEqual(readStoredIds(data), ['p1', 'p2', 'p3', 'p4'])

        # Incremental still appends only the new posts -- the decision reads the
        # data file (local newest = p4), never the (now absent) header.
        saver2, driver2 = self.saverFor(self.allPosts, pageSize=60)
        channel = makeChannel(messageCount=7)
        self.runCycle(saver2, channel)

        self.assertEqual(readStoredIds(data), ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'])
        self.assertTrue(all('after' not in req for req in driver2.requestLog),
                        driver2.requestLog)
        self.assertTrue(header.exists(), 'a fresh header is written on commit')

    def test_incremental_no_new_posts_is_noop(self):
        saver1, _ = self.saverFor(self.allPosts, pageSize=60)
        channel = makeChannel(messageCount=7)
        self.runCycle(saver1, channel)
        header, data, tmp = self.paths(saver1)
        before = data.read_text()

        saver2, _ = self.saverFor(self.allPosts, pageSize=60)
        self.runCycle(saver2, channel)

        self.assertEqual(data.read_text(), before, 'no-op incremental must not change data')
        self.assertFalse(tmp.exists())
        self.assertEqual(self.loadStorage(header)['count'], 7)


class ResumeTests(CycleTestBase):
    def test_resume_first_download_does_not_refetch_buffer(self):
        # Simulate an interrupted first download: buffer holds newest posts
        # p7,p6,p5 (newest -> oldest); no committed data file yet.
        saver, driver = self.saverFor(self.allPosts, pageSize=60)
        header, data, tmp = self.paths(saver)
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
        channel4 = makeChannel(messageCount=4)
        self.runCycle(saver1, channel4)
        header, data, tmp = self.paths(saver1)
        self.assertEqual(readStoredIds(data), ['p1', 'p2', 'p3', 'p4'])

        # Simulate an interrupted incremental that fetched p5,p6,p7 into the buffer
        # AND had already partially committed p5 onto the data file before crashing.
        with open(data, 'a', encoding='utf8') as f:
            f.write(storedLine('p5', 50) + '\n')
        tmp.write_text(storedLine('p7', 70) + '\n' + storedLine('p6', 60) + '\n' + storedLine('p5', 50) + '\n')

        saver2, _ = self.saverFor(self.allPosts, pageSize=60)
        channel7 = makeChannel(messageCount=7)
        resumed = self.runCycle(saver2, channel7)

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
        header, data, tmp = self.paths(saver)
        tmp.write_text(storedLine('p7', 70) + '\n' + storedLine('p6', 60) + '\n')
        channel = makeChannel(messageCount=7)
        request = ChannelRequest(config=ChannelOptions(), metadata=channel)
        template = ChannelHeader(channel=channel)

        saver.processChannel(OUTFILE, template, request)

        self.assertEqual(readStoredIds(data), ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'])
        self.assertFalse(tmp.exists())
        self.assertEqual(self.loadStorage(header)['count'], 7)
        # Resume's first request seeks below the buffer's oldest (p6).
        self.assertEqual(driver.requestLog[0].get('before'), 'p6')

    def test_catch_up_fetches_posts_arrived_after_interruption(self):
        # Interrupted download buffered p4,p3 (the channel only had p1..p4 then).
        # By resume time p5..p7 have arrived; the catch-up must capture them.
        saver, driver = self.saverFor(self.allPosts, pageSize=60)
        header, data, tmp = self.paths(saver)
        tmp.write_text(storedLine('p4', 40) + '\n' + storedLine('p3', 30) + '\n')
        channel = makeChannel(messageCount=7)
        request = ChannelRequest(config=ChannelOptions(), metadata=channel)
        template = ChannelHeader(channel=channel)

        saver.processChannel(OUTFILE, template, request)

        # Resume committed p1..p4, then the catch-up appended p5..p7.
        self.assertEqual(readStoredIds(data), ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'])
        self.assertFalse(tmp.exists())
        self.assertEqual(self.loadStorage(header)['count'], 7)


class ReusePolicyTests(CycleTestBase):
    def _commitBaseline(self):
        saver, _ = self.saverFor(self.allPosts[:4], pageSize=60)
        self.runCycle(saver, makeChannel(messageCount=4))
        return self.paths(saver)

    def test_existing_archive_default_appends(self):
        header, data, tmp = self._commitBaseline()
        saver, _ = self.saverFor(self.allPosts, pageSize=60)
        channel = makeChannel(messageCount=7)
        saver.processChannel(OUTFILE, ChannelHeader(channel=channel),
                             ChannelRequest(config=ChannelOptions(), metadata=channel))
        self.assertEqual(readStoredIds(data), ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'])

    def test_existing_archive_skip_policy_leaves_it(self):
        header, data, tmp = self._commitBaseline()
        saver, driver = self.saverFor(self.allPosts, pageSize=60)
        channel = makeChannel(messageCount=7)
        options = ChannelOptions()
        options.onExistingCompatibleArchive = RSkipDownload()
        saver.processChannel(OUTFILE, ChannelHeader(channel=channel),
                             ChannelRequest(config=options, metadata=channel))
        self.assertEqual(readStoredIds(data), ['p1', 'p2', 'p3', 'p4'])
        self.assertEqual(driver.requestLog, [], 'skip policy must not fetch anything')

    def test_existing_archive_delete_policy_redownloads(self):
        header, data, tmp = self._commitBaseline()
        saver, _ = self.saverFor(self.allPosts, pageSize=60)
        channel = makeChannel(messageCount=7)
        options = ChannelOptions()
        options.onExistingCompatibleArchive = RDelete()
        saver.processChannel(OUTFILE, ChannelHeader(channel=channel),
                             ChannelRequest(config=options, metadata=channel))
        # Deleted then redownloaded from scratch -> full channel, single copy.
        self.assertEqual(readStoredIds(data), ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'])
        self.assertFalse(tmp.exists())

    def test_existing_archive_backup_policy_preserves_old(self):
        header, data, tmp = self._commitBaseline()
        saver, _ = self.saverFor(self.allPosts, pageSize=60)
        channel = makeChannel(messageCount=7)
        options = ChannelOptions()
        options.onExistingCompatibleArchive = RBackup()
        saver.processChannel(OUTFILE, ChannelHeader(channel=channel),
                             ChannelRequest(config=options, metadata=channel))
        self.assertEqual(readStoredIds(data), ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'])
        backupData = self.dir / (OUTFILE + '--backup.data.json')
        self.assertTrue(backupData.exists(), 'previous archive should be backed up')
        self.assertEqual(readStoredIds(backupData), ['p1', 'p2', 'p3', 'p4'])


class InterruptResumeTests(CycleTestBase):
    def test_interrupt_then_resume_completes_without_restart(self):
        # Interrupt after the first page (p7,p6) is fetched and buffered.
        config = makeConfig(self.dir, pageSize=2)
        driver1 = FakePostsDriver(config, self.allPosts, interruptAfterGets=1)
        saver1 = makeSaver(config, driver1)
        header, data, tmp = self.paths(saver1)
        channel = makeChannel(messageCount=7)
        request = ChannelRequest(config=ChannelOptions(), metadata=channel)

        with self.assertRaises(KeyboardInterrupt):
            saver1.processChannel(OUTFILE, ChannelHeader(channel=channel), request)

        # Buffer persisted; nothing committed yet.
        self.assertFalse(data.exists())
        self.assertEqual(readStoredIds(tmp), ['p7', 'p6'])  # newest-first buffer

        # Resume with a clean driver: must seek below the buffer, not restart.
        driver2 = FakePostsDriver(makeConfig(self.dir, pageSize=2), self.allPosts)
        saver2 = makeSaver(driver2.configfile, driver2)
        saver2.processChannel(OUTFILE, ChannelHeader(channel=channel),
                              ChannelRequest(config=ChannelOptions(), metadata=channel))

        self.assertEqual(readStoredIds(data), ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'])
        self.assertFalse(tmp.exists())
        self.assertEqual(driver2.requestLog[0].get('before'), 'p6',
                         'resume must seek below the buffer oldest, not refetch from newest')


class ProgressUxTests(CycleTestBase):
    def ansiSaver(self, posts, pageSize=60):
        config = makeConfig(self.dir, pageSize=pageSize)
        # Force a real (visible) progress reporter regardless of tty.
        config.reportProgress = progress.ProgressSettings(
            mode=progress.VisualizationMode.AnsiEscapes, forceMode=True)
        driver = FakePostsDriver(config, posts)
        return makeSaver(config, driver)

    def test_fresh_download_shows_progress(self):
        saver = self.ansiSaver(self.allPosts, pageSize=3)
        channel = makeChannel(messageCount=7)
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            self.runCycle(saver, channel)
        self.assertIn('Progress:', err.getvalue())

    def test_up_to_date_channel_shows_no_progress_line(self):
        # Commit p1..p7 quietly (DumbTerminal config).
        saver0, _ = self.saverFor(self.allPosts, pageSize=60)
        channel = makeChannel(messageCount=7)
        self.runCycle(saver0, channel)

        # Re-run as a no-op incremental with the progress reporter forced on.
        saver = self.ansiSaver(self.allPosts, pageSize=60)
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            self.runCycle(saver, channel)
        self.assertNotIn('Progress:', err.getvalue(),
                         'an up-to-date channel must not print a frozen 0/N progress line')


if __name__ == '__main__':
    unittest.main()
