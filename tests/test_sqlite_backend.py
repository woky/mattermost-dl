'''
    Integration tests for the SQLite storage backend, driving the real saver /
    FakePostsDriver against output.format="sqlite" (the orchestration is
    backend-agnostic) and asserting directly on the produced database via a
    read-only cursor. Covers: fresh download, incremental append, no-op re-run,
    resume, post-resume catch-up, dimension dedup, thread reconstruction from
    promoted columns, inline asset BLOBs, FTS5 MATCH/bm25 and schema migrations.
'''

import sqlite3
import tempfile
import unittest
from pathlib import Path

from mattermost_dl.config import ChannelOptions
from mattermost_dl.saver import ChannelRequest
from mattermost_dl.storage.sqlite.schema import (LATEST_VERSION, MIGRATIONS,
                                                 applyMigrations)

from .helpers import (FakePostsDriver, makeChannel, makeConfig, makeSqliteSaver,
                      mmPost, seedStaging, sqliteCursor)

OUTFILE = 'o.team--chan'
CHAN = 'chan'


class SqliteTestBase(unittest.TestCase):
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
        return makeSqliteSaver(config, driver), driver

    def runCycle(self, saver, channel, options=None):
        options = options if options is not None else ChannelOptions()
        archive = saver.backend.channelArchive(OUTFILE, channel, None, options, [])
        return saver._runDownloadCycle(archive, channel, options)

    def request(self, channel, options=None):
        return ChannelRequest(config=options if options is not None else ChannelOptions(),
                              metadata=channel)

    def channelIds(self, saver, channelId=CHAN):
        with sqliteCursor(saver) as conn:
            return [r['id'] for r in conn.execute(
                'SELECT id FROM posts WHERE channel_id=? ORDER BY create_at',
                (channelId,)).fetchall()]

    def scalar(self, saver, sql, params=()):
        with sqliteCursor(saver) as conn:
            return conn.execute(sql, params).fetchone()[0]


class FreshDownloadTests(SqliteTestBase):
    def test_fresh_writes_all_posts(self):
        saver, _ = self.saverFor(self.allPosts, pageSize=3)
        resumed = self.runCycle(saver, makeChannel(id=CHAN, messageCount=7))

        self.assertFalse(resumed)
        self.assertEqual(self.channelIds(saver),
                         ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'])
        # Staging buffer drained on a successful commit.
        self.assertEqual(self.scalar(saver, 'SELECT count(*) FROM posts_staging'), 0)

    def test_fresh_populates_dimensions(self):
        saver, _ = self.saverFor(self.allPosts, pageSize=3)
        self.runCycle(saver, makeChannel(id=CHAN, messageCount=7))

        # The channel row and one author row (every post is by u1) are present,
        # and the FK from posts.user_id is satisfiable.
        self.assertEqual(self.scalar(saver, 'SELECT count(*) FROM channels'), 1)
        self.assertEqual(self.scalar(saver, "SELECT username FROM users WHERE id='u1'"),
                         'user-u1')

    def test_fresh_post_carries_archive_channel_id(self):
        # channel_id comes from the archive's channel, never the raw post.
        saver, _ = self.saverFor([mmPost('p1', 10)], pageSize=60)
        self.runCycle(saver, makeChannel(id=CHAN, messageCount=1))
        self.assertEqual(self.scalar(saver, "SELECT channel_id FROM posts WHERE id='p1'"),
                         CHAN)


class IncrementalTests(SqliteTestBase):
    def test_incremental_appends_only_new(self):
        saver1, _ = self.saverFor(self.allPosts[:4])
        self.runCycle(saver1, makeChannel(id=CHAN, messageCount=4))
        self.assertEqual(self.channelIds(saver1), ['p1', 'p2', 'p3', 'p4'])

        saver2, driver2 = self.saverFor(self.allPosts)
        resumed = self.runCycle(saver2, makeChannel(id=CHAN, messageCount=7))

        self.assertFalse(resumed)
        self.assertEqual(self.channelIds(saver2),
                         ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'])
        self.assertEqual(self.scalar(saver2, 'SELECT count(*) FROM posts'), 7)

    def test_norun_rerun_does_not_duplicate(self):
        saver1, _ = self.saverFor(self.allPosts)
        self.runCycle(saver1, makeChannel(id=CHAN, messageCount=7))

        saver2, _ = self.saverFor(self.allPosts)
        self.runCycle(saver2, makeChannel(id=CHAN, messageCount=7))

        self.assertEqual(self.scalar(saver2, 'SELECT count(*) FROM posts'), 7)
        self.assertEqual(self.scalar(saver2, 'SELECT count(*) FROM posts_staging'), 0)


class ResumeTests(SqliteTestBase):
    def test_resume_completes_from_staging(self):
        # Interrupted first download: staging holds the newest p5,p6,p7; no posts
        # committed yet.
        saver, driver = self.saverFor(self.allPosts)
        seedStaging(saver, CHAN, self.allPosts[4:7])

        resumed = self.runCycle(saver, makeChannel(id=CHAN, messageCount=7))

        self.assertTrue(resumed)
        self.assertEqual(self.channelIds(saver),
                         ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'])
        self.assertEqual(self.scalar(saver, 'SELECT count(*) FROM posts_staging'), 0)
        # Resume seeks straight below the buffer's oldest post (p5); p5..p7 are
        # taken from staging, never re-requested.
        self.assertTrue(all(req.get('before') == 'p5' for req in driver.requestLog),
                        driver.requestLog)

    def test_catch_up_after_resume(self):
        # Interrupted first download captured only p6,p7; by resume time p1..p7
        # exist. processChannel resumes then runs one catch-up incremental.
        saver, _ = self.saverFor(self.allPosts)
        seedStaging(saver, CHAN, self.allPosts[5:7])

        saver.processChannel(OUTFILE, self.request(makeChannel(id=CHAN, messageCount=7)))

        self.assertEqual(self.channelIds(saver),
                         ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'])
        self.assertEqual(self.scalar(saver, 'SELECT count(*) FROM posts_staging'), 0)


class DedupTests(SqliteTestBase):
    def test_same_user_across_channels_deduped(self):
        # Two channels, each with a post by the same author u1.
        saver, _ = self.saverFor([mmPost('a1', 10, user_id='u1')])
        self.runCycle(saver, makeChannel(id='chanA', messageCount=1))

        saver2, _ = self.saverFor([mmPost('b1', 20, user_id='u1')])
        self.runCycle(saver2, makeChannel(id='chanB', messageCount=1))

        self.assertEqual(self.scalar(saver2, "SELECT count(*) FROM users WHERE id='u1'"), 1)
        self.assertEqual(self.scalar(saver2, 'SELECT count(*) FROM channels'), 2)

    def test_emoji_reused_across_posts_deduped(self):
        emoji = {'id': 'e1', 'name': 'party', 'creator_id': 'u3', 'create_at': 8}
        posts = [
            mmPost('p1', 10, metadata={'emojis': [emoji]}),
            mmPost('p2', 20, metadata={'emojis': [emoji]}),
        ]
        options = ChannelOptions()
        options.emojiMetadata = True
        saver, _ = self.saverFor(posts)
        self.runCycle(saver, makeChannel(id=CHAN, messageCount=2), options)

        self.assertEqual(self.scalar(saver, "SELECT count(*) FROM emojis WHERE id='e1'"), 1)


class ThreadTests(SqliteTestBase):
    def threadPosts(self):
        # root + two replies + an unrelated standalone post.
        return [
            mmPost('root', 10, message='root msg'),
            mmPost('r1', 20, message='reply one', root_id='root'),
            mmPost('r2', 30, message='reply two', root_id='root'),
            mmPost('other', 40, message='unrelated'),
        ]

    def threadInfo(self, saver):
        '''post_id -> (thread_index, thread_size), via the consumer join.'''
        with sqliteCursor(saver) as conn:
            return {r['id']: (r['thread_index'], r['size']) for r in conn.execute(
                'SELECT p.id, pt.thread_index, t.size '
                'FROM posts p '
                'JOIN post_threads pt ON pt.post_id = p.id '
                'JOIN threads t ON t.root_id = pt.thread_root').fetchall()}

    def test_thread_reconstructed_from_columns(self):
        saver, _ = self.saverFor(self.threadPosts())
        self.runCycle(saver, makeChannel(id=CHAN, messageCount=4))

        # A flat thread is the root plus every post sharing its root_id, ordered by
        # time -- using promoted columns only, never `raw`.
        with sqliteCursor(saver) as conn:
            ids = [r['id'] for r in conn.execute(
                'SELECT id FROM posts WHERE id=:root OR root_id=:root ORDER BY create_at',
                {'root': 'root'}).fetchall()]
        self.assertEqual(ids, ['root', 'r1', 'r2'])

    def test_thread_index_and_size_precomputed(self):
        # Every post knows its 1-based position and its thread's total -- the "3/34".
        saver, _ = self.saverFor(self.threadPosts())
        self.runCycle(saver, makeChannel(id=CHAN, messageCount=4))
        self.assertEqual(self.threadInfo(saver), {
            'root': (1, 3), 'r1': (2, 3), 'r2': (3, 3), 'other': (1, 1)})

    def test_thread_size_grows_incrementally(self):
        posts = self.threadPosts()
        saver1, _ = self.saverFor(posts[:2])  # root + r1
        self.runCycle(saver1, makeChannel(id=CHAN, messageCount=2))
        self.assertEqual(self.threadInfo(saver1), {'root': (1, 2), 'r1': (2, 2)})

        saver2, _ = self.saverFor(posts[:3])  # a later run adds the newer r2
        self.runCycle(saver2, makeChannel(id=CHAN, messageCount=3))
        # r2 appends as the thread's newest member; root/r1 keep their positions.
        self.assertEqual(self.threadInfo(saver2),
                         {'root': (1, 3), 'r1': (2, 3), 'r2': (3, 3)})

    def test_thread_for_reply_without_archived_root(self):
        # Only the replies are archived (the root post itself is absent); they still
        # form a thread keyed by their shared root_id.
        posts = [
            mmPost('r1', 20, message='reply one', root_id='root'),
            mmPost('r2', 30, message='reply two', root_id='root'),
        ]
        saver, _ = self.saverFor(posts)
        self.runCycle(saver, makeChannel(id=CHAN, messageCount=2))
        self.assertEqual(
            self.scalar(saver, "SELECT size FROM threads WHERE root_id='root'"), 2)
        self.assertEqual(self.threadInfo(saver), {'r1': (1, 2), 'r2': (2, 2)})

    def test_recommit_does_not_double_count(self):
        # Re-committing already-stored posts takes the upsert's UPDATE path (the AU
        # trigger, whose WHEN guard is false), so thread sizes must not grow.
        posts = self.threadPosts()
        saver, _ = self.saverFor(posts)
        channel = makeChannel(id=CHAN, messageCount=4)
        self.runCycle(saver, channel)

        archive = saver.backend.channelArchive(OUTFILE, channel, None, ChannelOptions(), [])
        seedStaging(saver, CHAN, posts)
        archive.commit(incremental=True, localNewestId='other')

        self.assertEqual(self.threadInfo(saver), {
            'root': (1, 3), 'r1': (2, 3), 'r2': (3, 3), 'other': (1, 1)})


class AssetTests(SqliteTestBase):
    def assetPost(self):
        return mmPost('p1', 10, user_id='u1', metadata={
            'files': [
                {'id': 'f1', 'name': 'a.txt', 'size': 7, 'mime_type': 'text/plain',
                 'create_at': 9},
                {'id': 'f2', 'name': 'big.bin', 'size': 999,
                 'mime_type': 'application/octet-stream', 'create_at': 9},
            ],
            'emojis': [{'id': 'e1', 'name': 'party', 'creator_id': 'u3', 'create_at': 8}],
        })

    def assetOptions(self):
        options = ChannelOptions()
        options.downloadAttachments = True
        options.downloadAttachmentSizeLimit = 100  # excludes the 999-byte f2
        options.downloadEmoji = True
        options.downloadAvatars = True
        return options

    def test_blobs_stored_and_filtered(self):
        saver, _ = self.saverFor([self.assetPost()])
        self.runCycle(saver, makeChannel(id=CHAN, messageCount=1), self.assetOptions())

        with sqliteCursor(saver) as conn:
            # Both attachments get a metadata row; only the one passing the filters
            # carries content bytes.
            self.assertEqual(conn.execute('SELECT count(*) FROM attachments').fetchone()[0], 2)
            self.assertIsNotNone(conn.execute(
                "SELECT content FROM attachments WHERE id='f1'").fetchone()[0])
            self.assertIsNone(conn.execute(
                "SELECT content FROM attachments WHERE id='f2'").fetchone()[0])
            # Emoji image and avatar BLOBs are filled.
            self.assertIsNotNone(conn.execute(
                "SELECT image FROM emojis WHERE id='e1'").fetchone()[0])
            self.assertIsNotNone(conn.execute(
                "SELECT avatar FROM users WHERE id='u1'").fetchone()[0])

    def test_rerun_skips_present_blobs(self):
        post, options = self.assetPost(), self.assetOptions()
        saver, driver = self.saverFor([post])
        channel = makeChannel(id=CHAN, messageCount=1)
        self.runCycle(saver, channel, options)
        fetchedAfterFirst = len(driver.fetchedUrls)
        self.assertGreater(fetchedAfterFirst, 0)

        # Re-process the very same post: every referenced blob is already present,
        # so nothing is re-fetched (idempotent, resumable asset download).
        archive = saver.backend.channelArchive(OUTFILE, channel, None, options, [])
        seedStaging(saver, CHAN, [post])
        archive.commit(incremental=True, localNewestId='p1')
        self.assertEqual(len(driver.fetchedUrls), fetchedAfterFirst)


class FtsTests(SqliteTestBase):
    def matchIds(self, saver, query):
        with sqliteCursor(saver) as conn:
            return [r['id'] for r in conn.execute(
                'SELECT p.id FROM posts_fts JOIN posts p ON p.rowid = posts_fts.rowid '
                'WHERE posts_fts MATCH ? ORDER BY bm25(posts_fts)', (query,)).fetchall()]

    def test_match_finds_distinctive_message(self):
        posts = [
            mmPost('p1', 10, message='ordinary chatter about lunch'),
            mmPost('p2', 20, message='the quux deployment rolled back cleanly'),
        ]
        saver, _ = self.saverFor(posts)
        self.runCycle(saver, makeChannel(id=CHAN, messageCount=2))
        self.assertEqual(self.matchIds(saver, 'quux'), ['p2'])

    def test_bm25_orders_matches(self):
        posts = [
            mmPost('weak', 10, message='deploy happened once'),
            mmPost('strong', 20, message='deploy deploy deploy everywhere'),
        ]
        saver, _ = self.saverFor(posts)
        self.runCycle(saver, makeChannel(id=CHAN, messageCount=2))
        # bm25 ranks the denser match first (ascending bm25 = best first).
        self.assertEqual(self.matchIds(saver, 'deploy'), ['strong', 'weak'])

    def test_fts_stays_in_sync_on_reupsert(self):
        # A no-op re-run upserts the same posts (AFTER UPDATE trigger); the FTS
        # index must not accumulate duplicate matches.
        posts = [mmPost('p1', 10, message='unique sentinel token')]
        saver1, _ = self.saverFor(posts)
        self.runCycle(saver1, makeChannel(id=CHAN, messageCount=1))
        saver2, _ = self.saverFor(posts)
        self.runCycle(saver2, makeChannel(id=CHAN, messageCount=1))
        self.assertEqual(self.matchIds(saver2, 'sentinel'), ['p1'])


class ConcurrencyTests(SqliteTestBase):
    def test_parallel_channels_share_one_connection_safely(self):
        # Several channels downloaded concurrently must all land correctly: the
        # one connection (check_same_thread=False) is guarded by a single DB lock,
        # so the parallel network fetches converge onto serialized writes. Each
        # channel has its own globally-unique post ids, as in real Mattermost.
        names = ['chan0', 'chan1', 'chan2', 'chan3']
        postsByChannel = {
            n: [mmPost(f'{n}-p{i}', i * 10) for i in range(1, 8)] for n in names}
        config = makeConfig(self.dir, pageSize=2)
        config.throttlingMaxConcurrency = 4
        driver = FakePostsDriver(config, [], postsByChannel=postsByChannel)
        saver = makeSqliteSaver(config, driver)

        tasks = [
            (lambda n=n: saver.processChannel(
                f'o.t--{n}', self.request(makeChannel(id=n, messageCount=7))))
            for n in names
        ]
        with saver.backend:
            saver._processChannels(tasks)

        for n in names:
            self.assertEqual(self.channelIds(saver, n),
                             [f'{n}-p{i}' for i in range(1, 8)])
        self.assertEqual(self.scalar(saver, 'SELECT count(*) FROM posts'), 7 * len(names))
        self.assertEqual(self.scalar(saver, 'SELECT count(*) FROM posts_staging'), 0)


class MigrationTests(SqliteTestBase):
    def test_user_version_at_latest_after_open(self):
        saver, _ = self.saverFor(self.allPosts)
        saver.backend.open()
        self.assertEqual(self.scalar(saver, 'PRAGMA user_version'), LATEST_VERSION)

    def test_reopen_is_idempotent(self):
        saver, _ = self.saverFor(self.allPosts)
        self.runCycle(saver, makeChannel(id=CHAN, messageCount=7))
        saver.backend.close()

        # A brand-new backend over the existing db neither re-creates nor bumps.
        saver2, _ = self.saverFor(self.allPosts)
        saver2.backend.open()
        self.assertEqual(self.scalar(saver2, 'PRAGMA user_version'), LATEST_VERSION)
        self.assertEqual(self.scalar(saver2, 'SELECT count(*) FROM posts'), 7)

    def test_v3_backfills_thread_tables_for_existing_posts(self):
        # An older archive (schema v2, no thread tables) with posts already stored: the
        # v3 migration must create and backfill threads/post_threads from those posts.
        conn = sqlite3.connect(':memory:')
        conn.row_factory = sqlite3.Row
        for ver, sql in MIGRATIONS:
            if ver > 2:
                break
            conn.executescript(f'BEGIN;\n{sql}\nPRAGMA user_version = {ver};\nCOMMIT;')
        # Inserted out of create_at order to prove the backfill ranks by create_at,id.
        conn.executemany(
            'INSERT INTO posts(id, channel_id, root_id, message, create_at, raw) '
            'VALUES (?, ?, ?, ?, ?, ?)',
            [('r2', 'c', 'root', 'm', 30, '{}'),
             ('root', 'c', None, 'm', 10, '{}'),
             ('r1', 'c', 'root', 'm', 20, '{}'),
             ('other', 'c', None, 'm', 40, '{}')])
        conn.commit()

        applyMigrations(conn)  # runs v3: creates the tables, then backfills

        self.assertEqual(conn.execute('PRAGMA user_version').fetchone()[0], LATEST_VERSION)
        self.assertEqual(dict(conn.execute('SELECT root_id, size FROM threads').fetchall()),
                         {'root': 3, 'other': 1})
        self.assertEqual(
            {r['post_id']: (r['thread_root'], r['thread_index'])
             for r in conn.execute('SELECT * FROM post_threads').fetchall()},
            {'root': ('root', 1), 'r1': ('root', 2), 'r2': ('root', 3),
             'other': ('other', 1)})
        conn.close()


if __name__ == '__main__':
    unittest.main()
