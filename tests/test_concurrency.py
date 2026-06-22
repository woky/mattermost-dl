'''
    Tests for parallel downloading: the AIMD concurrency governor in the driver,
    its wiring into getRaw, parallel file downloads (Phase A), parallel channel
    downloads producing identical output (Phase B), and the cooperative stop that
    keeps a Ctrl-C resumable when channels run in a thread pool.
'''

import contextlib
import io
import logging
import tempfile
import threading
import time
import unittest
from pathlib import Path

from requests.exceptions import Timeout

from mattermost_dl import progress
from mattermost_dl.config import ChannelOptions, ConfigFile
from mattermost_dl.driver import AdaptiveConcurrency, MattermostDriver
from mattermost_dl.saver import ChannelRequest

from .helpers import (FakePostsDriver, makeChannel, makeConfig, makeSaver,
                      mmPost, readStoredIds)


class FakeClock:
    '''A clock the governor reads, so failure-window timing is deterministic.'''
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


class AdaptiveConcurrencyTests(unittest.TestCase):
    '''Unit tests of the AIMD logic, driven through a fake clock (no sleeps).'''
    def make(self, ceiling=8):
        self.clock = FakeClock()
        return AdaptiveConcurrency(ceiling, clock=self.clock)

    def test_single_failure_does_not_shrink_or_pause(self):
        g = self.make(8)
        g.onTransientFailure(1.0)
        self.assertEqual(g.live, 8, 'a lone flake must not cut concurrency')
        self.assertEqual(g.pauseUntil, 0.0, 'a lone flake must not pause peers')

    def test_clustered_failures_shrink_and_pause(self):
        g = self.make(8)
        g.onTransientFailure(2.0)
        g.onTransientFailure(2.0)  # second within the window -> a cluster
        self.assertEqual(g.live, 4, 'multiplicative decrease halves live')
        self.assertEqual(g.pauseUntil, self.clock.t + 2.0, 'peers told to back off')

    def test_simultaneous_burst_coalesced_into_one_cut(self):
        g = self.make(8)
        for _ in range(5):  # five concurrently-failing requests, same instant
            g.onTransientFailure(1.0)
        self.assertEqual(g.live, 4, 'a burst halves live once, not once per failure')

    def test_separate_clusters_each_shrink(self):
        g = self.make(8)
        g.onTransientFailure(1.0)
        g.onTransientFailure(1.0)
        self.assertEqual(g.live, 4)
        self.clock.advance(g.DECREASE_COOLDOWN + 0.1)
        g.onTransientFailure(1.0)  # still within the failure window -> cluster again
        self.assertEqual(g.live, 2)

    def test_failures_age_out_of_window(self):
        g = self.make(8)
        g.onTransientFailure(1.0)
        self.clock.advance(g.FAILURE_WINDOW + 1)
        g.onTransientFailure(1.0)  # the first has aged out, so this is lone again
        self.assertEqual(g.live, 8)

    def test_grows_back_after_sustained_success(self):
        g = self.make(8)
        g.onTransientFailure(1.0)
        g.onTransientFailure(1.0)
        self.assertEqual(g.live, 4)
        for _ in range(g.GROWTH_THRESHOLD):
            g.onSuccess()
        self.assertEqual(g.live, 5, 'additive increase reclaims one permit')

    def test_success_streak_reset_by_failure(self):
        g = self.make(8)
        g.live = 4
        for _ in range(g.GROWTH_THRESHOLD - 1):
            g.onSuccess()
        g.onTransientFailure(1.0)  # lone failure: no shrink, but resets the streak
        self.assertEqual(g.live, 4)
        g.onSuccess()
        self.assertEqual(g.live, 4, 'growth needs a fresh uninterrupted success run')

    def test_never_below_floor(self):
        g = self.make(2)
        for _ in range(10):
            g.onTransientFailure(1.0)
            self.clock.advance(g.DECREASE_COOLDOWN + 0.1)
        self.assertEqual(g.live, 1)

    def test_never_above_ceiling(self):
        g = self.make(3)
        for _ in range(50):
            g.onSuccess()
        self.assertEqual(g.live, 3)

    def test_acquire_caps_in_flight(self):
        g = AdaptiveConcurrency(2)  # real clock; no failures involved
        g.acquire()
        g.acquire()
        self.assertEqual(g.inFlight, 2)
        proceeded = threading.Event()

        def worker():
            g.acquire()
            proceeded.set()
            g.release()

        t = threading.Thread(target=worker)
        t.start()
        self.assertFalse(proceeded.wait(0.2), 'a third acquire must block at capacity')
        g.release()  # free one permit
        self.assertTrue(proceeded.wait(1.0), 'blocked acquire proceeds once a permit frees')
        g.release()
        t.join(1.0)

    def test_shared_pause_blocks_acquire(self):
        g = AdaptiveConcurrency(4)  # real clock
        with g._cond:
            g.pauseUntil = time.monotonic() + 0.3
        proceeded = threading.Event()

        def worker():
            g.acquire()
            proceeded.set()
            g.release()

        t = threading.Thread(target=worker)
        t.start()
        self.assertFalse(proceeded.wait(0.15), 'acquire must wait out the shared cooldown')
        self.assertTrue(proceeded.wait(1.0), 'acquire proceeds after the cooldown elapses')
        t.join(1.0)


class TokenBucketTests(unittest.TestCase):
    '''The shared rate limiter inside the governor (deterministic via a fake clock).'''
    def make(self, rate, ceiling=4):
        self.clock = FakeClock()
        return AdaptiveConcurrency(ceiling, rate=rate, clock=self.clock)

    def test_starts_full_allowing_a_burst_then_throttles(self):
        g = self.make(rate=2)  # capacity = max(1, rate) = 2
        now = self.clock.t
        self.assertEqual(g._tokenWaitLocked(now), 0.0)
        g._consumeTokenLocked()
        self.assertEqual(g._tokenWaitLocked(now), 0.0)
        g._consumeTokenLocked()
        # Bucket empty: the next token arrives in 1/rate seconds.
        self.assertAlmostEqual(g._tokenWaitLocked(now), 0.5, places=6)

    def test_refills_at_the_configured_rate(self):
        g = self.make(rate=2)
        now = self.clock.t
        g._tokenWaitLocked(now); g._consumeTokenLocked()
        g._tokenWaitLocked(now); g._consumeTokenLocked()  # drained
        self.clock.advance(0.5)                            # 0.5s * 2/s = 1 token
        self.assertEqual(g._tokenWaitLocked(self.clock.t), 0.0)
        g._consumeTokenLocked()
        self.assertAlmostEqual(g._tokenWaitLocked(self.clock.t), 0.5, places=6)

    def test_tokens_capped_at_capacity(self):
        g = self.make(rate=2)  # capacity 2
        now = self.clock.t
        g._tokenWaitLocked(now); g._consumeTokenLocked()
        g._tokenWaitLocked(now); g._consumeTokenLocked()  # drained
        self.clock.advance(100)  # would refill 200, but capacity caps it at 2
        self.assertEqual(g._tokenWaitLocked(self.clock.t), 0.0); g._consumeTokenLocked()
        self.assertEqual(g._tokenWaitLocked(self.clock.t), 0.0); g._consumeTokenLocked()
        self.assertGreater(g._tokenWaitLocked(self.clock.t), 0.0)

    def test_rate_zero_is_unlimited(self):
        g = self.make(rate=0)
        now = self.clock.t
        for _ in range(100):
            self.assertEqual(g._tokenWaitLocked(now), 0.0)
            g._consumeTokenLocked()

    def test_acquire_consumes_a_token(self):
        clock = FakeClock()
        g = AdaptiveConcurrency(4, rate=100, clock=clock)  # plenty of tokens, no blocking
        start = g._tokens
        for _ in range(3):
            g.acquire()
            g.release()
        self.assertAlmostEqual(g._tokens, start - 3, places=6)


class FakeResponse:
    def __init__(self, status_code=200, headers=None, content=b''):
        self.status_code = status_code
        self.headers = headers or {}
        self.content = content
        self.reason = 'Fake'

    def json(self):
        return {}


class FakeSession:
    '''Serves a scripted sequence of responses/exceptions to driver.session.get.'''
    def __init__(self, behaviors):
        self.behaviors = list(behaviors)
        self.calls = 0

    def get(self, url, headers=None, params=None, timeout=None):
        behavior = self.behaviors[min(self.calls, len(self.behaviors) - 1)]
        self.calls += 1
        if isinstance(behavior, Exception):
            raise behavior
        return behavior


class GovernorWiringTests(unittest.TestCase):
    '''getRaw routes outcomes through the governor and still retries correctly.'''
    def setUp(self):
        # These tests deliberately exercise the retry/429 paths; mute their logs.
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def makeDriver(self, ceiling=4):
        config = ConfigFile()
        config.hostname = 'http://example'
        config.throttlingMaxConcurrency = ceiling
        config.throttlingRequestTimeout = 1.0
        driver = MattermostDriver(config)
        driver.HTTP_RETRY_BASE_DELAY = 0.0  # make backoff sleeps instant
        return driver

    def test_success_releases_permit(self):
        driver = self.makeDriver()
        driver.session = FakeSession([FakeResponse(200)])
        r = driver.getRaw('ping')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(driver.concurrency.inFlight, 0)
        self.assertEqual(len(driver.concurrency._failures), 0)

    def test_timeout_is_retried_then_succeeds(self):
        driver = self.makeDriver()
        driver.session = FakeSession([Timeout('boom'), FakeResponse(200)])
        r = driver.getRaw('ping')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(driver.session.calls, 2, 'one retry after the timeout')
        self.assertEqual(len(driver.concurrency._failures), 1)
        self.assertEqual(driver.concurrency.inFlight, 0)

    def test_clustered_timeouts_shrink_concurrency(self):
        driver = self.makeDriver(ceiling=4)
        driver.session = FakeSession([Timeout('a'), Timeout('b'), FakeResponse(200)])
        driver.getRaw('ping')
        self.assertEqual(driver.concurrency.live, 2, 'two clustered timeouts halve live')

    def test_429_retry_after_is_honored(self):
        driver = self.makeDriver()
        driver.session = FakeSession([
            FakeResponse(429, headers={'Retry-After': '0'}),
            FakeResponse(200),
        ])
        r = driver.getRaw('ping')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(driver.session.calls, 2)
        self.assertEqual(len(driver.concurrency._failures), 1)

    def test_exhausted_retries_release_permit_and_raise(self):
        driver = self.makeDriver()
        driver.session = FakeSession([Timeout('always')])
        with self.assertRaises(Timeout):
            driver.getRaw('ping')
        self.assertEqual(driver.concurrency.inFlight, 0, 'permit released even on failure')


class FakeFileDriver:
    '''Minimal driver for Saver.processFiles: serves bytes per url.'''
    def __init__(self):
        self.lock = threading.Lock()
        self.fetched = []

    def getRaw(self, url):
        return FakeResponse(200, headers={}, content=b'')

    def storeUrlInto(self, url, fp):
        with self.lock:
            self.fetched.append(url)
        fp.write(('data:' + url).encode('utf8'))


class ParallelFileDownloadTests(unittest.TestCase):
    def processFiles(self, saver, entities, stored, driverCls=FakeFileDriver):
        saver.backend.processFiles(
            entities, 'files', 'things',
            getFilenameFromEntity=lambda e: e,
            shouldDownload=lambda e: True,
            getUrlFromEntity=lambda e: f'url/{e}',
            storeFilename=lambda e, name: stored.__setitem__(e, name),
            getSuffixHint=lambda e: '.bin')

    def test_all_files_downloaded_once_and_recorded(self):
        with tempfile.TemporaryDirectory() as d:
            config = makeConfig(d)
            config.throttlingMaxConcurrency = 4
            driver = FakeFileDriver()
            saver = makeSaver(config, driver)
            entities = [f'e{i}' for i in range(20)]
            stored = {}
            self.processFiles(saver, entities, stored)

            self.assertEqual(set(stored), set(entities))
            outdir = Path(d) / 'files'
            for e in entities:
                f = outdir / (e + '.bin')
                self.assertTrue(f.exists(), f'{e} not written')
                self.assertEqual(f.read_bytes(), ('data:url/' + e).encode('utf8'))
            self.assertEqual(sorted(driver.fetched), sorted(f'url/{e}' for e in entities))
            self.assertEqual(stored['e0'], 'e0.bin')

    def test_downloads_actually_run_in_parallel(self):
        # A barrier that only trips if >= 4 downloads are in flight at once; with a
        # sequential walk the first would block until the barrier times out.
        barrier = threading.Barrier(4, timeout=3.0)

        class BarrierDriver(FakeFileDriver):
            def storeUrlInto(self, url, fp):
                barrier.wait()
                super().storeUrlInto(url, fp)

        with tempfile.TemporaryDirectory() as d:
            config = makeConfig(d)
            config.throttlingMaxConcurrency = 4
            saver = makeSaver(config, BarrierDriver())
            entities = [f'e{i}' for i in range(8)]
            stored = {}
            self.processFiles(saver, entities, stored)
            self.assertEqual(len(stored), 8)

    def test_empty_output_folder_not_created_when_nothing_to_download(self):
        with tempfile.TemporaryDirectory() as d:
            config = makeConfig(d)
            config.throttlingMaxConcurrency = 4
            saver = makeSaver(config, FakeFileDriver())
            stored = {}
            saver.backend.processFiles(
                [], 'files', 'things',
                getFilenameFromEntity=lambda e: e,
                shouldDownload=lambda e: True,
                getUrlFromEntity=lambda e: f'url/{e}',
                storeFilename=lambda e, name: stored.__setitem__(e, name))
            self.assertFalse((Path(d) / 'files').exists())


class ParallelChannelTests(unittest.TestCase):
    def runChannels(self, outdir, concurrency, stems):
        config = makeConfig(outdir, pageSize=3)
        config.throttlingMaxConcurrency = concurrency
        driver = FakePostsDriver(config, [mmPost(f'p{i}', i * 10) for i in range(1, 8)])
        saver = makeSaver(config, driver)

        def downloadChannel(stem):
            channel = makeChannel(messageCount=7)
            saver.processChannel(stem, ChannelRequest(config=ChannelOptions(), metadata=channel))

        saver._processChannels([lambda s=s: downloadChannel(s) for s in stems])

    def test_parallel_output_matches_sequential_bytewise(self):
        stems = [f'o.team--chan{i}' for i in range(6)]
        with tempfile.TemporaryDirectory() as seqDir, tempfile.TemporaryDirectory() as parDir:
            self.runChannels(seqDir, concurrency=1, stems=stems)
            self.runChannels(parDir, concurrency=4, stems=stems)
            for stem in stems:
                seqFile = Path(seqDir) / (stem + '.data.json')
                parFile = Path(parDir) / (stem + '.data.json')
                self.assertTrue(seqFile.exists() and parFile.exists(), stem)
                self.assertEqual(seqFile.read_bytes(), parFile.read_bytes(),
                                 f'{stem} differs between sequential and parallel runs')
                self.assertEqual(readStoredIds(seqFile),
                                 ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'])


class CooperativeStopTests(unittest.TestCase):
    def test_stop_event_leaves_resumable_buffer_then_resumes(self):
        OUT = 'o.team--chan'
        with tempfile.TemporaryDirectory() as d:
            allPosts = [mmPost(f'p{i}', i * 10) for i in range(1, 8)]
            config = makeConfig(d, pageSize=2)
            config.throttlingMaxConcurrency = 2  # parallel mode

            holder = {}

            # Request a stop right after the 2nd page is fetched, so page 1's posts
            # are safely buffered when the next page's first post trips the stop.
            class StopAfterSecondPageDriver(FakePostsDriver):
                def get(self, command, params=None):
                    res = super().get(command, params)
                    if len(self.requestLog) == 2:
                        holder['saver'].stopEvent.set()
                    return res

            driver = StopAfterSecondPageDriver(config, allPosts)
            saver = makeSaver(config, driver)
            holder['saver'] = saver

            data = Path(d) / (OUT + '.data.json')
            tmp = Path(d) / (OUT + '.data.json.tmp')
            channel = makeChannel(messageCount=7)
            request = ChannelRequest(config=ChannelOptions(), metadata=channel)

            # The worker wrapper swallows the cooperative KeyboardInterrupt.
            saver._runChannelTask(
                lambda: saver.processChannel(OUT, request))

            self.assertTrue(saver.stopEvent.is_set())
            self.assertFalse(data.exists(), 'nothing is committed on a cooperative stop')
            self.assertTrue(tmp.exists() and tmp.stat().st_size > 0,
                            'a resumable buffer is left behind')
            self.assertEqual(readStoredIds(tmp), ['p7', 'p6'], 'page 1 buffered newest-first')

            # Resume with a fresh, non-stopping driver: must seek below the buffer.
            config2 = makeConfig(d, pageSize=2)
            driver2 = FakePostsDriver(config2, allPosts)
            saver2 = makeSaver(config2, driver2)
            saver2.processChannel(OUT, request)

            self.assertEqual(readStoredIds(data),
                             ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'])
            self.assertFalse(tmp.exists())
            self.assertEqual(driver2.requestLog[0].get('before'), 'p6',
                             'resume seeks below the buffer oldest, not from the newest')


class ParallelProgressDisplayTests(unittest.TestCase):
    '''The single progress path: a live multi-line block on a tty (one line per active
    channel), plain lines off a tty, nothing when disabled.'''
    POSTS = [mmPost(f'p{i}', i * 10) for i in range(1, 8)]

    def runChannels(self, outdir, names, reportProgress, concurrency=4):
        # Build the saver INSIDE the redirect so its progress manager targets the
        # captured stream; download several distinct channels concurrently.
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            config = makeConfig(outdir, pageSize=3)
            config.throttlingMaxConcurrency = concurrency
            config.progressInterval = 0  # render every update, deterministically
            config.reportProgress = reportProgress
            saver = makeSaver(config, FakePostsDriver(config, self.POSTS))

            def downloadChannel(name):
                channel = makeChannel(id=name, messageCount=7)
                saver.processChannel(f'o.t--{name}', ChannelRequest(config=ChannelOptions(), metadata=channel))

            with saver.progress, saver.progress.captureLogging():
                saver._processChannels([lambda n=n: downloadChannel(n) for n in names])
        return err.getvalue()

    def test_tty_shows_a_live_line_per_channel(self):
        names = ['alpha', 'beta', 'gamma']
        ansi = progress.ProgressSettings(progress.VisualizationMode.AnsiEscapes, forceMode=True)
        with tempfile.TemporaryDirectory() as d:
            out = self.runChannels(d, names, ansi)
        for name in names:
            self.assertIn(f'{name}: ', out, out)
        self.assertIn('\x1b[', out, 'tty mode drives the live block with cursor escapes')

    def test_offtty_prints_plain_lines(self):
        names = ['alpha', 'beta']
        autodetect = progress.ProgressSettings(progress.VisualizationMode.AnsiEscapes, forceMode=False)
        with tempfile.TemporaryDirectory() as d:
            out = self.runChannels(d, names, autodetect)  # StringIO is not a tty -> plain
        for name in names:
            self.assertIn(f'{name}: ', out)
        self.assertNotIn('\x1b[', out, 'off a tty there must be no cursor escapes')

    def test_disabled_is_silent(self):
        off = progress.ProgressSettings(progress.VisualizationMode.DumbTerminal, forceMode=True)
        with tempfile.TemporaryDirectory() as d:
            out = self.runChannels(d, ['alpha'], off)
        self.assertNotIn('posts', out)


if __name__ == '__main__':
    unittest.main()
