'''
    Tests for the unified progress reporter: the live multi-line region on a tty, the
    plain per-task lines off one, log records routed above the live block, throttling,
    line truncation and the disabled no-op.
'''

import io
import logging
import os
import re
import unittest

from mattermost_dl.progress import (ProgressManager, ProgressSettings,
                                     VisualizationMode)

_ANSI = re.compile(r'\x1b\[[0-9]*[A-Za-z]')


def stripAnsi(text: str) -> str:
    return _ANSI.sub('', text)


def liveManager(stream, intervalMs=0, clock=lambda: 0):
    return ProgressManager(stream,
        settings=ProgressSettings(VisualizationMode.AnsiEscapes, forceMode=True),
        updateIntervalMs=intervalMs, clock=clock)


def plainManager(stream, intervalMs=0, clock=lambda: 0):
    return ProgressManager(stream,
        settings=ProgressSettings(VisualizationMode.DumbTerminal, forceMode=True),
        updateIntervalMs=intervalMs, clock=clock)


class Clock:
    def __init__(self):
        self.t = 0

    def __call__(self):
        return self.t


class LiveRegionTests(unittest.TestCase):
    def test_single_task_renders_its_line(self):
        buf = io.StringIO()
        m = liveManager(buf)
        m.task('chan').update(3, 7)
        self.assertIn('chan: 3/7 posts', buf.getvalue())

    def test_count_only_when_total_unknown(self):
        buf = io.StringIO()
        liveManager(buf).task('chan').update(5)
        self.assertIn('chan: 5 posts', buf.getvalue())

    def test_note_replaces_count(self):
        buf = io.StringIO()
        liveManager(buf).task('chan').update(note='skipping filtered posts')
        self.assertIn('chan: skipping filtered posts', buf.getvalue())

    def test_multiple_tasks_render_multiple_lines(self):
        buf = io.StringIO()
        m = liveManager(buf)
        a, b = m.task('alpha'), m.task('beta')
        a.update(1, 10)
        b.update(2, 20)
        out = buf.getvalue()
        self.assertIn('alpha: 1/10 posts', out)
        self.assertIn('beta: 2/20 posts', out)
        # Redrawing a two-line block moves the cursor up over the first line.
        self.assertIn('\x1b[1A', out)

    def test_finishing_a_task_redraws_smaller_block(self):
        buf = io.StringIO()
        m = liveManager(buf)
        a, b = m.task('alpha'), m.task('beta')
        a.update(1, 10)
        b.update(2, 20)        # block is now two lines
        b.finish()             # redraw must step back over both lines and clear
        self.assertIn('\x1b[2A', buf.getvalue())

    def test_writeLine_prints_permanent_line_and_keeps_region(self):
        buf = io.StringIO()
        m = liveManager(buf)
        task = m.task('alpha')
        task.update(1, 10)
        m.writeLine('a log message')
        task.update(2, 10)
        out = buf.getvalue()
        self.assertIn('a log message', out)
        self.assertIn('alpha: 2/10 posts', out)

    def test_lazy_task_with_no_update_draws_nothing(self):
        buf = io.StringIO()
        m = liveManager(buf)
        task = m.task('idle')
        task.finish()
        self.assertEqual(buf.getvalue(), '')

    def test_long_line_truncated_to_terminal_width(self):
        prev = os.environ.get('COLUMNS')
        os.environ['COLUMNS'] = '20'
        try:
            buf = io.StringIO()
            liveManager(buf).task('x' * 50).update(1, 10)
            longest = max((len(stripAnsi(line)) for line in buf.getvalue().split('\n')),
                          default=0)
            self.assertLessEqual(longest, 20)
            self.assertIn('…', buf.getvalue())
        finally:
            if prev is None:
                del os.environ['COLUMNS']
            else:
                os.environ['COLUMNS'] = prev

    def test_throttle_skips_intermediate_renders(self):
        clock = Clock()
        buf = io.StringIO()
        m = liveManager(buf, intervalMs=10, clock=clock)  # 10ms == 10_000_000 ns
        task = m.task('chan')
        task.update(1, 100)         # t=0: renders
        task.update(2, 100)         # t=0: throttled, not rendered
        clock.t = 10_000_000
        task.update(3, 100)         # interval elapsed: renders
        out = buf.getvalue()
        self.assertIn('chan: 1/100 posts', out)
        self.assertNotIn('chan: 2/100 posts', out)
        self.assertIn('chan: 3/100 posts', out)


class PlainModeTests(unittest.TestCase):
    def test_plain_emits_plain_lines_without_escapes(self):
        buf = io.StringIO()
        plainManager(buf).task('chan').update(1, 10)
        out = buf.getvalue()
        self.assertEqual(out, 'chan: 1/10 posts\n')
        self.assertNotIn('\x1b', out)

    def test_plain_throttles_per_task(self):
        clock = Clock()
        buf = io.StringIO()
        m = plainManager(buf, intervalMs=10, clock=clock)
        a, b = m.task('alpha'), m.task('beta')
        a.update(1, 10)             # prints
        a.update(2, 10)             # throttled
        b.update(1, 10)             # different task: prints immediately
        clock.t = 10_000_000
        a.update(3, 10)             # interval elapsed: prints
        out = buf.getvalue()
        self.assertIn('alpha: 1/10 posts', out)
        self.assertNotIn('alpha: 2/10 posts', out)
        self.assertIn('beta: 1/10 posts', out)
        self.assertIn('alpha: 3/10 posts', out)

    def test_plain_writeLine(self):
        buf = io.StringIO()
        plainManager(buf).writeLine('a log line')
        self.assertEqual(buf.getvalue(), 'a log line\n')


class DisabledTests(unittest.TestCase):
    def test_disabled_manager_emits_nothing(self):
        buf = io.StringIO()
        m = ProgressManager(buf, enabled=False, clock=lambda: 0)
        task = m.task('chan')
        task.update(1, 10)
        m.writeLine('nope')
        task.finish()
        self.assertEqual(buf.getvalue(), '')

    def test_disabled_captureLogging_leaves_handlers(self):
        m = ProgressManager(io.StringIO(), enabled=False)
        root = logging.getLogger()
        before = root.handlers[:]
        with m.captureLogging():
            self.assertEqual(root.handlers, before)


class LoggingCaptureTests(unittest.TestCase):
    def setUp(self):
        self.root = logging.getLogger()
        self._level = self.root.level
        self._handlers = self.root.handlers[:]
        self.root.setLevel(logging.INFO)

    def tearDown(self):
        self.root.handlers = self._handlers
        self.root.setLevel(self._level)

    def test_records_routed_through_manager_and_handlers_restored(self):
        buf = io.StringIO()
        m = liveManager(buf)
        before = self.root.handlers[:]
        with m.captureLogging():
            self.assertNotEqual(self.root.handlers, before)
            logging.getLogger().info('hello from a log')
        self.assertIn('hello from a log', buf.getvalue())
        self.assertEqual(self.root.handlers, before)


if __name__ == '__main__':
    unittest.main()
