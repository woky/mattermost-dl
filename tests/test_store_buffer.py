'''
    Tests for the newline-delimited post buffer/data-file readers in store.py:
    backward line reading, last-post reading and the crash-safety reconcile.
'''

import json
import os
import tempfile
import unittest
from pathlib import Path

from mattermost_dl.bo import Time
from mattermost_dl.store import (_iterLinesBackward, countStoredPosts,
                                 iterPostsBackward, readLastStoredPost,
                                 trimDataFileNewerThan)


def postLine(id, createTime, **extra):
    info = {'id': id, 'userId': 'u', 'createTime': createTime, 'message': 'm'}
    info.update(extra)
    return json.dumps(info)


class BackwardLineTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.dir = Path(self._dir.name)

    def tearDown(self):
        self._dir.cleanup()

    def back(self, data, chunk=65536):
        path = self.dir / 'f'
        path.write_bytes(data)
        with open(path, 'rb') as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            return [(line.decode(), offset) for line, offset in _iterLinesBackward(f, size, chunk)]

    def test_empty(self):
        self.assertEqual(self.back(b''), [])

    def test_single_terminated(self):
        self.assertEqual(self.back(b'a\n'), [('a', 0)])

    def test_single_unterminated_dropped(self):
        # No terminating newline -> incomplete single line -> nothing.
        self.assertEqual(self.back(b'abc'), [])

    def test_two_lines_newest_first(self):
        self.assertEqual(self.back(b'a\nbb\n'), [('bb', 2), ('a', 0)])

    def test_trailing_partial_dropped(self):
        self.assertEqual(self.back(b'a\nbb\nPART'), [('bb', 2), ('a', 0)])

    def test_chunk_boundaries_consistent(self):
        data = b''.join(f'L{i}\n'.encode() for i in range(50))
        full = self.back(data, 1_000_000)
        for chunk in (1, 2, 3, 4, 7, 64):
            self.assertEqual(self.back(data, chunk), full, f'chunk={chunk}')

    def test_offsets_point_at_line_start(self):
        data = b''.join(f'L{i}\n'.encode() for i in range(20))
        for line, offset in self.back(data, 3):
            self.assertEqual(data[offset:offset + len(line)], line.encode())


class ReadLastStoredPostTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.dir = Path(self._dir.name)

    def tearDown(self):
        self._dir.cleanup()

    def test_missing_file(self):
        self.assertIsNone(readLastStoredPost(self.dir / 'nope'))

    def test_empty_file(self):
        path = self.dir / 'f'
        path.write_bytes(b'')
        self.assertIsNone(readLastStoredPost(path))

    def test_reads_last_complete(self):
        path = self.dir / 'f'
        path.write_text(postLine('a', 10) + '\n' + postLine('b', 20) + '\n')
        id_, time, validLen = readLastStoredPost(path)
        self.assertEqual(id_, 'b')
        self.assertEqual(time.timestamp, 20)
        self.assertEqual(validLen, path.stat().st_size)

    def test_ignores_partial_trailing_line(self):
        path = self.dir / 'f'
        good = postLine('a', 10) + '\n'
        path.write_text(good + postLine('b', 20))  # second line has no newline
        id_, time, validLen = readLastStoredPost(path)
        self.assertEqual(id_, 'a')
        self.assertEqual(time.timestamp, 10)
        self.assertEqual(validLen, len(good.encode()))  # buffer would be truncated here


class IterPostsBackwardTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.dir = Path(self._dir.name)

    def tearDown(self):
        self._dir.cleanup()

    def test_reverses_newest_to_oldest_buffer(self):
        # Buffer holds posts newest -> oldest; reverse-read yields oldest -> newest.
        path = self.dir / 'buf'
        path.write_text(postLine('c', 30) + '\n' + postLine('b', 20) + '\n' + postLine('a', 10) + '\n')
        ids = [json.loads(line)['id'] for line in iterPostsBackward(path)]
        self.assertEqual(ids, ['a', 'b', 'c'])

    def test_empty_buffer_yields_nothing(self):
        path = self.dir / 'buf'
        path.write_bytes(b'')
        self.assertEqual(list(iterPostsBackward(path)), [])


class CountStoredPostsTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.dir = Path(self._dir.name)

    def tearDown(self):
        self._dir.cleanup()

    def test_missing_is_zero(self):
        self.assertEqual(countStoredPosts(self.dir / 'nope'), 0)

    def test_counts_complete_lines(self):
        path = self.dir / 'f'
        path.write_text(postLine('a', 10) + '\n' + postLine('b', 20) + '\n')
        self.assertEqual(countStoredPosts(path), 2)

    def test_does_not_count_partial_tail(self):
        path = self.dir / 'f'
        path.write_text(postLine('a', 10) + '\n' + postLine('b', 20))  # no trailing newline
        self.assertEqual(countStoredPosts(path), 1)


class TrimDataFileNewerThanTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.dir = Path(self._dir.name)

    def tearDown(self):
        self._dir.cleanup()

    def ids(self, path):
        return [json.loads(line)['id'] for line in path.read_text().splitlines() if line]

    def test_trims_tail_at_or_after_boundary(self):
        path = self.dir / 'd'
        path.write_text(postLine('a', 10) + '\n' + postLine('b', 20) + '\n' + postLine('c', 30) + '\n')
        trimDataFileNewerThan(path, Time(20))  # remove createTime >= 20
        self.assertEqual(self.ids(path), ['a'])

    def test_keeps_everything_below_boundary(self):
        path = self.dir / 'd'
        path.write_text(postLine('a', 10) + '\n' + postLine('b', 20) + '\n')
        trimDataFileNewerThan(path, Time(100))
        self.assertEqual(self.ids(path), ['a', 'b'])

    def test_drops_only_partial_trailing_line(self):
        path = self.dir / 'd'
        path.write_text(postLine('a', 10) + '\n' + postLine('b', 20) + '\n' + '{trunc')
        trimDataFileNewerThan(path, Time(100))  # boundary above all complete posts
        self.assertEqual(self.ids(path), ['a', 'b'])

    def test_trims_everything(self):
        path = self.dir / 'd'
        path.write_text(postLine('a', 10) + '\n' + postLine('b', 20) + '\n')
        trimDataFileNewerThan(path, Time(5))  # boundary below all
        self.assertEqual(path.stat().st_size, 0)

    def test_missing_file_is_noop(self):
        trimDataFileNewerThan(self.dir / 'nope', Time(10))  # must not raise


if __name__ == '__main__':
    unittest.main()
