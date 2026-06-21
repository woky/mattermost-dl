'''
    Progress reporting for long-running foreground work.

    A single ProgressManager owns a "live region" at the bottom of an output stream and
    renders one line per active task, so several channels downloading concurrently each
    get their own updating line. On an interactive terminal the region is a block of
    lines redrawn in place (cursor up / clear / rewrite), git-style. Off a terminal (or
    when forced dumb) each task instead prints plain periodic lines.

    So ordinary logging coexists with the live block, log records are routed through the
    manager (see captureLogging): each is written as a permanent line above the region,
    which is then redrawn beneath it. This is the single progress/output path; there is
    no separate code path for the one-thread vs many-threads case.
'''

from .common import *

import contextlib
import logging
import shutil
import threading
from collections import OrderedDict
from copy import copy
from time import monotonic_ns as _monotonic


class VisualizationMode(Enum):
    DumbTerminal = 0
    AnsiEscapes = 1


@dataclass
class ProgressSettings:
    mode: VisualizationMode = VisualizationMode.AnsiEscapes
    forceMode: bool = False


_CURSOR_UP = '\x1b[{n}A'    # move cursor up n lines
_CLEAR_DOWN = '\x1b[0J'     # clear from cursor to end of screen


def _isInteractive(io: TextIO) -> bool:
    isatty = getattr(io, 'isatty', None)
    return bool(isatty and isatty()) and not sys.platform.startswith('win')


def _terminalWidth() -> int:
    return shutil.get_terminal_size(fallback=(80, 24)).columns


def _truncate(text: str, width: int) -> str:
    # Lines must not exceed the terminal width: a wrapped line occupies two rows and
    # breaks the cursor-up line accounting that the live region depends on.
    return text if len(text) <= width else text[:max(1, width - 1)] + '…'


class ProgressManager:
    '''
        Thread-safe owner of the live region. `enabled=False` makes every operation a
        no-op (used for quiet runs or when progress is switched off in config), leaving
        logging untouched.
    '''
    def __init__(self, io: TextIO, settings: ProgressSettings = ProgressSettings(),
            updateIntervalMs: int = 500, enabled: bool = True,
            clock: Callable[[], int] = _monotonic):
        self.io = io
        self.settings = copy(settings)
        self.enabled = enabled
        self.updateIntervalNs = 1_000_000 * updateIntervalMs
        self._clock = clock
        self._lock = threading.RLock()
        self._tasks: "OrderedDict[int, str]" = OrderedDict()  # id -> current line text
        self._plainNext: Dict[int, int] = {}                  # id -> next allowed print (plain mode)
        self._nextId = 0
        self._drawn = 0          # region lines currently on screen (live mode)
        self._nextRender = 0     # throttle gate for the live region
        self._closed = False

        # Pick the backend once. Outside a tty (or on Windows cmd) fall back to plain
        # lines unless the caller forces a mode.
        if enabled and not self.settings.forceMode and not _isInteractive(self.io):
            self.settings.mode = VisualizationMode.DumbTerminal

    @property
    def _live(self) -> bool:
        return self.enabled and self.settings.mode == VisualizationMode.AnsiEscapes

    # -- task API -------------------------------------------------------------
    def task(self, label: str, unit: str = 'posts') -> 'ProgressTask':
        with self._lock:
            taskId = self._nextId
            self._nextId += 1
        return ProgressTask(self, taskId, label, unit)

    def _setTaskText(self, taskId: int, text: str):
        with self._lock:
            if not self.enabled or self._closed:
                return
            self._tasks[taskId] = text
            if self._live:
                self._renderLocked(force=False)
            else:
                self._plainLineLocked(taskId, text)

    def _removeTask(self, taskId: int):
        with self._lock:
            existed = self._tasks.pop(taskId, None) is not None
            self._plainNext.pop(taskId, None)
            if existed and self._live:
                self._renderLocked(force=True)

    # -- permanent lines (logs scroll above the live region) ------------------
    def writeLine(self, msg: str):
        with self._lock:
            if not self.enabled or self._closed:
                return
            if self._live:
                self._moveToRegionTopLocked()
                self.io.write(msg + '\n')
                self._drawn = 0
                self._renderLocked(force=True)
            else:
                self.io.write(msg + '\n')
                self.io.flush()

    # -- live rendering -------------------------------------------------------
    def _moveToRegionTopLocked(self):
        if self._drawn:
            self.io.write(_CURSOR_UP.format(n=self._drawn))
        self.io.write(_CLEAR_DOWN)

    def _renderLocked(self, *, force: bool):
        now = self._clock()
        if not force and now < self._nextRender:
            return
        self._nextRender = now + self.updateIntervalNs
        self._moveToRegionTopLocked()
        width = _terminalWidth()
        for text in self._tasks.values():
            self.io.write(_truncate(text, width) + '\n')
        self._drawn = len(self._tasks)
        self.io.flush()

    # -- plain (non-tty) line per task, throttled independently ---------------
    def _plainLineLocked(self, taskId: int, text: str):
        now = self._clock()
        if now < self._plainNext.get(taskId, 0):
            return
        self._plainNext[taskId] = now + self.updateIntervalNs
        self.io.write(text + '\n')
        self.io.flush()

    # -- lifecycle ------------------------------------------------------------
    def close(self):
        with self._lock:
            if self._closed:
                return
            if self._live and self._drawn:
                self.io.write(_CURSOR_UP.format(n=self._drawn) + _CLEAR_DOWN)
                self.io.flush()
            self._drawn = 0
            self._closed = True

    def __enter__(self) -> 'ProgressManager':
        return self

    def __exit__(self, *exc) -> bool:
        self.close()
        return False

    @contextlib.contextmanager
    def captureLogging(self):
        '''
            For the duration, route root-logger records through writeLine so they print
            as permanent lines above the live region instead of scribbling into it. A
            no-op when disabled (logging then behaves normally).
        '''
        if not self.enabled:
            yield self
            return
        root = logging.getLogger()
        previous = root.handlers[:]
        handler = ProgressLogHandler(self)
        if previous:
            handler.setLevel(previous[0].level)
            if previous[0].formatter is not None:
                handler.setFormatter(previous[0].formatter)
        root.handlers = [handler]
        try:
            yield self
        finally:
            root.handlers = previous


class ProgressTask:
    '''One updating line. Obtain via ProgressManager.task(); usable as a context manager.'''
    def __init__(self, manager: ProgressManager, taskId: int, label: str, unit: str):
        self._manager = manager
        self._id = taskId
        self.label = label
        self.unit = unit
        self._finished = False

    def update(self, count: Optional[int] = None, total: Optional[int] = None,
            note: Optional[str] = None):
        if note is not None:
            text = f'{self.label}: {note}'
        elif total is not None and total > 0:
            text = f'{self.label}: {count}/{total} {self.unit}'
        elif count is not None:
            text = f'{self.label}: {count} {self.unit}'
        else:
            text = f'{self.label}: …'
        self._manager._setTaskText(self._id, text)

    def finish(self):
        if self._finished:
            return
        self._finished = True
        self._manager._removeTask(self._id)

    def __enter__(self) -> 'ProgressTask':
        return self

    def __exit__(self, *exc) -> bool:
        self.finish()
        return False


class ProgressLogHandler(logging.Handler):
    '''Routes log records through a ProgressManager so they coexist with the live region.'''
    def __init__(self, manager: ProgressManager):
        super().__init__()
        self._manager = manager

    def emit(self, record: logging.LogRecord):
        try:
            self._manager.writeLine(self.format(record))
        except Exception:
            self.handleError(record)


if __name__ == '__main__':
    import sys
    import threading as _threading
    from time import sleep

    print("Multi-line progress demo (three concurrent tasks)...")
    manager = ProgressManager(sys.stderr, updateIntervalMs=50)

    def work(name, n):
        with manager.task(name) as task:
            for i in range(n):
                task.update(i + 1, n)
                sleep(0.01)
        logging.getLogger().warning(f'{name}: done ({n})')

    with manager, manager.captureLogging():
        threads = [_threading.Thread(target=work, args=(f'channel-{k}', 50 + 40 * k))
                   for k in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    print("done.")
