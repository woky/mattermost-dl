'''
    The SQLite storage backend: a normalized single-file archive with full-text
    search, written directly during download (no separate load step).

    It is fed raw Mattermost API reply dicts and maps them onto a star schema
    (see `schema.py`): deduped dimension tables (teams/channels/users/emojis),
    a `posts` fact table mirrored into an FTS5 index by triggers, plus a durable
    per-channel `posts_staging` table that makes interrupted downloads resumable.
    Every table keeps the untouched API object in a `raw` column as a
    forward-compatibility safety net, so a future field can be promoted to a
    column by a migration that backfills from `raw` -- no re-download.

    Referenced binary assets (attachments, custom-emoji images, avatars) are
    stored inline as deduped BLOBs, gated by the same `ChannelOptions` knobs as
    the directory backend. The asset phase is split into a knob-gated, deduped,
    parallel *fetch* and a tiny *sink* (`_hasContent`/`_storeContent`), so moving
    blobs to the filesystem later is a localized change.
'''

import io
import json
import logging
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from pathlib import Path
from typing import (Callable, Dict, Generator, Iterable, List, Optional, Sequence,
                    Tuple)

from ...config import ChannelOptions, ConfigFile
from ...types import Id, Time
from ..base import (ChannelArchive, DownloadServices, ResumeState, StagingWriter,
                    StorageBackend)
from .schema import applyMigrations

def _dumps(obj) -> str:
    '''Compact, stable JSON for a `raw` column.'''
    return json.dumps(obj, ensure_ascii=False, separators=(',', ':'))


@contextmanager
def _writeTxn(conn: sqlite3.Connection) -> 'Generator[None, None, None]':
    '''
        An explicit transaction on an autocommit connection. The connection runs in
        autocommit mode (isolation_level=None) so it is never left with a dangling
        implicit transaction between locked regions -- a hazard on a single
        connection shared by concurrent channel workers. A multi-statement write
        brackets itself with BEGIN/COMMIT here; the whole bracket runs under the DB
        lock, so transactions from different threads never overlap.
    '''
    conn.execute('BEGIN')
    try:
        yield
    except BaseException:
        conn.execute('ROLLBACK')
        raise
    conn.execute('COMMIT')


def _buildUpsert(table: str, cols: Sequence[str], pk: Sequence[str], merge: bool) -> str:
    '''
        An ``INSERT ... ON CONFLICT`` upsert. ``merge`` keeps existing non-NULL
        values (``COALESCE``) so a minimal author row never clobbers a richer one;
        otherwise the incoming row wins (``=excluded``). Mirrors the reference
        loader's `_build_stmt`.
    '''
    updatable = [c for c in cols if c not in pk]
    if updatable:
        assign = ', '.join(
            (f'{c}=COALESCE(excluded.{c}, {table}.{c})' if merge else f'{c}=excluded.{c}')
            for c in updatable)
        conflict = f'DO UPDATE SET {assign}'
    else:
        conflict = 'DO NOTHING'
    return (f"INSERT INTO {table} ({', '.join(cols)}) "
            f"VALUES ({', '.join('?' for _ in cols)}) "
            f"ON CONFLICT ({', '.join(pk)}) {conflict}")


# Metadata upserts. Blob columns (users.avatar, emojis.image, attachments.content)
# are deliberately omitted here: they are filled by the asset phase and must not be
# reset to NULL when a row is re-upserted on an incremental run.
_TEAM_SQL = _buildUpsert('teams', ('id', 'name', 'display_name', 'raw'), ('id',), merge=True)
_CHANNEL_SQL = _buildUpsert('channels',
    ('id', 'team_id', 'name', 'display_name', 'type', 'purpose', 'raw'), ('id',), merge=True)
_USER_SQL = _buildUpsert('users',
    ('id', 'username', 'nickname', 'first_name', 'last_name', 'email', 'raw'), ('id',), merge=True)
_EMOJI_SQL = _buildUpsert('emojis', ('id', 'name', 'creator_id', 'raw'), ('id',), merge=True)
_POST_SQL = _buildUpsert('posts',
    ('id', 'channel_id', 'user_id', 'root_id', 'type', 'message',
     'create_at', 'edit_at', 'delete_at', 'raw'), ('id',), merge=False)
_REACTION_SQL = _buildUpsert('reactions',
    ('post_id', 'user_id', 'emoji_name', 'create_at'),
    ('post_id', 'user_id', 'emoji_name'), merge=False)
_ATTACHMENT_SQL = _buildUpsert('attachments',
    ('id', 'post_id', 'name', 'extension', 'size', 'mime_type', 'raw'), ('id',), merge=True)


def _attachmentExtension(name: Optional[str]) -> Optional[str]:
    if name and '.' in name:
        return name.rsplit('.', 1)[-1].lower()
    return None


class SqliteBackend(StorageBackend):
    '''
        Run-scoped owner of one SQLite connection. All DB access serializes through
        a single lock (SQLite forbids concurrent use of one connection); network
        fetches still parallelize across channel workers, only the fast DB writes
        serialize.
    '''

    def __init__(self, config: ConfigFile, services: DownloadServices, progress):
        self.config = config
        self.services = services
        self.progress = progress
        self.path: Path = self._resolvePath(config)
        self._conn: Optional[sqlite3.Connection] = None
        self._initLock = threading.Lock()   # guards lazy connect/close
        self._dbLock = threading.Lock()     # guards every use of the connection

    @staticmethod
    def _resolvePath(config: ConfigFile) -> Path:
        configured = getattr(config, 'outputSqlitePath', None)
        if configured:
            return Path(configured)
        outdir = config.outputDirectory
        name = outdir.name or 'archive'
        return outdir / (name + '.sqlite')

    # ----- connection ---------------------------------------------------------
    def _connection(self) -> sqlite3.Connection:
        '''Lazily open the connection (and run migrations) on first use.'''
        with self._initLock:
            if self._conn is None:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                conn = sqlite3.connect(str(self.path), check_same_thread=False)
                conn.row_factory = sqlite3.Row
                # Autocommit: transactions are managed explicitly under the DB lock
                # so the one shared connection is never mid-transaction across
                # locked regions (see `_writeTxn`).
                conn.isolation_level = None
                conn.execute('PRAGMA journal_mode=WAL')
                conn.execute('PRAGMA foreign_keys=ON')
                conn.execute('PRAGMA synchronous=NORMAL')
                applyMigrations(conn)
                self._conn = conn
            return self._conn

    @contextmanager
    def locked(self) -> 'Generator[sqlite3.Connection, None, None]':
        '''Yield the connection under the DB lock; the single DB-access gate.'''
        conn = self._connection()
        with self._dbLock:
            yield conn

    def open(self) -> None:
        # Lazy-connect (here and on first channelArchive use) so tests that skip
        # `with backend:` still work.
        self._connection()

    def close(self) -> None:
        with self._initLock:
            if self._conn is not None:
                with self._dbLock:
                    self._conn.commit()
                    self._conn.close()
                self._conn = None

    def channelArchive(self, key: str, channel: dict, team: Optional[dict],
                       options: ChannelOptions, seedUsers: Iterable[dict] = ()) -> ChannelArchive:
        self._connection()
        return SqliteChannelArchive(self, key, channel, team, options, list(seedUsers))

    # ----- asset sink + parallel fetch ---------------------------------------
    # Split into "fetch bytes" (knob-gated, deduped, parallel; independent of where
    # bytes land) and a tiny sink (`_hasContent`/`_storeContent`). Today the only
    # sink is the in-DB BLOB columns.
    def _hasContent(self, conn: sqlite3.Connection, table: str, blobCol: str, rowId: str) -> bool:
        row = conn.execute(
            f'SELECT 1 FROM {table} WHERE id=? AND {blobCol} IS NOT NULL', (rowId,)).fetchone()
        return row is not None

    def _storeContent(self, table: str, blobCol: str, rowId: str, data: bytes) -> None:
        # Autocommit: this single UPDATE is its own durable transaction, held under
        # the lock, so a long parallel download never holds the write lock.
        with self.locked() as conn:
            conn.execute(f'UPDATE {table} SET {blobCol}=? WHERE id=?', (data, rowId))

    def downloadBlobs(self, label: str, unit: str, table: str, blobCol: str,
                      ids: Iterable[str], getUrl: Callable[[str], str]) -> None:
        '''
            Fetch the bytes for each id missing its blob and store them inline.
            Already-present blobs are skipped, so this is idempotent and resumable
            across runs. Each blob is written in its own short transaction, outside
            any metadata commit, so a long download never holds the write lock.
        '''
        wanted = list(dict.fromkeys(ids))  # de-dupe, keep order
        if not wanted:
            return
        with self.locked() as conn:
            todo = [i for i in wanted if not self._hasContent(conn, table, blobCol, i)]
        if not todo:
            return

        def fetch(rowId: str) -> Tuple[str, bytes]:
            buf = io.BytesIO()
            self.services.storeUrlInto(getUrl(rowId), buf)
            return rowId, buf.getvalue()

        workers = max(1, self.config.throttlingMaxConcurrency)
        with self.progress.task(label, unit=unit) as task:
            completed = 0
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [pool.submit(fetch, rowId) for rowId in todo]
                for future in as_completed(futures):
                    rowId, data = future.result()
                    self._storeContent(table, blobCol, rowId, data)
                    completed += 1
                    task.update(completed, len(todo))

    def downloadEmojiDatabase(self, rawEmojis: Iterable[dict]) -> None:
        '''Upsert every custom emoji and fetch its image (config.downloadEmojis).'''
        emojis = [e for e in rawEmojis if e.get('id')]
        if not emojis:
            return
        with self.locked() as conn:
            with _writeTxn(conn):
                conn.executemany(_EMOJI_SQL, [
                    (e['id'], e.get('name'), e.get('creator_id'), _dumps(e)) for e in emojis])
        self.downloadBlobs('emojis', 'emojis', 'emojis', 'image',
            (e['id'] for e in emojis), self.services.getEmojiUrl)


class _SqliteStagingWriter(StagingWriter):
    '''
        Appends raw posts into the channel's durable `posts_staging` rows. Each add
        is its own autocommit write, so an interrupted run always leaves durable,
        resumable staging without any explicit flush.
    '''

    def __init__(self, archive: 'SqliteChannelArchive'):
        self._archive = archive

    def add(self, rawPost: dict) -> None:
        a = self._archive
        with a.backend.locked() as conn:
            conn.execute(
                'INSERT OR REPLACE INTO posts_staging(channel_id, id, create_at, raw) '
                'VALUES (?, ?, ?, ?)',
                (a.channelId, rawPost['id'], rawPost['create_at'], _dumps(rawPost)))


class SqliteChannelArchive(ChannelArchive):
    '''Maps the per-channel persistence contract onto SQL against one shared DB.'''

    def __init__(self, backend: SqliteBackend, key: str, channel: dict,
                 team: Optional[dict], options: ChannelOptions, seedUsers: List[dict]):
        self.backend = backend
        self.key = key
        self.options = options
        self._channel = channel
        self._team = team
        self._seedUsers = seedUsers
        self.channelId: Id = Id(channel['id'])

    # ----- download boundary --------------------------------------------------
    def committedNewestPost(self) -> Optional[Tuple[Id, Time]]:
        with self.backend.locked() as conn:
            row = conn.execute(
                'SELECT id, create_at FROM posts WHERE channel_id=? '
                'ORDER BY create_at DESC LIMIT 1', (self.channelId,)).fetchone()
        if row is None:
            return None
        return Id(row['id']), Time(row['create_at'])

    def reconcileBuffer(self) -> ResumeState:
        # Commits are atomic (staged rows are deleted in the same transaction that
        # upserts the posts), so no trim of committed data is ever needed: a
        # non-empty staging table simply means an interrupted fetch to continue.
        with self.backend.locked() as conn:
            rows = conn.execute(
                'SELECT id FROM posts_staging WHERE channel_id=? ORDER BY create_at ASC',
                (self.channelId,)).fetchall()
        if not rows:
            return ResumeState()
        return ResumeState(resume=True, resumeCursor=Id(rows[0]['id']), priorCount=len(rows))

    @contextmanager
    def stagingWriter(self, resume: bool) -> 'Generator[StagingWriter, None, None]':
        if not resume:
            # Fresh fetch: drop any leftover staging for this channel (a resume would
            # have been signalled by reconcileBuffer instead).
            with self.backend.locked() as conn:
                conn.execute('DELETE FROM posts_staging WHERE channel_id=?', (self.channelId,))
        yield _SqliteStagingWriter(self)

    # ----- commit -------------------------------------------------------------
    def commit(self, *, incremental: bool, localNewestId: Optional[Id]) -> None:
        '''
            Durably persist the staged posts: one transaction upserts the channel's
            dimensions and posts (mirrored into FTS via triggers) and clears its
            staging; then the asset phase fetches referenced blobs. The
            `incremental`/`localNewestId` args are unused -- upserts are idempotent
            and ordering comes from the `create_at` index.
        '''
        with self.backend.locked() as conn:
            staged = conn.execute(
                'SELECT raw FROM posts_staging WHERE channel_id=? ORDER BY create_at ASC',
                (self.channelId,)).fetchall()
        rawPosts = [json.loads(r['raw']) for r in staged]

        options = self.options
        takeEmojis = options.emojiMetadata or options.downloadEmoji
        users = self._resolveUsers(rawPosts)

        with self.backend.locked() as conn:
            with _writeTxn(conn):  # one atomic transaction; dimensions before facts (FK order)
                if self._team is not None and self._team.get('id'):
                    conn.execute(_TEAM_SQL, (self._team['id'], self._team.get('name'),
                        self._team.get('display_name'), _dumps(self._team)))
                conn.execute(_CHANNEL_SQL, (
                    self._channel['id'], self._channel.get('team_id'),
                    self._channel.get('name'), self._channel.get('display_name'),
                    self._channel.get('type'), self._channel.get('purpose'),
                    _dumps(self._channel)))
                conn.executemany(_USER_SQL, [
                    (u['id'], u.get('username'), u.get('nickname'), u.get('first_name'),
                     u.get('last_name'), u.get('email'),
                     _dumps(u) if len(u) > 1 else None)
                    for u in users])
                if takeEmojis:
                    conn.executemany(_EMOJI_SQL, self._emojiRows(rawPosts))
                conn.executemany(_POST_SQL, [self._postRow(p) for p in rawPosts])
                conn.executemany(_REACTION_SQL, self._reactionRows(rawPosts))
                if options.downloadAttachments:
                    conn.executemany(_ATTACHMENT_SQL, self._attachmentRows(rawPosts))
                conn.execute('DELETE FROM posts_staging WHERE channel_id=?', (self.channelId,))

        self._persistAssets(rawPosts, users)

    # ----- row mapping --------------------------------------------------------
    def _resolveUsers(self, rawPosts: List[dict]) -> List[dict]:
        '''
            Every post author needs a `users` row (the FK). Start from the seed
            users (rich), then resolve each remaining author via the driver; a
            failed lookup falls back to a minimal {id} row, which the COALESCE merge
            never lets clobber a real name learned elsewhere.
        '''
        byId: Dict[Id, dict] = {}
        for u in self._seedUsers:
            if u.get('id'):
                byId[u['id']] = u
        for p in rawPosts:
            uid = p.get('user_id')
            if not uid or uid in byId:
                continue
            try:
                byId[uid] = self.backend.services.getUserById(uid)
            except Exception:
                logging.warning(f"Unable to resolve author '{uid}'; storing a minimal user row.")
                byId[uid] = {'id': uid}
        return list(byId.values())

    def _postRow(self, p: dict) -> tuple:
        return (
            p['id'], self.channelId, p.get('user_id') or None,
            p.get('root_id') or None, p.get('type') or None, p.get('message') or '',
            p['create_at'], p.get('edit_at') or None, p.get('delete_at') or None,
            _dumps(p))

    def _reactionRows(self, rawPosts: List[dict]) -> List[tuple]:
        rows = []
        for p in rawPosts:
            for r in (p.get('metadata') or {}).get('reactions') or []:
                if not isinstance(r, dict):
                    continue
                rows.append((p['id'], r.get('user_id') or '', r.get('emoji_name') or '',
                             r.get('create_at')))
        return rows

    def _emojiRows(self, rawPosts: List[dict]) -> List[tuple]:
        rows, seen = [], set()
        for p in rawPosts:
            for e in (p.get('metadata') or {}).get('emojis') or []:
                eid = e.get('id') if isinstance(e, dict) else None
                if not eid or eid in seen:
                    continue
                seen.add(eid)
                rows.append((eid, e.get('name'), e.get('creator_id'), _dumps(e)))
        return rows

    def _attachmentRows(self, rawPosts: List[dict]) -> List[tuple]:
        rows, seen = [], set()
        for p in rawPosts:
            for f in (p.get('metadata') or {}).get('files') or []:
                fid = f.get('id') if isinstance(f, dict) else None
                if not fid or fid in seen:
                    continue
                seen.add(fid)
                rows.append((fid, p['id'], f.get('name'),
                             _attachmentExtension(f.get('name')), f.get('size'),
                             f.get('mime_type'), _dumps(f)))
        return rows

    # ----- assets -------------------------------------------------------------
    def _persistAssets(self, rawPosts: List[dict], users: List[dict]) -> None:
        options = self.options
        services = self.backend.services
        if options.downloadAttachments:
            self.backend.downloadBlobs('files', 'files', 'attachments', 'content',
                self._attachmentBlobIds(rawPosts), services.getFileUrl)
        if options.downloadEmoji and not self.backend.config.downloadAllEmojis:
            self.backend.downloadBlobs('emojis', 'emojis', 'emojis', 'image',
                self._emojiBlobIds(rawPosts), services.getEmojiUrl)
        if options.downloadAvatars:
            self.backend.downloadBlobs('user avatars', 'avatars', 'users', 'avatar',
                (u['id'] for u in users if u.get('id')), services.getAvatarUrl)

    def _attachmentBlobIds(self, rawPosts: List[dict]) -> List[str]:
        '''Attachment ids passing the size/type download filters.'''
        opts = self.options
        out, seen = [], set()
        for p in rawPosts:
            for f in (p.get('metadata') or {}).get('files') or []:
                fid = f.get('id') if isinstance(f, dict) else None
                if not fid or fid in seen:
                    continue
                seen.add(fid)
                size = f.get('size') or 0
                mime = f.get('mime_type')
                if opts.downloadAttachmentSizeLimit and size > opts.downloadAttachmentSizeLimit:
                    continue
                if opts.downloadAttachmentTypes and mime not in opts.downloadAttachmentTypes:
                    continue
                out.append(fid)
        return out

    def _emojiBlobIds(self, rawPosts: List[dict]) -> List[str]:
        out, seen = [], set()
        for p in rawPosts:
            for e in (p.get('metadata') or {}).get('emojis') or []:
                eid = e.get('id') if isinstance(e, dict) else None
                if eid and eid not in seen:
                    seen.add(eid)
                    out.append(eid)
        return out
