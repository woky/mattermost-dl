'''
    Schema definition and migrations for the SQLite storage backend.

    Versioning uses SQLite's native ``PRAGMA user_version`` as the counter plus an
    ordered ``MIGRATIONS`` list applied transactionally on connect -- the canonical
    dependency-free pattern for an embedded single-file SQLite app. To extend the
    schema later (e.g. to enable sqlite-zstd transparent compression on the ``raw``
    columns), append a new ``(version, sql)`` step; it is applied incrementally to
    older databases with no re-download.

    Every table carries a real rowid (no ``WITHOUT ROWID``) and a plain ``raw``
    column so sqlite-zstd can later compress those columns with no schema change.

    Some derived data is maintained by triggers on ``posts`` rather than recomputed at
    query time: the FTS index (``posts_ai``/``au``/``ad``) and, since v3, each post's
    thread position and size (``posts_thread_ai``/``au``/``ad`` feeding the ``threads``
    and ``post_threads`` side tables) so a consumer can render "3/34" with a couple of
    PK joins instead of a per-row correlated COUNT.
'''

import sqlite3
from typing import List, Tuple

# Star schema (v1): dimensions deduped by id via upsert, fact table `posts`, a
# durable per-channel staging table for resume, and an external-content FTS5 index
# kept in sync by triggers. Asset bytes live inline as deduped BLOBs (avatar /
# image / content), filled by the asset phase, not the metadata commit.
_V1 = '''
CREATE TABLE teams (
    id TEXT PRIMARY KEY, name TEXT, display_name TEXT, raw TEXT NOT NULL
);
CREATE TABLE channels (
    id TEXT PRIMARY KEY, team_id TEXT, name TEXT, display_name TEXT,
    type TEXT, purpose TEXT, raw TEXT NOT NULL
);
CREATE TABLE users (
    id TEXT PRIMARY KEY, username TEXT, nickname TEXT, first_name TEXT,
    last_name TEXT, email TEXT, raw TEXT, avatar BLOB
);  -- raw NULL for minimal author rows; avatar filled when downloadAvatars
CREATE TABLE emojis (
    id TEXT PRIMARY KEY, name TEXT, creator_id TEXT, raw TEXT, image BLOB
);  -- custom-emoji dim, deduped; image filled when downloadEmoji
CREATE TABLE posts (
    id TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL REFERENCES channels(id),
    user_id TEXT REFERENCES users(id),
    root_id TEXT, type TEXT, message TEXT,
    create_at INTEGER NOT NULL, edit_at INTEGER, delete_at INTEGER,
    raw TEXT NOT NULL
);
CREATE INDEX posts_channel_idx ON posts(channel_id, create_at);
CREATE INDEX posts_root_idx ON posts(root_id);
CREATE INDEX posts_user_idx ON posts(user_id);
CREATE TABLE reactions (
    post_id TEXT NOT NULL REFERENCES posts(id),
    user_id TEXT NOT NULL DEFAULT '',
    emoji_name TEXT NOT NULL DEFAULT '',
    create_at INTEGER,
    PRIMARY KEY (post_id, user_id, emoji_name)
);
CREATE TABLE attachments (
    id TEXT PRIMARY KEY,
    post_id TEXT NOT NULL REFERENCES posts(id),
    name TEXT, extension TEXT, size INTEGER, mime_type TEXT,
    raw TEXT NOT NULL, content BLOB
);  -- metadata deduped by id; content filled when it passes the download filters
CREATE INDEX attachments_post_idx ON attachments(post_id);
-- durable newest->oldest download buffer; survives interruption for resume
CREATE TABLE posts_staging (
    channel_id TEXT NOT NULL, id TEXT NOT NULL,
    create_at INTEGER NOT NULL, raw TEXT NOT NULL,
    PRIMARY KEY (channel_id, id)
);
CREATE INDEX posts_staging_chan_idx ON posts_staging(channel_id, create_at);
-- FTS5 external-content over post text, kept in sync by triggers (porter+unicode61)
CREATE VIRTUAL TABLE posts_fts USING fts5(
    message, content='posts', content_rowid='rowid', tokenize='porter unicode61'
);
CREATE TRIGGER posts_ai AFTER INSERT ON posts BEGIN
    INSERT INTO posts_fts(rowid, message) VALUES (new.rowid, new.message);
END;
CREATE TRIGGER posts_ad AFTER DELETE ON posts BEGIN
    INSERT INTO posts_fts(posts_fts, rowid, message) VALUES ('delete', old.rowid, old.message);
END;
CREATE TRIGGER posts_au AFTER UPDATE ON posts BEGIN
    INSERT INTO posts_fts(posts_fts, rowid, message) VALUES ('delete', old.rowid, old.message);
    INSERT INTO posts_fts(rowid, message) VALUES (new.rowid, new.message);
END;
'''

# v2: indexes for the no-query "posts of a user, optionally in a given channel" browse -- the
# only browse path that hits the posts b-tree indexes. The FTS-driven search path never
# consults them: it drives from posts_fts and applies channel/user/date as residual filters.
# Widening posts_user_idx to (user_id, create_at) lets a by-author browse read newest-first
# straight from the index instead of sorting, and covers user_id + date ranges. The
# (user_id, channel_id, create_at) composite extends that to "that user's posts in one
# channel": both equalities are matched and create_at stays last, so the pair is still read
# newest-first without a sort. No channel-leading or date-only index is added -- those would
# serve browse modes outside this workload.
_V2 = '''
DROP INDEX posts_user_idx;
CREATE INDEX posts_user_idx ON posts(user_id, create_at);
CREATE INDEX posts_user_channel_idx ON posts(user_id, channel_id, create_at);
'''

# v3: precomputed thread position/size, so a consumer can render "3/34" (3rd post in a
# 34-post thread) without a per-row correlated COUNT at query time. A thread is flat: a
# root post (root_id NULL) plus every post sharing that root_id, ordered by create_at.
# Two slim side tables keep the posts row and the FTS index untouched: `threads` holds
# one size per thread root, `post_threads` one position per post. They are maintained by
# an insert/update/delete trigger trio that mirrors the FTS posts_ai/au/ad set, plus a
# one-time set-based backfill of any pre-existing posts. All archived posts count,
# including deleted ones (delete_at>0). See `backend.py`'s commit() for the insert order
# the AI trigger's index relies on.
_V3 = '''
CREATE TABLE threads (
    root_id TEXT PRIMARY KEY, size INTEGER NOT NULL
);  -- keyed by thread root (a reply's root_id even when its root isn't archived); not a FK
CREATE TABLE post_threads (
    post_id TEXT PRIMARY KEY REFERENCES posts(id),
    thread_root TEXT NOT NULL, thread_index INTEGER NOT NULL
);  -- one row per post; a standalone post is a thread of size 1 at index 1
CREATE INDEX post_threads_root_idx ON post_threads(thread_root);

-- AI: bump (or seed) the thread size, then read it back as this post's index. The index
-- is correct because posts are inserted in non-decreasing create_at order within a
-- thread (commit() drains staging ORDER BY create_at ASC, and incremental runs only add
-- newer posts -- a reply's create_at always exceeds its root's), so the new post is
-- always the thread's newest member and its index is the post-increment size. A re-run
-- of an existing post takes the upsert's UPDATE path (posts_thread_au), not this one, so
-- it never double-counts.
CREATE TRIGGER posts_thread_ai AFTER INSERT ON posts BEGIN
    INSERT INTO threads(root_id, size)
        VALUES (coalesce(new.root_id, new.id), 1)
        ON CONFLICT(root_id) DO UPDATE SET size = size + 1;
    INSERT INTO post_threads(post_id, thread_root, thread_index)
        VALUES (new.id, coalesce(new.root_id, new.id),
                (SELECT size FROM threads WHERE root_id = coalesce(new.root_id, new.id)));
END;
-- AU: fires only when a post is actually re-homed to another thread. A normal re-upsert
-- rewrites root_id to the same value, so the WHEN guard makes it a no-op (both operands
-- of <> are non-null -- coalesce backs root_id with the NOT NULL id). Re-homing does not
-- happen in Mattermost; kept for symmetry with the FTS trigger set. The moved post
-- leaves its old thread and rejoins the new one as its newest member.
CREATE TRIGGER posts_thread_au AFTER UPDATE ON posts
WHEN coalesce(new.root_id, new.id) <> coalesce(old.root_id, old.id)
BEGIN
    UPDATE threads SET size = size - 1 WHERE root_id = coalesce(old.root_id, old.id);
    DELETE FROM post_threads WHERE post_id = old.id;
    INSERT INTO threads(root_id, size)
        VALUES (coalesce(new.root_id, new.id), 1)
        ON CONFLICT(root_id) DO UPDATE SET size = size + 1;
    INSERT INTO post_threads(post_id, thread_root, thread_index)
        VALUES (new.id, coalesce(new.root_id, new.id),
                (SELECT size FROM threads WHERE root_id = coalesce(new.root_id, new.id)));
END;
-- AD: defensive insurance, like the FTS posts_ad. The archive never deletes posts, so
-- this never fires in practice; if it did it would decrement the size and drop the row,
-- leaving an index gap (renumbering is not trigger-friendly) -- acceptable precisely
-- because it does not occur.
CREATE TRIGGER posts_thread_ad AFTER DELETE ON posts BEGIN
    UPDATE threads SET size = size - 1 WHERE root_id = coalesce(old.root_id, old.id);
    DELETE FROM post_threads WHERE post_id = old.id;
END;

-- One-time backfill of pre-existing posts (set-based; inserts into the new tables
-- directly, so the per-row posts triggers above never fire). No-op on a fresh database.
INSERT INTO post_threads(post_id, thread_root, thread_index)
SELECT id, coalesce(root_id, id),
       row_number() OVER (PARTITION BY coalesce(root_id, id) ORDER BY create_at, id)
FROM posts;
INSERT INTO threads(root_id, size)
SELECT coalesce(root_id, id), count(*) FROM posts GROUP BY coalesce(root_id, id);
'''

# Ordered list of (version, DDL). Append new steps; never edit a shipped one.
MIGRATIONS: List[Tuple[int, str]] = [
    (1, _V1),
    (2, _V2),
    (3, _V3),
]

LATEST_VERSION = MIGRATIONS[-1][0]


def applyMigrations(conn: sqlite3.Connection) -> None:
    '''
        Bring `conn`'s schema up to `LATEST_VERSION`, idempotently.

        Reads ``PRAGMA user_version`` and applies each newer migration step inside
        its own transaction, bumping the version. A no-op (one PRAGMA read) when the
        database is already current; schema-creating from an empty database
        (user_version=0); incremental on an older one.
    '''
    version = conn.execute('PRAGMA user_version').fetchone()[0]
    for ver, sql in MIGRATIONS:
        if ver <= version:
            continue
        # executescript commits any pending transaction first, then runs the script
        # as written; wrapping it in BEGIN/COMMIT makes the whole step atomic, and
        # the user_version bump rolls back with it on failure.
        try:
            conn.executescript(f'BEGIN;\n{sql}\nPRAGMA user_version = {ver};\nCOMMIT;')
        except Exception:
            conn.executescript('ROLLBACK;')
            raise
        version = ver
