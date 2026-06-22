'''
    Golden-shape tests for the on-disk post format (the lines written to
    <channel>.data.json). These pin the exact serialized schema so an accidental
    change to Post.fromMattermost / toStore -- e.g. starting to store channel_id, or
    renaming a field -- fails loudly. The stored shape is a stability contract:
    downstream consumers parse these files.
'''

import json
import unittest

from mattermost_dl.storage.directory_json.entities import Post

from .helpers import mmPost


# Every Mattermost post field that fromMattermost deliberately discards as redundant.
DROPPED_FIELDS = ('channel_id', 'reply_count', 'has_reactions', 'file_ids',
                  'hashtags', 'last_reply_at')


def _toStoreFallback(obj):
    # Matches the directory_json backend's serializer: nested toStore-able objects
    # (attachments, emojis, reactions) are serialized lazily by json.dump, not by
    # Post.toStore itself.
    if hasattr(obj, 'toStore'):
        return obj.toStore()
    return str(obj)


class StoredPostShapeTests(unittest.TestCase):
    def store(self, raw):
        '''The exact dict written as a line of <channel>.data.json: the post serialized
        the way the saver writes it (json.dump with a toStore fallback), parsed back.'''
        post = Post.fromMattermost(raw)
        return json.loads(json.dumps(post.toStore(), default=_toStoreFallback,
                                     ensure_ascii=False))

    def test_minimal_post_stored_shape(self):
        # The common case: a plain message. Note createTime is an int, and nothing
        # else (no channel_id, no empty lists) is emitted.
        stored = self.store(mmPost('p1', 1234, user_id='u1', message='hello'))
        self.assertEqual(stored, {
            'id': 'p1',
            'userId': 'u1',
            'createTime': 1234,
            'message': 'hello',
        })

    def test_full_post_stored_shape(self):
        # A richly-populated post pins every stored field name and the nested
        # emoji/attachment/reaction shapes, and that the dropped fields stay dropped.
        raw = {
            'id': 'p1',
            'channel_id': 'chan1',          # dropped
            'user_id': 'u1',
            'create_at': 1000,
            'update_at': 2000,              # -> updateTime
            'edit_at': 3000,                # -> publicUpdateTime
            'delete_at': 0,
            'message': 'hi',
            'is_pinned': True,              # -> isPinned
            'root_id': 'root1',             # -> rootPostId
            'parent_id': '',
            'type': 'system_join_channel',  # -> specialMsgType
            'props': {'addedUsername': 'bob',
                      'disable_group_highlight': True,  # filtered out of props
                      'channel_mentions': {}},          # filtered out of props
            'reply_count': 5,               # dropped
            'has_reactions': True,          # dropped
            'file_ids': ['f1'],             # dropped
            'hashtags': '#tag',             # dropped
            'last_reply_at': 4000,          # dropped
            'metadata': {
                'reactions': [{'user_id': 'u2', 'post_id': 'p1', 'emoji_name': 'tada',
                               'create_at': 1500, 'update_at': 1500, 'delete_at': 0}],
                'files': [{'id': 'f1', 'name': 'a.txt', 'size': 7, 'mime_type': 'text/plain',
                           'create_at': 900, 'update_at': 900, 'delete_at': 0,
                           'user_id': 'u1', 'post_id': 'p1', 'width': 0, 'height': 0,
                           'has_preview_image': False, 'mini_preview': None, 'extension': 'txt'}],
                'emojis': [{'id': 'e1', 'creator_id': 'u3', 'name': 'party',
                            'create_at': 800, 'update_at': 800, 'delete_at': 0}],
                'embeds': [{'type': 'link'}],   # ignored
                'images': {'http://x': {}},     # ignored
            },
        }
        self.assertEqual(self.store(raw), {
            'misc': {'props': {'addedUsername': 'bob'}},
            'id': 'p1',
            'userId': 'u1',
            'createTime': 1000,
            'message': 'hi',
            'updateTime': 2000,
            'publicUpdateTime': 3000,
            'isPinned': True,
            'rootPostId': 'root1',
            'specialMsgType': 'system_join_channel',
            'emojis': [{'id': 'e1', 'creatorId': 'u3', 'name': 'party', 'createTime': 800}],
            'attachments': [{'id': 'f1', 'name': 'a.txt', 'byteSize': 7,
                             'createTime': 900, 'mimeType': 'text/plain'}],
            'reactions': [{'userId': 'u2', 'createTime': 1500, 'emojiName': 'tada'}],
        })

    def test_dropped_fields_never_stored(self):
        raw = mmPost('p1', 1000)
        raw.update({k: v for k, v in zip(
            DROPPED_FIELDS, ('chan1', 5, True, ['f1'], '#x', 4000))})
        stored = self.store(raw)
        misc = stored.get('misc', {})
        for field in DROPPED_FIELDS:
            self.assertNotIn(field, stored, f'{field} leaked into the stored post')
            self.assertNotIn(field, misc, f'{field} leaked into the stored post misc')


if __name__ == '__main__':
    unittest.main()
