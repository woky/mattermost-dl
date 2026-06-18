'''
    Contains recovery scenario enumerators useful for arbitrage in situations
    where downloaded data may get lost.

    Sublogic of recovery module.
'''


class RecoveryAction:
    '''
        Subtypes describe general recovery strategies.
        For concrete meaning, see documentation of individual
        functions returning these.
    '''
    def __eq__(self, other: 'RecoveryAction') -> bool:
        return type(self) == type(other)

    # Defining __eq__ makes instances unhashable by default, which Python 3.11+
    # rejects when such an instance is used as a dataclass field default
    # (see config.py). Equal objects share a type, so hash by type.
    def __hash__(self) -> int:
        return hash(type(self))

class RSkipDownload(RecoveryAction):
    '''
        Download is not performed.
        This way, no space is wasted on (possibly redundant) backups.
    '''
    pass

class RDelete(RecoveryAction):
    '''Old archive is overriden.'''
    pass

class RBackup(RecoveryAction):
    '''Old archive is backed up.'''
    pass

class RReuse(RecoveryAction):
    '''
        Old archive is reused if possible.
        If not viable outright, fix is attempted,
        for example by rollbacking uncommited data.
    '''
    pass
