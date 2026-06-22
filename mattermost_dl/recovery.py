'''
    Contains logic pertaining to arbitrage in situations
    where downloaded data may get lost.
'''

from .common import *

from .config import ChannelOptions, ConfigFile
from .recovery_actions import RBackup, RDelete, RReuse, RSkipDownload

class RecoveryArbiter:
    '''
        Decision maker that centralises reasoning in all situations
        that may result in data loss.

        Acts as an interface that subclasses may use to, for example, ask user
        for decision interactively.
    '''
    def __init__(self, config: ConfigFile) -> None:
        self.config = config

    def onArchiveReuse(self, options: ChannelOptions, reusable: bool) -> Union[RBackup, RDelete, RReuse, RSkipDownload]:
        '''
            Decides how to handle previous channel archive that was downloaded already should be appended into or downloaded from scratch altogether.

            @param reusable True if archive storage is viable for updating (appending)

            @returns either
                - RBackup - stores new file from scratch, backups previous
                - RReuse - if reusable, appends previous content, otherwise start new one from scratch, but keep previous in case of rollback
                - RDelete - stores new file from scratch, deletes previous
                - RSkipDownload - aborts downloading given channel
        '''
        if reusable:
            return options.onExistingCompatibleArchive
        else:
            if options.onExistingIncompatibleArchive == RDelete():
                return RReuse()
            else:
                return options.onExistingIncompatibleArchive

    def onExistingChannelBackup(self, channel: dict, headerFilename: Path, dataFilename: Path) -> Union[RBackup, RDelete, RSkipDownload]:
        '''
            Called if backup creation is requested and its primary backup already exists.

            `channel` is the raw channel API reply; the path arguments are
            backend-specific hints (the directory backend's backup file names).

            @returns either
                - RBackup - old backup is retained under different name
                - RDelete - old backup is overriden
                - RSkipDownload - aborts downloading given channel
        '''
        logging.warning(
            f"Can't backup archive for '{channel['name']}', as previous backup exist. Previous backup will be renamed."
        )
        return RBackup()
