# Ingest data and preprocess it
# TS | NRP

from os import listdir
from os.path import isfile, isdir, join
from collections import Counter

import pandas as pd
from tzlocal import get_localzone_name
#from .chats import GenericChat, MessengerChat, DiscordChat
pd.set_option('display.max_columns', None)


class ChatGroup:
    """A group of Chats from conversations"""

    def __init__(self):
        self.chats = []
        self._timezone = get_localzone_name()

    def load(self, chatType, path):
        """Loads a chat of certain type

        Ex. load(DiscordChat, filePath)
        :param chatType: type of chat to load
        :param path: the name of the file to load
        :return:
        """
        newChat = chatType()
        newChat.set_timezone(self._timezone)
        # use batchLoad to walk the directory --
        # maybe give it a different name, or make
        # a function that walks directories?
        newChat.batch_load(path, do_walk=True)
        self.chats += [newChat]

    def batch_load(self, chatType, path):
        """Loads a batch of chats of certain type

        Ex. batchLoad(DiscordChat, dirPath)
        :param chatType: type of chat to load
        :param path: the directory to load from
        :return:
        """
        if not isdir(path):
            self.load(chatType, path)
            return

        for f in listdir(path):
            if not isdir(f"{path}/{f}"):
                continue

            newChat = chatType()
            newChat.set_timezone(self._timezone)
            # use batchLoad to walk the directory --
            # maybe give it a different name, or make
            # a function that walks directories?
            newChat.batch_load(f"{path}/{f}", do_walk=True)
            self.chats += [newChat]

    def set_timezone(self, tz=None):
        if tz is None:
            self._timezone = get_localzone_name()
        else:
            self._timezone = tz

        for chat in self.chats:
            chat.set_timezone(self._timezone)

    def reset_timezone(self):
        self.set_timezone()

    def __eq__(self, other):
        """Test equality by comparing hashes"""
        return Counter(self.chats) == Counter(other.chats)
