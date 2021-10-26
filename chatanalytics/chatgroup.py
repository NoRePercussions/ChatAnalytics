# Ingest data and preprocess it
# TS | NRP

from os import listdir
from os.path import isfile, isdir, join

import pandas as pd
#from .chats import GenericChat, MessengerChat, DiscordChat
pd.set_option('display.max_columns', None)


class ChatGroup:
    """A group of Chats from conversations"""

    def __init__(self):
        self.chats = []

    def load(self, chatType, path):
        """Loads a chat of certain type

        Ex. load(DiscordChat, filePath)
        :param chatType: type of chat to load
        :param path: the name of the file to load
        :return:
        """
        newChat = chatType()
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
            # use batchLoad to walk the directory --
            # maybe give it a different name, or make
            # a function that walks directories?
            newChat.batch_load(f"{path}/{f}", do_walk=True)
            self.chats += [newChat]

#    def __eq__(self, other):
#
