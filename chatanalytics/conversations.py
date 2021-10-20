# Ingest data and preprocess it
# TS | NRP
import json
import re
from os import walk
from os.path import isfile

import pandas as pd

pd.set_option('display.max_columns', None)


class _GenericChat:
    """Contains data from a chat with one or more people"""

    messageColumns = ["sender", "timestamp", "channel", "conversation", "source", "content"]
    messages = pd.DataFrame(columns=messageColumns)

    convoColumns = ["startMessage", "endMessage", "startTimestamp", "endTimestamp"]
    conversations = pd.DataFrame(columns=convoColumns)

    def load(self, path: str, _postProcess: bool = True) -> None:
        """Loads a single JSON message file

        :param path: the name of the file to load
        :param _postProcess: whether to postprocess data, default True
        :return: None
        """

        with open(path, "r", encoding='utf-8') as file:
            data = json.load(file)

        df = self._preProcess(data)
        self.messages = pd.concat([self.messages, df])

        # Easy enough to completely regroup
        if _postProcess:
            self._postProcess()

    def batchLoad(self, path: str, doWalk: bool = False, _postProcess: bool = True) -> None:
        """Load a directory of data files

        Lists or walks through the directory and import *all* files

        :param path: The path to the directory
        :param doWalk: whether to walk through the directory,
        :param _postProcess: whether to postprocess data, default True
        :return: None
        """
        if isfile(path):
            self.load(path)
            return

        for (dirpath, dirnames, filenames) in walk(path):
            for f in filenames:
                if "message" in f and ".json" in f:
                    self.load(dirpath + "/" + f, _postProcess=False)
            if not doWalk:
                break

        if _postProcess:
            self._postProcess()

    def clear(self) -> None:
        """Clears all messages in the conversation

        :return: None
        """
        self.messages = self.messages.iloc[0:0]

    def regroupAll(self) -> None:
        """Sorts and groups all messages

        :return: None
        """
        self._postProcess()

    ######################
    # Internal Functions #
    ######################

    def _preProcess(self, data: [dict, pd.DataFrame]):
        df = pd.DataFrame(data)

        if df.loc[0, "timestamp"] > df.loc[1, "timestamp"]:
            df = df.reindex(index=df.index[::-1])
        df = df.assign(source=None)
        df = df.assign(conversation=0)

        df = df.drop(columns=[col for col in df if col not in self.messageColumns])
        return df

    def _postProcess(self):
        self._sort()
        self._makeConversations()

    def _sort(self):
        self.messages = self.messages.sort_values("timestamp", ignore_index=True)

    def _makeConversations(self, df=None):
        if df is None:
            df = self.messages

        # Find all timing gaps of an hour or more
        gaps = (df.timestamp.diff() > pd.Timedelta(hours=1))
        # cumsum to compute conversation numbers
        #   (increments every time the limit expires)
        cumsum = gaps.cumsum()
        df["conversation"] = cumsum

        # Build conversation DataFrame (with numpy searchsorted)
        # Get every time a new conversation starts (time limit elapsed, gaps==True)
        changes = gaps[gaps].index.to_series().reset_index(drop=True)

        # Assign startMessage and endMessage, including start and end indices
        # Note: converting to lists is un-ideal but more readable than concatenation
        self.conversations["startMessage"] = [0] + changes.tolist()
        self.conversations["endMessage"] = (changes - 1).tolist() + [self.messages.last_valid_index()]

        # Start and end timestamps - shift truth table, index, consolidate
        starts = gaps
        starts[0] = True
        self.conversations["startTimestamp"] = self.messages.timestamp[starts].reset_index(drop=True)
        ends = gaps.shift(periods=-1, fill_value=True)
        self.conversations["endTimestamp"] = self.messages.timestamp[ends].reset_index(drop=True)


class MessengerChat(_GenericChat):
    """Contains data on Facebook Messenger chats"""

    pattern = re.compile(r"message_\d+\.json")

    def batchLoad(self, path: str, doWalk: bool = True, _postProcess: bool = True) -> None:
        """Load a directory of data files

        Lists or walks through the directory and import *all* files

        :param path: The path to the directory
        :param doWalk: whether to walk through the directory,
        :param _postProcess: whether to postprocess data, default True
        :return: None
        """
        if isfile(path):
            self.load(path)
            return

        for (dirpath, dirnames, filenames) in walk(path):
            for f in filenames:
                if self.pattern.match(f):
                    self.load(dirpath + "/" + f, _postProcess=False)
            if not doWalk:
                break

        if _postProcess:
            self._postProcess()

    ######################
    # Internal Functions #
    ######################

    def _preProcess(self, data):
        df = pd.DataFrame(data["messages"])
        df = df.rename(columns={"sender_name": "sender"})
        df = df.assign(channel=data["title"])

        # Remove extraneous messages
        df = df[
            (df['type'] == "Generic") &
            (df['is_unsent'] == False) &
            (~df['content'].isna())].copy()

        # Swap to using DateTimes
        df['timestamp'] = pd.to_datetime(df['timestamp_ms'], unit="ms") \
            .dt.tz_localize('UTC') \
            .dt.tz_convert('America/New_York')

        # Drop extra columns
        df = df.drop(columns=[col for col in df if col not in self.messageColumns])

        # Reindex and label
        if df.shape[0] > 1 and df.iloc[0].loc["timestamp"] > df.iloc[1].loc["timestamp"]:
            df = df.reindex(index=df.index[::-1])
        df = df.assign(source="Facebook Messenger")
        df = df.assign(conversation=0)
        return df


class DiscordChat(_GenericChat):
    """Contains data on Facebook Messenger chats"""

    def load(self, path: str, _postProcess: bool = True) -> None:
        """Loads data for a single channel from discord

        Loads messages.csv and channel.json files
        :param path: the directory of the file to load,
                        or path to either file
        :param _postProcess: whether to postprocess data, default True
        :return: None
        """

        if path.endswith("channel.json") or path.endswith("messages.csv"):
            path = path[:-12]

        with open(path + "/" + "channel.json", "r", encoding='utf-8') as file:
            channel = json.load(file)
        with open(path + "/" + "messages.csv", "r", encoding='utf-8') as file:
            messages = pd.read_csv(file)

        df = self._preProcess(channel, messages)
        self.messages = pd.concat([self.messages, df])

        # Easy enough to completely regroup
        if _postProcess:
            self._postProcess()

    def batchLoad(self, path: str, doWalk: bool = True, _postProcess: bool = True) -> None:
        """Load a directory of data files

        Lists or walks through the directory and import *all* files

        :param path: The path to the directory
        :param doWalk: whether to walk through the directory,
        :param _postProcess: whether to postprocess data, default True
        :return: None
        """
        if isfile(path):
            self.load(path)
            return

        for (dirpath, dirnames, filenames) in walk(path):
            if "messages.csv" in filenames and "channel.json" in filenames:
                self.load(dirpath, _postProcess=False)
            if not doWalk:
                break

        if _postProcess:
            self._postProcess()

    ######################
    # Internal Functions #
    ######################

    def _preProcess(self, channel, messages):
        df = pd.DataFrame(messages)
        df = df.rename(columns={"Contents": "content"})
        df = df.assign(sender="user")

        if "guild" in channel.keys():
            df = df.assign(channel=f"{channel['guild']['name']}: #{channel['name']}")
        elif "recipients" in channel.keys():
            df = df.assign(channel=", ".join(channel["recipients"]))
        else:
            df = df.assign(channel=channel["id"])

        # Remove extraneous messages
        df = df[
            (df['content'] != "") &
            (~df['content'].isna())].copy()

        # Swap to using DateTimes
        timestamps = pd.to_datetime(df['Timestamp'])
        if timestamps.dt.tz is None:
            timestamps = timestamps.dt.tz_localize('UTC')
        df["timestamp"] = timestamps.dt.tz_convert('America/New_York')

        # Drop extra columns
        df = df.drop(columns=[col for col in df if col not in self.messageColumns])

        # Reindex and label
        if df.shape[0] > 1 and df.iloc[0].loc["timestamp"] > df.iloc[1].loc["timestamp"]:
            df = df.reindex(index=df.index[::-1])
        df = df.assign(source="Discord")
        df = df.assign(conversation=0)
        return df

# todo support for non-text messages (content)
