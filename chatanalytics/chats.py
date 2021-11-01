# Ingest data and preprocess it
# TS | NRP
import json
import re
from os import walk
from os.path import isfile

import pandas as pd
from pandas.util import hash_pandas_object
from tzlocal import get_localzone, get_localzone_name
from pytz import UnknownTimeZoneError
import functools

from .chatanalysis import ChatAnalysis
from .chatgraph import ChatGraph

pd.set_option('display.max_columns', None)


class GenericChat:
    """Contains data from a chat with one or more people"""

    _message_columns = ["sender", "timestamp", "channel", "conversation", "source", "content"]
    _conversation_columns = ["startMessage", "endMessage", "start_timestamp", "end_timestamp"]

    def __init__(self):
        self.messages = pd.DataFrame(columns=self._message_columns)
        self.conversations = pd.DataFrame(columns=self._conversation_columns)
        self.hash = None
        self._timezone = self._get_localtime()

        self.analyze = ChatAnalysis(self)
        self.graph = ChatGraph(self)

    def load(self, path: str, _post_process: bool = True) -> None:
        """Loads a single JSON message file

        :param path: the name of the file to load
        :param _post_process: whether to postprocess data, default True
        :return: None
        """

        self._reset_hash()  # Altering data!

        with open(path, "r", encoding='utf-8') as file:
            data = json.load(file)

        df = self._pre_process(data)
        self.messages = pd.concat([self.messages, df])

        # Easy enough to completely regroup
        if _post_process:
            self._post_process()

    def batch_load(self, path: str, do_walk: bool = False, _post_process: bool = True) -> None:
        """Load a directory of data files

        Lists or walks through the directory and import *all* files

        :param path: The path to the directory
        :param do_walk: whether to walk through the directory,
        :param _post_process: whether to postprocess data, default True
        :return: None
        """

        self._reset_hash()  # Altering data!

        if isfile(path):
            self.load(path)
            return

        for (dirpath, dirnames, filenames) in walk(path):
            for f in filenames:
                if "message" in f and ".json" in f:
                    self.load(dirpath + "/" + f, _post_process=False)
            if not do_walk:
                break

        if _post_process:
            self._post_process()

    def clear(self) -> None:
        """Clears all messages in the conversation

        :return: None
        """

        self._reset_hash()  # Altering data!

        self.messages = self.messages.iloc[0:0]

    def regroup_all(self) -> None:
        """Sorts and groups all messages

        :return: None
        """
        self._post_process()

    def set_timezone(self, timezone=None):
        """Sets the timezone to use

        :param timezone: None, tz name (str), or tzlocal/pytz object"""
        self._reset_hash()

        if timezone is None:
            self._timezone = self._get_localtime()
        else:
            self._timezone = timezone

        if not self.messages.empty:
            self.messages['timestamp'] = self.messages['timestamp'].dt.tz_convert(self._timezone)
            self.conversations['start_timestamp'] = self.conversations['start_timestamp'].dt.tz_convert(self._timezone)
            self.conversations['end_timestamp'] = self.conversations['end_timestamp'].dt.tz_convert(self._timezone)

    def reset_timezone(self):
        """Sets the timezone to local time"""
        self.set_timezone(timezone=None)

    ######################
    # Internal Functions #
    ######################

    def _pre_process(self, data: [dict, pd.DataFrame]):
        """Processes data before adding to data record

        Creates a dataframe, orders the data,
        sets source column, and removes extra columns

        :param data: dict or DataFrame with data
        :return: Dataframe of processed data"""
        self._reset_hash()  # Altering data!

        df = pd.DataFrame(data)

        if df.loc[0, "timestamp"] > df.loc[1, "timestamp"]:
            df = df.reindex(index=df.index[::-1])
        df = df.assign(source=None)
        df = df.assign(conversation=0)

        df = df.drop(columns=[col for col in df if col not in self._message_columns])
        return df

    def _post_process(self):
        """Processes entire data after adding to record

        Sorts all messages by timestamp and then groups conversations

        :return: None"""
        self._reset_hash()  # Altering data!

        self._sort()
        self._make_conversations()

    def _sort(self):
        """Sorts all messages by timestamp

        :return: None"""
        self._reset_hash()  # Altering data!

        self.messages = self.messages.sort_values("timestamp", ignore_index=True)

    def _make_conversations(self, df=None):
        """Groups messages into conversations

        Messages sent less than an hour apart to the same person
        count as the same conversation,
        then makes conversation dataframe

        :return: None"""
        self._reset_hash()  # Altering data!

        if df is None:
            df = self.messages

        # Find all timing gaps of an hour or more
        gaps = (df.timestamp.diff() > pd.Timedelta(hours=1)) | (~df.channel.eq(df.channel.shift()))
        gaps[0] = False
        # cumsum to compute conversation numbers
        #   (increments every time the limit expires)
        cumsum = gaps.cumsum()
        df["conversation"] = cumsum

        # Build conversation DataFrame (with numpy searchsorted)
        # Get every time a new conversation starts (time limit elapsed, gaps==True)
        changes = gaps[gaps].index.to_series().reset_index(drop=True)

        # Reset conversations so previous setup disappears
        self.conversations = self.conversations.iloc[0:0]
        # Assign startMessage and endMessage, including start and end indices
        # Note: converting to lists is un-ideal but more readable than concatenation
        self.conversations["startMessage"] = [0] + changes.tolist()
        self.conversations["endMessage"] = (changes - 1).tolist() + [self.messages.last_valid_index()]

        # Start and end timestamps - shift truth table, index, consolidate
        starts = gaps
        starts[0] = True
        self.conversations["start_timestamp"] = self.messages.timestamp[starts].reset_index(drop=True)
        ends = gaps.shift(periods=-1, fill_value=True)
        self.conversations["end_timestamp"] = self.messages.timestamp[ends].reset_index(drop=True)

    def _reset_hash(self):
        """Reset hash if data changes"""
        self.hash = None

    @staticmethod
    def _get_localtime():
        # Unix may not support get_localzone_name,
        # but otherwise avoid pytz get_localzone
        try:
            return get_localzone_name()
        except UnknownTimeZoneError:
            return get_localzone()

    #####################
    # Special Functions #
    #####################

    def __eq__(self, other):
        if not isinstance(other, GenericChat):
            # don't attempt to compare against unrelated types
            return NotImplemented
        return (self.messages.equals(other.messages)
                and self.conversations.equals(other.conversations))

    def __hash__(self):
        if self.hash is None:
            messages_hash = hash_pandas_object(self.messages).sum()  # unordered hashes
            conversations_hash = hash_pandas_object(self.conversations).sum()
            self.hash = hash(str(messages_hash) + str(conversations_hash))
        return self.hash


class MessengerChat(GenericChat):
    """Contains data on Facebook Messenger chats"""

    pattern = re.compile(r"message_\d+\.json")

    def batch_load(self, path: str, do_walk: bool = True, _post_process: bool = True) -> None:
        """Load a directory of data files

        Lists or walks through the directory and import *all* files

        :param path: The path to the directory
        :param do_walk: whether to walk through the directory,
        :param _post_process: whether to postprocess data, default True
        :return: None
        """
        if isfile(path):
            self.load(path)
            return

        for (dirpath, dirnames, filenames) in walk(path):
            for f in filenames:
                if self.pattern.match(f):
                    self.load(dirpath + "/" + f, _post_process=False)
            if not do_walk:
                break

        if _post_process:
            self._post_process()

    ######################
    # Internal Functions #
    ######################

    def _pre_process(self, data):
        """Processes data before adding to data record

        Creates a dataframe, orders the data,
        sets source column, and removes extra columns

        :param data: dict or DataFrame with data
        :return: Dataframe of processed data"""
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
            .dt.tz_convert(self._timezone)

        # Drop extra columns
        df = df.drop(columns=[col for col in df if col not in self._message_columns])

        # Reindex and label
        if df.shape[0] > 1 and df.iloc[0].loc["timestamp"] > df.iloc[1].loc["timestamp"]:
            df = df.reindex(index=df.index[::-1])
        df = df.assign(source="Facebook Messenger")
        df = df.assign(conversation=0)
        return df


class DiscordChat(GenericChat):
    """Contains data on Discord chats"""

    def load(self, path: str, _post_process: bool = True) -> None:
        """Loads data for a single channel from discord

        Loads messages.csv and channel.json files
        :param path: the directory of the file to load,
                        or path to either file
        :param _post_process: whether to postprocess data, default True
        :return: None
        """

        if path.endswith("channel.json") or path.endswith("messages.csv"):
            path = path[:-12]

        with open(path + "/" + "channel.json", "r", encoding='utf-8') as file:
            channel = json.load(file)
        with open(path + "/" + "messages.csv", "r", encoding='utf-8') as file:
            messages = pd.read_csv(file)

        df = self._pre_process(channel, messages)
        self.messages = pd.concat([self.messages, df])

        # Easy enough to completely regroup
        if _post_process:
            self._post_process()

    def batch_load(self, path: str, do_walk: bool = True, _post_process: bool = True) -> None:
        """Load a directory of data files

        Lists or walks through the directory and import *all* files

        :param path: The path to the directory
        :param do_walk: whether to walk through the directory,
        :param _post_process: whether to postprocess data, default True
        :return: None
        """
        if isfile(path):
            self.load(path)
            return

        for (dirpath, dirnames, filenames) in walk(path):
            if "messages.csv" in filenames and "channel.json" in filenames:
                self.load(dirpath, _post_process=False)
            if not do_walk:
                break

        if _post_process:
            self._post_process()

    ######################
    # Internal Functions #
    ######################

    def _pre_process(self, channel, messages):
        """Processes data before adding to data record

        Creates a dataframe, orders the data,
        sets source column, and removes extra columns

        :param channel: Channel information
        :param messages: Message data
        :return: Dataframe of processed data"""
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
        df["timestamp"] = timestamps.dt.tz_convert(self._timezone)

        # Drop extra columns
        df = df.drop(columns=[col for col in df if col not in self._message_columns])

        # Reindex and label
        if df.shape[0] > 1 and df.iloc[0].loc["timestamp"] > df.iloc[1].loc["timestamp"]:
            df = df.reindex(index=df.index[::-1])
        df = df.assign(source="Discord")
        df = df.assign(conversation=0)
        return df
