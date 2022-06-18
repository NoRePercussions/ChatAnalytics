import hashlib
import json
import os
from typing import List

import pandas as pd
import pytz_deprecation_shim
import tzlocal
from pandas.util import hash_pandas_object
from pytz import UnknownTimeZoneError

from .chatanalysis import ChatAnalysis
from .chatgraph import ChatGraph

pd.set_option('display.max_columns', None)


class Chat:
    """
    Contains data from a chat with one or more people

    Methods
    -------
    analyze:
        Analyzes
    load:
        Loads
    batch_load:
        Loads, but in batches
    clear:
        Clears
    set_timezone:
        Sets the timezone
    reset_timezone:
        Resets the timezone

    Attributes
    ----------
    conversations:
        The Pandas Dataframe of conversations
    messages:
        The Pandas Dataframe of messages
    """

    _message_columns = ["sender", "timestamp", "channel", "conversation", "source", "content"]
    _conversation_columns = ["startMessage", "endMessage", "start_timestamp", "end_timestamp"]

    _messages: pd.DataFrame
    _conversations: pd.DataFrame

    _analyze_backend: ChatAnalysis
    _graph_backend: ChatGraph

    _processed: bool
    _hash: int or None
    _timezone: str or pytz_deprecation_shim._impl__PytzShimTimezone
    _loaded_files: List[str]

    def __init__(self):
        self._messages = pd.DataFrame(columns=self._message_columns)
        self._conversations = pd.DataFrame(columns=self._conversation_columns)

        self._analyze_backend = ChatAnalysis(self)
        self.graph = ChatGraph(self)  # potentially awkward?

        self._processed = False
        self._hash = None
        self._timezone = self._get_localtime()
        self._loaded_files = []

    #############
    # Accessors #
    #############

    @property
    def messages(self):
        """The Pandas Dataframe of messages
        """
        if not self._processed:
            self._post_process()
        return self._messages

    @property
    def conversations(self):
        """The Pandas Dataframe of conversations"""
        if not self._processed:
            self._post_process()
        return self._conversations

    #######################
    # Public data methods #
    #######################

    def analyze(self, query):
        return self._analyze_backend.analyze(query)

    def load(self, path: str, allow_repeat_load: bool = True):
        """
        Loads data from a file or folder, automatically detecting
        if it is discord or messenger data

        Parameters
        ----------
        path : str
            The path to the file or directory to load
        allow_repeat_load : bool, optional
            Whether to allow loading the same file multiple times. Duplicate
            data will be removed in postprocessing.
            Defaults to True.

        Returns
        -------
        self : Chat
            Returns self with loaded data

        See Also
        --------
        batch_load : Loads all message data from a folder

        Notes
        -----
        This operation is in-place; it returns self to allow for functional chaining.
        This function should only really be used for manually structured data. To import
        pre-structured data, see classes such as MessengerChat or DiscordChat.

        Examples
        --------
        >>> chat = Chat()
        >>> chat.load("messages.json")
        """

        self._reset_cache()  # Altering data!

        # Todo: custom error types
        # Todo: restructure unit tests

        if self._type_is_messenger(path):
            if not allow_repeat_load and path in self._loaded_files:
                return self
            self._loaded_files += [os.path.abspath(path)]

            with open(path, "r", encoding='utf-8') as file:
                data = json.load(file)

            df = self._messenger_pre_process(data)
        elif parent := self._type_is_discord(path):
            if not allow_repeat_load and parent in self._loaded_files:
                return self
            self._loaded_files += [os.path.abspath(parent)]

            with open(parent + "/channel.json", "r", encoding='utf-8') as file:
                channel = json.load(file)
            with open(parent + "/messages.csv", "r", encoding='utf-8') as file:
                messages = pd.read_csv(file)

            df = self._discord_pre_process(channel, messages)
        else:
            raise FileNotFoundError("Cannot auto-type file")

        self._messages = pd.concat([self._messages, df])

        return self

    def batch_load(self, path: str, do_walk: bool = False):
        """Load a directory of data files

        Lists or walks through the directory and import *all* files

        :param path: The path to the directory
        :param do_walk: whether to walk through the directory,
        :param _post_process: whether to postprocess data, default True
        :return: None
        """

        self._reset_cache()  # Altering data!

        if os.path.isfile(path):
            self.load(path)
            return

        for (dirpath, dirnames, filenames) in os.walk(path):
            for f in filenames:
                if self._type_is_discord(f"{dirpath}/{f}") \
                        or self._type_is_discord(f"{dirpath}/{f}")\
                        or self._type_is_messenger(f"{dirpath}/{f}"):
                    self.load(dirpath + "/" + f, allow_repeat_load=False)
            if not do_walk:
                break

        return self

    def clear(self):
        """
        Clears all messages and conversations stored in the object

        Returns
        -------
        self : GenericChat
            Returns self with cleared messages

        See Also
        --------
        load : Loads data from a generic JSON chat file into self
        batch_load : Loads all message data from a folder

        Notes
        -----
        This operation is in-place; it returns self to allow for functional chaining.

        Examples
        --------
        >>> a = Chat()
        >>> a.load("messages.json")  # Loads everything in the messages/ folder
        >>> a.clear()
        >>> a.messages
        Empty Dataframe
        Columns: []
        Index: []
        """

        self._reset_cache()  # Altering data!

        self._messages = self._messages.iloc[0:0]

        return self

    def set_timezone(self, timezone=None):
        """Sets the timezone to use

        :param timezone: None, tz name (str), or tzlocal/pytz object"""
        self._reset_cache()

        if timezone is None:
            self._timezone = self._get_localtime()
        else:
            self._timezone = timezone

        if not self._messages.empty:
            self._messages['timestamp'] = self._messages['timestamp'].dt.tz_convert(self._timezone)
            self._conversations['start_timestamp'] = self._conversations['start_timestamp'].dt.tz_convert(self._timezone)
            self._conversations['end_timestamp'] = self._conversations['end_timestamp'].dt.tz_convert(self._timezone)

        return self

    def reset_timezone(self):
        """Sets the timezone to local time"""
        self.set_timezone(timezone=None)

        return self

    ######################
    # Internal Functions #
    ######################

    def _type_is_messenger(self, path: str) -> False or str:
        if not os.path.isfile(path):
            return False

        _, extension = os.path.splitext(path)
        if extension != ".json":
            return False

        with open(path, "r", encoding='utf-8') as file:
            data = json.load(file)
        if "magic_words" not in data:
            return False

        return path

    def _type_is_discord(self, path: str) -> False or str:
        if not os.path.isfile(path):
            if not os.path.isdir(path):
                return False
            if "messages.csv" and "channel.json" in os.listdir(path):
                return path

        if path.endswith("channel.json"):
            parent = os.path.dirname(path)
            if "messages.csv" in os.listdir(parent):
                return parent
        elif path.endswith("messages.csv"):
            parent = os.path.dirname(path)
            if "channel.json" in os.listdir(parent):
                return parent

        return False



    def _pre_process(self, data: [dict, pd.DataFrame]) -> pd.DataFrame:
        """Processes data before adding to data record

        Creates a dataframe, orders the data,
        sets source column, and removes extra columns

        :param data: dict or DataFrame with data
        :return: Dataframe of processed data"""
        self._reset_cache()  # Altering data!

        df = pd.DataFrame(data)

        if df.loc[0, "timestamp"] > df.loc[1, "timestamp"]:
            df = df.reindex(index=df.index[::-1])
        df = df.assign(source=None)
        df = df.assign(conversation=0)

        df = df.drop(columns=[col for col in df if col not in self._message_columns])
        return df

    def _messenger_pre_process(self, data: [dict, pd.DataFrame]) -> pd.DataFrame:
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
        df['timestamp'] = pd.to_datetime(df['timestamp_ms'], unit="ms", errors='coerce') \
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

    def _discord_pre_process(self, channel, messages) -> pd.DataFrame:
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
        timestamps = pd.to_datetime(df['Timestamp'], errors='coerce')
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

    def _post_process(self):
        """Processes entire data after adding to record

        Sorts all messages by timestamp and then groups conversations

        :return: None"""
        self._reset_cache()  # Altering data!

        self._messages = self._messages.sort_values("timestamp", ignore_index=True)
        self._messages = self._messages.drop_duplicates(ignore_index=True)

        self._make_conversations()

        self._processed = True

    def _make_conversations(self, df=None):
        """Groups messages into conversations

        Messages sent less than an hour apart to the same person
        count as the same conversation,
        then makes conversation dataframe

        :return: None"""
        self._reset_cache()  # Altering data!

        if df is None:
            df = self._messages

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
        self._conversations = self._conversations.iloc[0:0]
        # Assign startMessage and endMessage, including start and end indices
        # Note: converting to lists is un-ideal but more readable than concatenation
        self._conversations["startMessage"] = [0] + changes.tolist()
        self._conversations["endMessage"] = (changes - 1).tolist() + [self._messages.last_valid_index()]

        # Start and end timestamps - shift truth table, index, consolidate
        starts = gaps
        starts[0] = True
        self._conversations["start_timestamp"] = self._messages.timestamp[starts].reset_index(drop=True)
        ends = gaps.shift(periods=-1, fill_value=True)
        self._conversations["end_timestamp"] = self._messages.timestamp[ends].reset_index(drop=True)

    def _reset_cache(self):
        """Reset hash and internals if data changes"""
        self._hash = None
        self._processed = False

    @staticmethod
    def _get_localtime():
        # Unix may not support get_localzone_name,
        # but otherwise avoid pytz get_localzone
        try:
            return tzlocal.get_localzone_name()
        except UnknownTimeZoneError:
            return tzlocal.get_localzone()

    #####################
    # Special Functions #
    #####################

    def __eq__(self, other):
        if not isinstance(other, Chat):
            # don't attempt to compare against unrelated types
            return NotImplemented
        return (self.messages.equals(other.messages)
                and self.conversations.equals(other.conversations))

    def __hash__(self):
        if self._hash is None:
            self._hash = 0
            self._hash += hash_pandas_object(self.messages, index=True).sum()
            self._hash += hash_pandas_object(self.conversations, index=True).sum()
            self._hash %= 2**64
        return self._hash
