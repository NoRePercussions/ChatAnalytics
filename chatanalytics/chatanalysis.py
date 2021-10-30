# Manages analysis
# NRP 21

from . import utils

from datetime import timedelta


class ChatAnalysis:  # stored as GenericChat.analyze
    """Analyzes Chats"""

    def __init__(self, parent):
        self._parent = parent

    ################

    def messages_per_conversation(self, everything=False):
        messages = self._parent.messages
        conversations = self._parent.conversations

        diffs = conversations["endMessage"] - conversations["startMessage"]
        lengths = diffs + 1
        if not everything:
            return lengths
        else:
            return messages.assign(length=lengths)

    def mean_messages_per_conversation(self):
        lengths = self.messages_per_conversation()
        return lengths.mean()

    ################

    def conversation_durations(self, everything=False):
        messages = self._parent.messages
        conversations = self._parent.conversations

        diffs = conversations["endTimestamp"] - conversations["startTimestamp"]
        if not everything:
            return diffs
        else:
            return messages.assign(duration=diffs)

    def mean_conversation_duration(self):
        lengths = self.conversation_durations()

        # Exclude 0-length (1-message) "conversations"
        lengths = lengths[lengths != timedelta(0)]

        return lengths.mean()

    ################

    def day_of_messages(self, everything=False):
        messages = self._parent.messages
        conversations = self._parent.conversations

        days = messages.apply(lambda row: utils.get_last_day(row["timestamp"]), axis=1)
        if not everything:
            return days
        else:
            return messages.assign(day=days)

    def messages_per_day(self):
        days = self.day_of_messages()
        counts = days.value_counts()
        return counts

    def mean_messages_per_day(self):
        counts = self.messages_per_day()
        return counts.mean()

    ################

    def waking_day_of_messages(self, everything=False):
        messages = self._parent.messages
        conversations = self._parent.conversations

        days = messages.apply(lambda row: utils.get_last_waking_day(row["timestamp"]), axis=1)
        if not everything:
            return days
        else:
            return messages.assign(waking_day=days)

    def messages_per_waking_day(self):
        days = self.waking_day_of_messages()
        counts = days.value_counts()
        return counts

    def mean_messages_per_waking_day(self):
        counts = self.messages_per_waking_day()
        return counts.mean()

    ################

    def week_of_messages(self, everything=False):
        messages = self._parent.messages
        conversations = self._parent.conversations

        days = messages.apply(lambda row: utils.get_last_monday(row["timestamp"]), axis=1)
        if not everything:
            return days
        else:
            return messages.assign(last_monday=days)

    def messages_per_week(self):
        days = self.week_of_messages()
        counts = days.value_counts()
        return counts

    def mean_messages_per_week(self):
        counts = self.messages_per_week()
        return counts.mean()

    ################

    def characters_per_message(self):
        messages = self._parent.messages
        conversations = self._parent.conversations

        lengths = messages.content.str.len()
        return lengths

    def mean_characters_per_message(self):
        lengths = self.characters_per_message()
        return lengths.mean()

    def characters_per_conversation(self):
        messages = self._parent.messages
        conversations = self._parent.conversations

        group = messages.groupby('conversation')['content']
        return group.sum().str.len()

    def mean_characters_per_conversation(self):
        lengths = self.characters_per_conversation()
        return lengths.mean()

    def mean_characters_per_message_by_conversation(self):
        messages = self._parent.messages
        conversations = self._parent.conversations

        group = messages.groupby('conversation')['content']
        return group.sum().str.len() / group.count()

    ################

    def words_per_message(self):
        messages = self._parent.messages
        conversations = self._parent.conversations

        splits = messages.content.str.split()
        lengths = splits.str.len()  # str works for list operations?
        return lengths

    def mean_words_per_message(self):
        lengths = self.words_per_message()
        return lengths.mean()

    def words_per_conversation(self):
        messages = self._parent.messages
        conversations = self._parent.conversations

        group = messages.groupby('conversation')['content']
        joined = group.apply(lambda x: ' '.join(x))
        splits = joined.str.split()
        lengths = splits.str.len()
        return lengths

    def mean_words_per_conversation(self):
        lengths = self.words_per_conversation()
        return lengths.mean()

    def mean_words_per_message_by_conversation(self):
        messages = self._parent.messages
        conversations = self._parent.conversations

        group = messages.groupby('conversation')['content']
        joined = group.apply(lambda x: ' '.join(x))
        splits = joined.str.split()
        lengths = splits.str.len()
        return lengths / group.count()
