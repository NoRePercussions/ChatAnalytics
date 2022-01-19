# Manages analysis
# NRP 21
import warnings

import pandas as pd

from . import utils, autocorrect

from datetime import timedelta
import re
from logging import debug


class ChatAnalysis:  # stored as GenericChat.analyze
    """Analyzes Chats

    1) Take a query and parse it into:
        - <target> per <groups>
        - <operation> of <target> per <groups> sorted by <groups>
    2) Validate all components
    3) Group by final groups
    4) Aggregate by initial groups, applying operation to target
    """

    # https://regex101.com/r/NmimVo/5/
    simple_query = re.compile(r"^([a-zA-Z]+?) (?:per|by|sorted by) ([a-zA-Z]+?(?:(?: per| by|,|, and| and) [a-zA-Z]+" +
                              r"?)*?)$")
    # https://regex101.com/r/tirsTV/4/
    complex_query = re.compile(r"^(?:([a-zA-Z]+) |([a-zA-Z]+(?: [a-zA-Z]+)*) of )([a-zA-Z]+) (?:per|by) ([a-zA-Z]+)" +
                               r"(?: (?:per|by|sorted by) ([a-zA-Z]+(?:(?:, |, and| and)[a-zA-Z]+)*))?$")
    # https://regex101.com/r/81gpcU/2/
    decomposer = re.compile(r" by |, |, and | per | and | ")

    def __init__(self, parent):
        self._parent = parent
        self._initialize_internals()

    def _initialize_internals(self):
        self.ops = {
            "mean": self._operator_mean,
            "median": self._operator_median,
            "mode": self._operator_mode,
            "range": self._operator_range,
            "stdev": self._operator_stdev,
            "total": self._operator_total,
            "max": self._operator_max,
            "min": self._operator_min,
        }
        self.op_subs = self._invert_dict({
            "mean": ["average"],
            "median": ["medial"],
            "mode": ["modal"],
            "range": [],
            "stdev": ["standard deviation", "std dev", "std"],
            "total": ["sum"],
            "max": ["maximum"],
            "min": ["minimum"]
        })

        self.targets = {
            "message": self._target_message,
            "conversation": self._target_conversation,
            "word": self._target_word,
            "character": self._target_character,
            "duration": self._target_duration
        }
        self.target_subs = self._invert_dict({
            "message": ["messages", "msg", "msgs"],
            "conversation": ["conversations", "conv", "convs", "convo", "convos"],
            "word": ["words", "wd", "wds"],
            "character": ["characters", "char", "chars"],
            "duration": ["time", "length"]
        })

        self.groups = {
            "message": self._group_pre_message,
            "conversation": self._group_pre_conversation,
            "day": self._group_pre_day,
            "week": self._group_pre_week,
            "month": self._group_pre_month,
            "year": self._group_pre_year,
            "sender": self._group_pre_sender
        }
        self.group_subs = self._invert_dict({
            "message": ["messages", "msg", "msgs"],
            "conversation": ["conversations", "conv", "convs"],
            "day": [],
            "week": ["wk"],
            "month": ["mo"],
            "year": ["yr"],
            "sender": ["person"]
        })

    @staticmethod
    def _invert_dict(dictionary):
        new_dict = {}
        for key, vals in dictionary.items():
            for v in vals:
                new_dict[v] = key
        return new_dict

    ################

    def __call__(self, query, *args, **kwargs):
        dist, query = autocorrect.correct_passage(query)
        if dist > 0:
            warnings.warn(f"\nQuery corrected to: '{query}'")
        args = self._parse_query(query)
        return self._execute_query(*args)

    ####################
    # Internal methods #
    ####################

    def _parse_query(self, query):
        if match := self.simple_query.match(query):
            operation = None
            target = match.group(1)
            igroup = None
            fgroups = self._decompose(match.group(2))
        elif match := self.complex_query.match(query):
            operation = match.group(1) or match.group(2)
            target = match.group(3)
            igroup = match.group(4)
            fgroups = self._decompose(match.group(5))
        else:
            raise ValueError(f"Query '{query}' is invalid")

        operation = self._validate_ops(operation)
        target = self._validate_target(target)
        igroup = self._validate_group(igroup)
        fgroups = self._validate_groups(fgroups)

        return operation, target, igroup, fgroups

    def _decompose(self, query):
        if isinstance(query, str):
            return self.decomposer.split(query)
        return None

    def _validate_ops(self, op):
        if op is None:
            return None
        elif op in self.ops:
            return op
        elif op in self.op_subs:
            return self.op_subs[op]
        else:
            raise ValueError(f"Operation '{op}' is invalid")

    def _validate_target(self, target):
        if target is None:
            return None
        elif target in self.targets:
            return target
        elif target in self.target_subs:
            return self.target_subs[target]
        else:
            raise ValueError(f"Target '{target}' is invalid")

    def _validate_group(self, group):
        if group is None:
            return None
        elif group in self.groups:
            return group
        elif group in self.group_subs:
            return self.group_subs[group]
        else:
            raise ValueError(f"Group '{group}' is invalid")

    def _validate_groups(self, groups):
        if groups is None:
            return None
        return [self._validate_group(g) for g in groups]

    def _execute_query(self, op, target, igroup, fgroups):
        if fgroups is None and igroup is not None:
            # Apply initial groupings
            grouped = self._group(self._parent.messages, igroup)
            # Apply targeting
            targeted = self._target(grouped, target)
            operated = self._operate(targeted, op)
            return operated
        elif fgroups is not None and igroup is None:
            # Apply final groupings
            grouped = self._group(self._parent.messages, fgroups)

            # Apply targeting (no operation)
            targeted = self._target(grouped, target)  # group -> series
            return targeted
        elif fgroups is not None and igroup is not None:
            # Apply final groupings
            grouped = self._group(self._parent.messages, fgroups)

            def process(group):
                # Apply initial groupings
                igrouped = self._group(group, igroup)
                # Apply targeting
                targeted = self._target(igrouped, target)
                operated = self._operate(targeted, op)
                return operated

            return grouped.apply(process)

    #########
    # Group #
    #########

    def _group(self, df, groups):
        if isinstance(groups, str):
            groups = [groups]
        grouped = df
        for group in groups:
            group_func = self.groups[group]
            grouped = group_func(grouped)
        return grouped.groupby(groups)

    def _group_pre_message(self, df):
        return df.assign(message=df.index)

    def _group_pre_conversation(self, df):
        return df

    def _group_pre_day(self, df):
        return utils.get_day_of_messages(df)

    def _group_pre_week(self, df):
        return utils.get_week_of_messages(df)

    def _group_pre_month(self, df):
        return utils.get_month_of_messages(df)

    def _group_pre_year(self, df):
        return utils.get_year_of_messages(df)

    def _group_pre_sender(self, df):
        return df

    #####################
    # Target from group #
    #####################

    def _target(self, group, target):  # transforms group of dataframes to *series* of scalars
        target_func = self.targets[target]
        return group.apply(lambda x: target_func(x))

    def _target_message(self, df):  # df --> scalar
        return len(df.index)

    def _target_conversation(self, df):
        return df.conversation.nunique()

    def _target_word(self, df):
        return df.content.str.split().str.len().sum()

    def _target_character(self, df):
        return df.content.str.len().sum()

    def _target_duration(self, df):
        return df.timestamp.max() - df.timestamp.min()

    ######################
    # Operation on group #
    ######################

    def _operate(self, series, op):
        return self.ops[op](series)

    def _operator_mean(self, series):
        return series.mean()

    def _operator_median(self, series):
        return series.median()

    def _operator_mode(self, series):
        return series.mode()

    def _operator_range(self, series):
        return series.max() - series.min()

    def _operator_stdev(self, series):
        return series.std()

    def _operator_total(self, series):
        return series.sum()

    def _operator_max(self, series):
        return series.max()

    def _operator_min(self, series):
        return series.min()
