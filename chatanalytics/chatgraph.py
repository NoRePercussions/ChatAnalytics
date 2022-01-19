# Manages analysis
# NRP 21
import pandas as pd

from . import utils

from datetime import timedelta
import re
from logging import debug


class ChatGraph:  # stored as GenericChat.analyze
    """Graphs chats"""

    #
    simple_query = re.compile(r"^([a-zA-Z]+?) (?:per|by|sorted by) ([a-zA-Z]+?(?:(?: per| by|,|, and| and) [a-zA-Z]+" +
                              r"?)*?)$")
    #
    complex_query = re.compile(r"^((?:(?:[a-zA-Z]+) |(?:[a-zA-Z]+(?: [a-zA-Z]+)*) of )(?:[a-zA-Z]+) (?:per|by) (?:[a-zA-Z]+))" +
                               r"(?: (?:per|by|sorted by) ([a-zA-Z]+(?:(?:, |, and| and)[a-zA-Z]+)*))?$")

    # https://regex101.com/r/81gpcU/2/
    decomposer = re.compile(r" by |, |, and | per | and | ")

    def __init__(self, parent):
        self._parent = parent

    def __call__(self, query, type, *args, **kwargs):
        parsed = self._parse_query(query)
        return self._generate_graph(query, parsed)

    def _parse_query(self, query):
        if match := self.simple_query.match(query):
            y_axis_name = match.group(1)
            x_groups = match.group(2)
        elif match := self.complex_query.match(query):
            y_axis_name = match.group(1)
            x_groups = match.group(2)
        else:
            raise ValueError(f"Query '{query}' is invalid")

        title = query.title()
        y_axis_name = y_axis_name.title()
        x_groups = x_groups.title()
        return {"y_axis_name":y_axis_name, "x_groups":x_groups, "title":title}

    def _generate_graph(self, query, parsed):
        splits = self._decompose(parsed['x_groups'])
        if len(splits) == 0:
            raise ValueError("No groups were passed to sort by:\n" +
                                 f" Query {query} returns value {self._parent._analyze_backend(query)}")
        elif len(splits) == 1:
            ax = self._parent._analyze_backend(query).plot(title=parsed['title'])
            ax.set_xlabel(parsed['x_groups'])
            ax.set_ylabel(parsed['y_axis_name'])
            return ax
        elif len(splits) == 2:
            result = self._parent._analyze_backend(query)
            idx = result.index
            if len(idx.unique(level=0)) < len(idx.unique(level=1)) or len(idx.unique(level=0)) < 5:
                # If x < y or x < 5: Prefer y for the x axis, x for the legend
                unstacked = result.unstack(level=0) # unstack x for legend
                ax = unstacked.plot(title=parsed['title'])
                ax.set_xlabel(splits[1])
                ax.legend(title=splits[0])
                ax.set_ylabel(parsed['y_axis_name'])
            else:
                # Otherwise prefer x for x-axis, y for legend
                unstacked = result.unstack(level=1) # unstack y for legend
                ax = unstacked.plot(title=parsed['title'])
                ax.set_xlabel(splits[0])
                ax.legend(title=splits[1])
                ax.set_ylabel(parsed['y_axis_name'])
            return ax
        elif len(splits) > 2:
            raise ValueError(f"Query {query} has too many final groups")


    def _decompose(self, query):
        if isinstance(query, str):
            return self.decomposer.split(query)
        return None

