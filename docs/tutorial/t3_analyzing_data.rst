.. analyzing_data:

Analyzing Data
==============

So, you've loaded your data. How do you get information
out of it?

If you are experienced with Pandas, you can easily do your
analysis manually. The following properties are available::

    mychat.messages         # Dataframe of messages
    mychat.conversations    # Dataframe of conversations


However, ChatAnalytics is designed to make analysis easy
regardless of your experience. Here are some examples of
what you can do::

    mychat.analyze("words per conversation")
    mychat.analyze("number of messages per day")
    mychat.analyze("median conversations per month per person")

There are three basic structures for analytics queries:

* ``<target> per <group>``
* ``<metric> [of] <target> per <group>``
* ``<metric> [of] <target> per <group> per <sortgroup1>[, <sortgroup2>]...``

Lists of all metrics, targets, and groups are below.

There is some leeway in the implementation: for example,
``"mean messages per day per person"`` and
``"mean of messages by day sorted by person"``
are interpreted as the same. The full template is:

.. code-block:: text

    <metric> [of] <target> (per|by) <group>
        [per|by|sorted by] <sortgroup1>
        [(, and|,| and) <sortgroup2>]
        [(, and|,| and) <sortgroup3>]
        ...

There is also a rudimentary autocorrect. Prompts with few
errors, such as ``"meidan mesages per perspn"`` are automatically
corrected and a Warning is raised warning the user of the error.

List of Metrics
---------------

* mean / average
* median / medial
* mode / modal
* range
* standard deviation / stdev / std dev / std
* total / sum
* maximum / max
* minimum / min


List of Targets
---------------

* message / messages / msg / msgs
* conversation / conversations / conv / convs / convo / convos
* word / words / wd / wds
* character / characters / char / chars
* duration / time / length


List of Groups
--------------

* message / msg
* conversation / conv / convo
* day
* week / wk
* month / mo
* year / yr
* sender / person
* channel / chat
