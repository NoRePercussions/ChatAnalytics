.. importing_data:

Importing Data
=========

For the sake of demonstration, we will use the directories of test
data located on the `project GitHub <https://github.com/NoRePercussions/ChatAnalytics/tree/main/test/test_data>`_.

First, create a ``Chat`` object::

    from chatanalytics import Chat

    mychat = Chat()

You can import individual files with the `load` method::

    path = "./messenger/messages/inbox/" +
        "directmessage_78o3u1q7/message_1.json"
    mychat.load(path)


Or, load all the valid files in a particular folder::

    folder = "./messenger/messages/inbox/" +
        "directmessage_78o3u1q7/
    mychat.batch_load(folder)

However, ``batch_load`` only looks at files in the folder by
default. It will **not** walk through subdirectories.

If we wanted to import every conversation in every subdirectory
of ``./messenger/``, we do the following::

    mychat.batch_load("./messenger/", do_walk=True)

.. note::
    The setter methods of the ``Chat`` objects are in-place,
    but also return themselves. This allows optional chaining::

        mychat.load(x).load(y)


