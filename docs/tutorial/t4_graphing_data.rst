.. graphing_data:

Graphing Data
=============

Any query that returns sorted data with 1-2 sort groups
(``<metric> [of] <target> per <group> per
<sortgroup1>[ <sortgroup2>]``) can be easily
graphed with Plotly or MatPlotLib by setting the backend::

    import pandas as pd
    pd.options.plotting.backend = 'plotly'
    # Or,
    pd.options.plotting.backend = 'matplotlib'

Once you've loaded your data, you pick one of the
graph types and run your query - for example::

    # Using Plotly
    fig = mychat.graph.bar("mean words per conversation sorted by person")
    fig.show()

    # Using MatPlotLib
    import matplotlib.pyplot as plt
    mychat.graph.bar("mean words per conversation sorted by person")
    plt.show()


.. note::
    If you specify two sort groups, one will be on
    the x-axis and the other will be in the key.

    If both groups have less than 5 elements (ie if
    you sort by person and conversation and only have
    4 unique people and 4 different conversations) then
    the first group will go in the legend and the second
    will go in the y-axis

    Otherwise, whichever group is larger goes on the
    y-axis and the smaller group goes in the legend.

    Currently there is not a way to override this behavior.



Graph Types
-----------

======== ==========================
Function Type
======== ==========================
line     line graph
hist     histogram
bar      bar graph
vbar*    unimplemented
kde*     kernel density estimation
density* density
area     line with filled-in area
======== ==========================

\* = unimplemented with Plotly

