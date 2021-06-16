# pywarp10

Makes it easier to work with warp10 in python. The [Warp 10 platform](https://warp10.io)
is built to simplify managing and processing Time Series data. It includes a Geo Time
Series database and a companion analytics engine.

# Motivation

The Warp10 platform can already interact with python using the
[`py4j`](https://www.py4j.org/) module. The documentation can be found
[here](https://www.warp10.io/content/03_Documentation/04__Tooling/03_Python). Few blog
posts are also written to detail the use of this module: [The Py4J plugin for Warp
10](https://blog.senx.io/the-py4j-plugin-for-warp-10/) or [WarpScript for
Pythonists](https://blog.senx.io/warpscript-for-pythonists/) for instance.

Py4J is a low level tool which needs to be configured in order to work with warp10: the
creation of the gateway, the entry point and handling the stack is all done manually.
Moreover, all objects are not recognised directly: GTS cannot be transformed directly to
a python object with py4j and user must first transform the GTS into a
[PICKLE](https://www.warp10.io/doc/AItFHJCAI3J) object,
[ARROW](https://blog.senx.io/conversions-to-apache-arrow-format/) object or a MAP in
order to be correctly read with python.

This module aims to expose some handy tools in order to work with warp10 database
without dealing with configurations and object transformations.

# Examples

The module exposes the `Warpscript` object with some useful methods:

```python
from pywarp10 import Warpscript

# The address and the port to connect to the Warp10 server can either be passed in the
# object parameter or by setting environment variables: WARP10_HOST and WARP10_PORT.
ws = Warpscript(host="127.0.0.1", port=25333)

# Script
# ------

# The `script` constructs WarpScript by translating python parameters into WarpScript.
# The optional parameter `fun` is used to add a WarpScript function at the end of the
# script.
#
# Durations, dates and datetime are automatically parsed using `dateparser` and 
# `durations` python modules.

python_object = {
    "token": "some-token",
    "class": "classname",
    "labels": {},
    "end", "1 day ago",
    "count": 1,
}
ws.script(python_object, fun = "FETCH")
# > { 
# >   'token' 'some-token' 
# >   'class' 'class' 
# >   'end' '2021-06-16T12:15:15.684532Z'
# >   'count' 1 
# > } FETCH


# Multiple scripts can be chained together.
# The `ws:` prefix to a string indicates that the string should not be sanitized (i.e.
# the string should not be surrounded by single quotes in the warpscript).

bucketize = ["ws:SWAP", "ws:bucketizer.mean", 0, "1 h", 0]
ws.script(bucketize, fun = "BUCKETIZE")
# > { 
# >   'token' 'some-token' 
# >   'class' 'class' 
# >   'end' '2021-06-16T12:15:15.684532Z'
# >   'count' 100 
# > } FETCH
# > [ SWAP bucketizer.mean 0 3600 0 ] BUCKETIZE

# Exec
# ----

# The `exec` method execute warpscript build with script on the warp10 server and
# returns the corresponding python objects, including GTS and LGTS which are 
# automatically transformed into pandas dataframe.
# Once a warpscript is executed, the stack is cleared and the gateway is closed. A new
# one is opened for another execution.

ws.exec()
# >            timestamps  traffic_in service_name          _source     host_name 
# > 0 2020-10-28 00:00:00  6127530.00 some-service         centreon     some-host 
# > 1 2020-11-16 16:00:00  6871267.68          NaN           client           NaN 
# > 2 2020-11-16 16:00:00  6871267.68 some-service         centreon     some-host 
# > 3 2021-04-30 16:00:00  9158182.05 some-service           client     some-host

# In the last example, the FETCH-BUCKETIZE produced a list of GTS. The resulting
# dataframe has at least two columns: `timestamps` and `values` where the value column
# is named after the classname.
# All the other columns are labels that differ in at least one GTS from the list of GTS.
```
