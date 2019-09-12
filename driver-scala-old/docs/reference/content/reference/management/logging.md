+++
date = "2015-03-18T21:14:20-04:00"
title = "Logging"
[menu.main]
  parent = "Management"
  identifier = "Logging"
  weight = 10
  pre = "<i class='fa'></i>"
+++

# Logging

By default, logging is enabled via the popular [SLF4J](http://www.slf4j.org/) API. The use of [SLF4J](http://www.slf4j.org/) is optional;
the driver will use SLF4J if the driver detects the presence of SLF4J in the classpath. Otherwise, the driver will fall back to 
JUL (`java.util.logging`).

The driver uses the following logger names:

- `org.mongodb.driver`: the root logger
    - `cluster`: for logs related to monitoring of the MongoDB servers to which the driver connects
    - `connection`: for logs related to connections and connection pools
    - `protocol`: for logs related to protocol message sent to and received from a MongoDB server
        - `insert`: for logs related to insert messages and responses
        - `update`: for logs related to update messages and responses
        - `delete`: for logs related to delete messages and responses
        - `query`: for logs related to query messages and responses
        - `getmore`: for logs related to getmore messages and responses
        - `killcursor`: for logs related to killcursor messages and responses
        - `command`: for logs related to command messages and responses
    - `uri`: for logs related to connection string parsing
    - `management`: for logs related to JMX
