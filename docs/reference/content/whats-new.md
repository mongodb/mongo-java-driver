+++
date = "2016-06-09T12:47:43-04:00"
title = "What's New"
[menu.main]
  identifier = "Release notes"
  weight = 15
  pre = "<i class='fa fa-level-up'></i>"
+++

# What's New in 3.3

New features of the 3.3 driver include:

- [Cluster Monitoring]({{<ref "driver/reference/monitoring.md#cluster-monitoring">}}) in the synchronous and asynchronous
drivers
- [Command Monitoring]({{<ref "driver-async/reference/monitoring.md#command-monitoring">}}) in the asynchronous driver
(support in the synchronous driver was added in a previous release)
- Additional query parameters in the [connection string]({{<ref "driver/tutorials/connect-to-mongodb.md">}})
- [GridFS]({{<ref "driver-async/tutorials/gridfs.md">}}) in the asynchronous driver
- Additional configuration options for [GSSAPI authentication]({{<ref "driver/tutorials/authentication.md#gssapi">}}).
- [JNDI]({{<ref "driver/tutorials/jndi.md">}}) ObjectFactory implementation

## Upgrading

See the [upgrading guide]({{<ref "upgrading.md">}}) on how to upgrade to 3.3.
