+++
date = "2015-03-19T12:53:35-04:00"
title = "What's New"
[menu.main]
  identifier = "Whats New"
  weight = 10
  pre = "<i class='fa fa-cog'></i>"
+++

# What's New in 3.3

New features of the 3.3 driver include:

- [Cluster Monitoring]({{<ref "driver/reference/management/monitoring.md#cluster-monitoring">}}) in the synchronous and asynchronous
drivers
- [Command Monitoring]({{<ref "driver/reference/management/monitoring.md#command-monitoring">}}) in the asynchronous driver
(support in the synchronous driver was added in a previous release)
- Additional query parameters in the [connection string]({{<ref "driver/reference/connecting/connection-settings.md#connection-string">}})
- [GridFS]({{<ref "driver-async/reference/gridfs/index.md">}}) in the asynchronous driver
- Additional configuration options for [GSSAPI authentication]({{<ref "driver/reference/connecting/authenticating.md#gssapi">}}).
- [JNDI]({{<ref "driver/reference/connecting/jndi.md">}}) ObjectFactory implementation

## Upgrading

See the [upgrading guide]({{<ref "whats-new/upgrading.md">}}) on how to upgrade to 3.3.
