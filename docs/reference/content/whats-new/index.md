+++
date = "2015-03-19T12:53:35-04:00"
title = "What's New"
[menu.main]
  identifier = "Whats New"
  weight = 10
  pre = "<i class='fa fa-cog'></i>"
+++

# What's New in 3.1

Key new features of the 3.1 driver include:

- Builder support for [updates]({{< ref "builders/updates.md" >}})
- Builder support for [aggregation stages and accumulators]({{< ref "builders/aggregation.md" >}})
- Builder support for [geospatial query filters]({{< ref "builders/filters.md#geospatial" >}})
- Builder support for [index keys]({{< ref "builders/indexes.md" >}})
- A new [GridFS API]({{< ref "driver/reference/gridfs/index.md" >}}) that is compatible with the CRUD API introduced in the 3.0 
driver release 
(not yet available in the async driver)
- An [event-based API]({{< ref "driver/reference/management/monitoring.md#command-monitoring" >}}) for monitoring all commands that the 
driver sends to a MongoDB server and all responses received (not yet available in the async driver)

## Upgrading

See the [upgrading guide]({{<ref "whats-new/upgrading.md">}}) on how to upgrade to 3.1
