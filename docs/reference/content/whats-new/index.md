+++
date = "2015-03-19T12:53:35-04:00"
title = "What's New"
[menu.main]
  identifier = "Whats New"
  weight = 10
  pre = "<i class='fa fa-cog'></i>"
+++

# What's New in 3.2

Key new features of the 3.2 driver include:

- Support for bypassing [document validation]({{< docsref "release-notes/3.2/#document-validation" >}}) on collections where document 
validation has been enabled. 
- Builder support for new [aggregation stages]({{< docsref "release-notes/3.2/#new-aggregation-stages" >}}) 
and [accumulators]({{< docsref "release-notes/3.2/#new-accumulators-for-group-stage" >}}) for the $group stage.
- Support for [read concern]({{< docsref "release-notes/3.2/#readconcern" >}}).
- Support for [version 3 text indexes]({{< docsref "release-notes/3.2/#text-index-version-3" >}})
- Support for write concern on all DBCollection helpers for the findandmodify command

## Upgrading

See the [upgrading guide]({{<ref "whats-new/upgrading.md">}}) on how to upgrade to 3.2.
