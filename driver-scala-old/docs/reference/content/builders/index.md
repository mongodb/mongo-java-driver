+++
date = "2015-03-19T12:53:30-04:00"
title = "Builders"
[menu.main]
  identifier = "Builders"
  weight = 50
  pre = "<i class='fa fa-wrench'></i>"
+++

## Builders

The driver provides several classes that make it easier to use the CRUD API.

- [Filters]({{< relref "filters.md" >}}): Documentation of the driver's support for building query filters
- [Projections]({{< relref "projections.md" >}}): Documentation of the driver's support for building projections
- [Sorts]({{< relref "sorts.md" >}}): Documentation of the driver's support for building sort criteria
- [Aggregation]({{< relref "aggregation.md" >}}): Documentation of the driver's support for building aggregation pipelines
- [Updates]({{< relref "updates.md" >}}): Documentation of the driver's support for building updates
- [Indexes]({{< relref "indexes.md" >}}): Documentation of the driver's support for creating index keys

{{% note class="important" %}}
Builders make use of the [`Bson`]({{< relref "bson/documents.md#bson" >}}) helper which unlike the [`Document`]({{< relref "bson/documents.md#document" >}}) is not type safe. Instead conversion to `BSON` is done via  
[Codecs and the CodecRegistry]({{< coredocref "bson/codecs" >}}).
{{% /note %}}
