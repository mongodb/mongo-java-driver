+++
date = "2015-03-17T15:36:56Z"
title = "Quick Tour Primer"
[menu.main]
  parent = "Getting Started"
  identifier = "Primer"
  weight = 20
  pre = "<i class='fa'></i>"
+++

# Quick Tour Primer

The aim of this guide is to provide background about the Scala driver and its asynchronous API before going onto 
looking at how to use the driver and MongoDB.

{{% note %}}
See the [installation guide]({{< relref "getting-started/installation-guide.md" >}})
for instructions on how to install the MongoDB Scala Driver.
{{% /note %}}

## The Observable API

The Scala driver is free of dependencies on any third-party frameworks for asynchronous programming. To achieve this the Scala API makes 
use of an custom implementation of the Observer pattern which comprises three traits:

1. Observable
2. Observer
3. Subscription 

An `Observable` is a provider of a potentially unbounded number of sequenced elements, published according to the demand received from it's 
`Subscription`.

In response to a call to `Observable.subscribe(Subscriber)` the possible invocation sequences for methods on the `Observer` are given by 
the following protocol:

```
onSubscribe onNext* (onError | onComplete)?
```

This means that `onSubscribe` is always signalled, followed by a possibly unbounded number of `onNext` signals (as requested by the 
`Subscription`) followed by an `onError` signal if there is a failure, or an `onComplete` signal when no more elements are available - 
as long as the `Subscription` is not cancelled.

The implementation draws inspiration from the [ReactiveX](http://reactivex.io/) and [reactive streams](http://www.reactive-streams.org) 
libraries and provides easy interoperability with them.  For more information see the 
[integrations guide]({{< relref "integrations/index.md" >}}).


## From Async Callbacks to Observables

The MongoDB Scala Driver is built upon the callback-driven MongoDB async driver. The API mirrors the 
async driver API and any methods that cause network IO return an instance of the 
[`Observable[T]`]({{< apiref "org/mongodb/scala/Observable">}}) where `T` is the type of response for the operation. The 
exception to that rule is for methods in the async driver that return a `Void` value in the callback. 
As an `Observable[Void]` never calls `onNext` it stops it from being composable with other `Observables`, so  in these 
circumstances we return a [`Observable[Completed]`]({{< apiref "org/mongodb/scala/Completed">}}) for the operation instead.

### Observable

The [`Observable`]({{< apiref "org/mongodb/scala/Observable">}}) is a trait wrapping the Java interface and where appropriate 
implementations of the trait extend it to make a fluent API. One such example is 
[`FindObservable`]({{< apiref "org/mongodb/scala/FindObservable">}}), accessible through `collection.find()`.

{{% note %}}
All `Observables` returned from the API are cold, meaning that no I/O happens until they are subscribed to. As such an observer is
 guaranteed to see the whole sequence from the beginning. So just creating an `Observable` won't cause any network IO, and it's not until 
`Subscriber.request()` is called that the driver executes the operation.  

Each `Subscription` to an `Observable` relates to a single MongoDB operation and its "Subscriber" will receive it's own specific set of 
results. 
{{% /note %}}

### Back Pressure

By default the `Observer` trait will request all the results from the `Observer` as soon as the `Observable` is subscribed to. Care should 
be taken to ensure that the `Observer` has the capacity to handle all the results from the `Observable`. Custom implementations of the 
`Observer.onSubscribe` can save the `Subscription` so that data is only requested when the `Observer` has the capacity.

## Helpers used in the Quick Tour

For the Quick Tour we use custom implicit helpers defined in [`Helpers.scala`]({{< srcref "examples/src/test/scala/tour/Helpers.scala">}}). 
These helpers get and print results and although this is an artificial scenario for asynchronous code we block on  the results of one 
example before starting the next, so as to ensure the state of the database. The `Helpers` object
provides the following methods:

*   results()

    Blocks until the `Observable` is completed and returns the collected results

*   headResult()

    Blocks until the first result of the `Observable` can be returned

*   printResults()
  
    Blocks until the `Observable` is completed, and prints out each result.
   
*   printHeadResult()

    Blocks until the first result of the `Observable` is available and then prints it.
