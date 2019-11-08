+++
date = "2015-03-17T15:36:56Z"
title = "Quick Start Primer"
[menu.main]
  parent = "MongoDB Scala Driver"
  identifier = "Scala Primer"
  weight = 20
  pre = "<i class='fa'></i>"
+++

# Quick Start Primer

The aim of this guide is to provide background about the Scala driver and its asynchronous API before going onto 
looking at how to use the driver and MongoDB.

{{% note %}}
See the [installation guide]({{< relref "driver-scala/getting-started/installation.md" >}})
for instructions on how to install the MongoDB Scala Driver.
{{% /note %}}

## Reactive Streams

The MongoDB Scala driver is now built upon the MongoDB Reactive Streams driver and is an implementation of the 
[reactive streams](http://www.reactive-streams.org) specification and the reactive stream API 
consists of the following components:

1. Observable - a custom implementation of a Publisher 
2. Observer - a custom implementation of a Subscriber
3. Subscription

An `Observable` is a provider of a potentially unbounded number of sequenced elements, published according to the demand received from it's `Observer(s)`.

In response to a call to `Observable.subscribe(Observer)` the possible invocation sequences for methods on the `Observer` are given by the following protocol:

```
onSubscribe onNext* (onError | onComplete)?
```

This means that `onSubscribe` is always signalled, followed by a possibly unbounded number of `onNext` signals (as requested by `Subscriber`) 
followed by an `onError` signal if there is a failure, or an `onComplete` signal when no more elements are available—all as long as 
the `Subscription` is not cancelled.

For more information about reactive streams go to: [http://www.reactive-streams.org](http://www.reactive-streams.org).


## Observables

The MongoDB Scala Driver API mirrors the sync driver API and any methods that cause network IO return a `Observable<T>`, 
where `T` is the type of response for the operation.  

{{% note %}}
All [`Observables`](http://www.reactive-streams.org/reactive-streams-1.0.1-javadoc/?org/reactivestreams/Publisher.html) returned from the API are cold, meaning that no I/O happens until they are subscribed to and the subscription makes a request. So just creating a  `Observables` won't cause any network IO. It's not until `Subscription.request()` is called that the driver executes the operation.

Publishers in this implementation are unicast. Each [`Subscription`](http://www.reactive-streams.org/reactive-streams-1.0.1-javadoc/?org/reactivestreams/Subscription.html)  to a `Observables` relates to a single MongoDB operation and its [`Observer`](http://www.reactive-streams.org/reactive-streams-1.0.1-javadoc/?org/reactivestreams/Subscriber.html) will receive its own specific set of results. 
{{% /note %}}

### Back Pressure

By default the `Observer` trait will request all the results from the `Observer` as soon as the `Observable` is subscribed to. Care should 
be taken to ensure that the `Observer` has the capacity to handle all the results from the `Observable`. Custom implementations of the 
`Observer.onSubscribe` can save the `Subscription` so that data is only requested when the `Observer` has the capacity.

## Helpers used in the Quick Tour

For the Quick Tour we use custom implicit helpers defined in [`Helpers.scala`]({{< srcref "driver-scala/src/it/scala/tour/Helpers.scala">}}). 
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