+++
date = "2015-03-17T15:36:56Z"
title = "Quick Start Primer"
[menu.main]
  parent = "MongoDB Reactive Streams"
  identifier = "Primer"
  weight = 20
  pre = "<i class='fa'></i>"
+++

# Quick Start Primer

The aim of this guide is to provide background about the MongoDB Reactive Streams driver and its asynchronous API before going onto 
looking at how to use the driver and MongoDB.

{{% note %}}
See the [installation guide]({{< relref "driver-reactive/getting-started/installation.md" >}})
for instructions on how to install the MongoDB Reactive Streams Driver.
{{% /note %}}

## Reactive Streams

This library is an implementation of the [reactive streams](http://www.reactive-streams.org) specification and the reactive stream API 
consists of the following components:

1. Publisher
2. Subscriber
3. Subscription

A `Publisher` is a provider of a potentially unbounded number of sequenced elements, published according to the demand received from it's `Subscriber(s)`.

In response to a call to `Publisher.subscribe(Subscriber)` the possible invocation sequences for methods on the `Subscriber` are given by the following protocol:

```
onSubscribe onNext* (onError | onComplete)?
```

This means that `onSubscribe` is always signalled, followed by a possibly unbounded number of `onNext` signals (as requested by `Subscriber`) 
followed by an `onError` signal if there is a failure, or an `onComplete` signal when no more elements are available—all as long as 
the `Subscription` is not cancelled.

For more information about reactive streams go to: [http://www.reactive-streams.org](http://www.reactive-streams.org).


## Subscribers

The MongoDB Reactive Streams Driver API mirrors the Sync driver API and any methods that cause network IO return a `Publisher<T>`, 
where `T` is the type of response for the operation.

{{% note %}}
All [`Publishers`](http://www.reactive-streams.org/reactive-streams-1.0.1-javadoc/?org/reactivestreams/Publisher.html) returned 
from the API are [cold](https://projectreactor.io/docs/core/release/reference/#reactor.hotCold), meaning that nothing happens until 
they are subscribed to. So just creating a `Publisher` won't cause any network IO. It's not until `Publisher.subscribe` is called that 
the driver executes the operation.

Publishers in this implementation are unicast. Each
[`Subscription`](http://www.reactive-streams.org/reactive-streams-1.0.1-javadoc/?org/reactivestreams/Subscription.html) to a `Publisher` 
relates to a single MongoDB operation and its 
['Subscriber'](http://www.reactive-streams.org/reactive-streams-1.0.1-javadoc/?org/reactivestreams/Subscriber.html) will receive its own 
specific set of results.
{{% /note %}}


## Subscribers used in the Quick Start

For the Quick Start we have implemented a couple of different Subscribers and although this is an artificial scenario for reactive streams we
do block on the results of one example before starting the next, so as to ensure the state of the database.

1. ObservableSubscriber

    The base subscriber class is the [`ObservableSubscriber<T>`]({{< srcref "driver-reactive-streams/src/examples/reactivestreams/tour/src/main/tour/SubscriberHelpers.java" >}}), 
    a Subscriber that stores the results of the `Publisher<T>`. It also contains an `await()` method so we can block for results to ensure the state of 
    the database before going on to the next example.

2. OperationSubscriber

    An implementation of the `ObservableSubscriber` that immediately calls `Subscription.request` when it's subscribed to.

3.  PrintSubscriber

    An implementation of the `OperationSubscriber` that prints a message `Subscriber.onComplete`.
    
4.  ConsumerSubscriber

    An implementation of the `OperationSubscriber` that takes a `Consumer` and calls `Consumer.accept(result)` when `Subscriber.onNext(T result)` is called.

5.  PrintToStringSubscriber

    An implementation of the `ConsumerSubscriber` that prints the string version of the `result` on `Subscriber.onNext()`.

6.  PrintDocumentSubscriber

    An implementation of the `ConsumerSubscriber` that prints the json version of a `Document` on `Subscriber.onNext()`.


##  Blocking and non-blocking examples

As our subscribers contain a latch that is only released when the `onComplete` method of the `Subscriber` is called, we can use that latch 
to block on by calling the `await` method.  Below are two examples using our auto-requesting `PrintDocumentSubscriber`.  
The first is non-blocking and the second blocks waiting for the `Publisher` to complete:

```java
// Create a publisher
Publisher<Document> publisher = collection.find();

// Non blocking
publisher.subscribe(new PrintDocumentSubscriber());

Subscriber<Document> subscriber = new PrintDocumentSubscriber();
publisher.subscribe(subscriber);
subscriber.await(); // Block for the publisher to complete
```

## Publishers, Subscribers and Subscriptions

In general `Publishers`, `Subscribers` and `Subscriptions` are a low level API and it's expected that users and libraries will build more 
expressive APIs upon them rather than solely use these interfaces.  As a library solely implementing these interfaces, users will benefit
from this growing ecosystem, which is a core design principle of reactive streams.
