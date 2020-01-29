+++
date = "2015-05-14T08:01:00-00:00"
title = "Observables"
[menu.main]
  parent = "Scala Reference"
  identifier = "Observables"
  weight = 75
  pre = "<i class='fa'></i>"
+++

## Observables

The MongoDB Scala Driver is an asynchronous and non blocking driver. Using the `Observable` model asynchronous events become simple, composable operations, freed from the complexity of nested callbacks.  

For asynchronous operations there are three interfaces [`Observable`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/Observable.html" >}}), [`Subscription`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/Subscription.html" >}}) and [`Observer`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/Observer.html" >}}).

{{% note %}}
The MongoDB Scala driver is now built upon the MongoDB Reactive Streams driver and is an implementation of the 
[reactive streams](http://www.reactive-streams.org) specification. Observerables are implementations of Publishers and Observers are implementations of Subscribers.

Class naming convention:

1. Observable - a custom implementation of a Publisher
2. Observer - a custom implementation of a Subscriber
3. Subscription
{{% /note %}}

## Observable
The [`Observable`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/Observable.html" >}}) is an extended `Publisher` implementation and in general represents a MongoDB operation which emits its results to the `Observer` based on demand requested by the `Subscription` to the `Observable`. 

{{% note class="important" %}}
Observables can be thought of as partial functions and like partial functions nothing happens until they are called. 
An `Observable` can be subscribed to multiple times, with each subscription potentially causing new side effects eg: querying MongoDB or inserting data.
{{% /note %}}

### SingleObservable
The [`SingleObservable`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/SingleObservable.html" >}}) trait is a `Publisher` implementation that will only return a single item.
It can be used in the same way as ordinary `Observables`.

## Subscription

A [`Subscription`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/Subscription.html" >}}) represents a one-to-one lifecycle of an `Observer` subscribing to an `Observable`.  A `Subscription` to an `Observable` can only be used by a single `Observer`.  The purpose of a `Subscription` is to control demand and to allow unsubscribing from the `Observable`.

## Observer

An [`Observer`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/Observer.html" >}}) provides the mechanism for receiving push-based notifications from the
`Observable`.  Demand for these events is signalled by its `Subscription`.

On subscription to an `Observable[TResult]` the `Observer` will be passed the `Subscription` via the 
`onSubscribe(subscription: Subscription)`. Demand for results is signaled via the `Subscription` and any results are passed to the 
`onNext(result: TResult)` method.  If there is an error for any reason the `onError(e: Throwable)` will be 
called and no more events passed to the `Observer`. Alternatively, when the `Observer` has consumed all the results from the `Observable` 
the `onComplete()` method will be called.


## Back Pressure

In the following example, the `Subscription` is used to control demand when iterating an `Observable`. The default `Observer` implementation
automatically requests all the data. Below we override the `onSubscribe` method custom so we can manage the demand driven iteration of the 
`Observable`:

 ```scala
collection.find().subscribe(new Observer[Document](){

  var batchSize: Long = 10
  var seen: Long = 0
  var subscription: Option[Subscription] = None
  
  override def onSubscribe(subscription: Subscription): Unit = {
    this.subscription = Some(subscription)
    subscription.request(batchSize)
  }
  
  override def onNext(result: Document): Unit = {
    println(document.toJson())
    seen += 1
    if (seen == batchSize) {
      seen = 0
      subscription.get.request(batchSize)
    }
  }

  override def onError(e: Throwable): Unit = println(s"Error: $e")

  override def onComplete(): Unit = println("Completed")
})
```
## Observable Helpers

The `org.mongodb.scala` package provides improved interaction with `Publishers`. The extended functionality includes simple 
subscription via anonymous functions:

```scala
// Subscribe with custom onNext:
collection.find().subscribe((doc: Document) => println(doc.toJson()))

// Subscribe with custom onNext and onError
collection.find().subscribe((doc: Document) => println(doc.toJson()),
                            (e: Throwable) => println(s"There was an error: $e"))

// Subscribe with custom onNext, onError and onComplete
collection.find().subscribe((doc: Document) => println(doc.toJson()),
                            (e: Throwable) => println(s"There was an error: $e"),
                            () => println("Completed!"))
```

The `org.mongodb.scala` package includes implicit class also provides the following Monadic operators to make chaining and working with `Publisher` / `Observable` instances 
simpler:


```scala
GenerateHtmlObservable().andThen({
  case Success(html: String) => renderHtml(html)
  case Failure(t) => renderHttp500
})

```

The full list of Monadic operators available are:

 - *[`andThen`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/ObservableImplicits$BoxedPublisher.html#andThen[U](pf:PartialFunction[scala.util.Try[T],U]):org.mongodb.scala.Observable[T]" >}})*: 
    Allows the chaining of Observables. 
 - *[`collect`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/ObservableImplicits$BoxedPublisher.html#collect[S]():org.mongodb.scala.Observable[Seq[T]]" >}})* :
    Collects all the results into a sequence.
 - *[`fallbackTo`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/ObservableImplicits$BoxedPublisher.html#fallbackTo[U>:T](that:org.mongodb.scala.Observable[U]):org.mongodb.scala.Observable[U]" >}})* :
    Allows falling back to an alternative `Observable` if there is a failure
 - *[`filter`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/ObservableImplicits$BoxedPublisher.html#filter(predicate:T=>Boolean):org.mongodb.scala.Observable[T]" >}})* :
    Filters results of the `Observable`.
 - *[`flatMap`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/ObservableImplicits$BoxedPublisher.html#flatMap[S](mapFunction:T=>org.mongodb.scala.Observable[S]):org.mongodb.scala.Observable[S]" >}})* :
    Create a new `Observable` by applying a function to each result of the `Observable`.
 - *[`foldLeft`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/ObservableImplicits$BoxedPublisher.html#foldLeft[S](initialValue:S)(accumulator:(S,T)=>S):org.mongodb.scala.Observable[S]" >}})* :
    Creates a new Observable that contains the single result of the applied accumulator function.
 - *[`foreach`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/ObservableImplicits$BoxedPublisher.html#foreach[U](doOnEach:T=>U):Unit" >}})* :
    Applies a function applied to each emitted result.
 - *[`head`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/ObservableImplicits$BoxedPublisher.html#head():scala.concurrent.Future[T]" >}})* :
    Returns the head of the `Observable` in a `Future`.
 - *[`map`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/ObservableImplicits$BoxedPublisher.html#map[S](mapFunction:T=>S):org.mongodb.scala.Observable[S]" >}})* :
    Creates a new Observable by applying a function to each emitted result of the Observable.
 - *[`observeOn`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/ObservableImplicits$BoxedPublisher.html#observeOn[S](context:ExecutionContext):org.mongodb.scala.Observable[S]" >}})* :
    Creates a new Observable that uses a specific `ExecutionContext` for future operations.
 - *[`recover`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/ObservableImplicits$BoxedPublisher.html#recover[U>:T](pf:PartialFunction[Throwable,U]):org.mongodb.scala.Observable[U]" >}})* :
    Creates a new `Observable` that will handle any matching throwable that this `Observable` might contain by assigning it a value of 
    another `Observable`.
 - *[`recoverWith`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/ObservableImplicits$BoxedPublisher.html#recoverWith[U>:T](pf:PartialFunction[Throwable,org.mongodb.scala.Observable[U]]):org.mongodb.scala.Observable[U]" >}})* :
    Creates a new Observable that will handle any matching throwable that this Observable might contain.
 - *[`toFuture`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/ObservableImplicits$BoxedPublisher.html#toFuture():scala.concurrent.Future[Seq[T]]" >}})* :
    Collects the `Observable` results and converts to a `Future`.
 - *[`transform`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/ObservableImplicits$BoxedPublisher.html#transform[S](mapFunction:T=>S,errorMapFunction:Throwable=>Throwable):org.mongodb.scala.Observable[S]" >}})* :
    Creates a new `Observable` by applying the resultFunction function to each emitted result.
 - *[`withFilter`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/ObservableImplicits$BoxedPublisher.html#withFilter(p:T=>Boolean):org.mongodb.scala.Observable[T]" >}})* :
    Provides for-comprehensions support to Observables.
 - *[`zip`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/ObservableImplicits$BoxedPublisher.html#zip[U](that:org.mongodb.scala.Observable[U]):org.mongodb.scala.Observable[(T,U)]" >}})* :
    Zips the values of this and that `Observable`, and creates a new `Observable` holding the tuple of their results.

### SingleObservable

As we know that a [`SingleObservable[T]`]({{< apiref "mongo-scala-driver" "org/mongodb/scala/SingleObservable.html" >}}) will only return a single item the `toFuture()` method will return a `Future[T]` in the same way as the `head` method does.
There is also an implicit converter that converts a `Publisher` to a `SingleObservable`
