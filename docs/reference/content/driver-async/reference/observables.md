+++
date = "2015-05-14T08:01:00-00:00"
title = "Observables"
[menu.main]
  parent = "Async Reference"
  identifier = "Observables"
  weight = 90
  pre = "<i class='fa'></i>"
+++

## Observables

The MongoDB Async Driver is fully callback-based and makes extensive use of [`SingleResultCallback<T>`]({{< apiref "com/mongodb/async/client/SingleResultCallback" >}}) to achieve this. The `SingleResultCallback<T>` interface requires the implementation of a single method `onResult(T result, Throwable t)` which is called once the operation has completed or errored. The `result` parameter contains the result of the operation if successful. If the operation failed for any reason then the `t` contains the `Throwable` reason for the failure. This pattern allows the users application logic to be deferred until the underlying network IO to MongoDB has been completed. 

The callback pattern is extremely flexible but can quickly become unwieldy if the application logic requires a chain of operations. Nesting of callbacks can make code harder to read and give the appearance of making the codebase more complex that it actually is. To help with this we also have released two observable based asynchronous drivers: 

  1. [MongoDB Reactive Streams Driver](http://mongodb.github.io/mongo-java-driver-reactivestreams/)
  2. [MongoDB RxJava Driver](http://mongodb.github.io/mongo-java-driver-rx/)

These observable drivers follow similar patterns that split the logic into `onNext`, `onError` and `onComplete(d)` methods. These methods split out the logic used by `SingleResultCallback<T>.onResult(T result, Throwable t)` into individual components that can make the code  easier to reason with.

The MongoDB Async Driver provides a factory and interfaces that do the heavy lifting of converting callback-based operations into an observable operations.  There are three interfaces [`Observable`]({{< apiref "com/mongodb/async/client/Observable" >}}), [`Subscription`]({{< apiref "com/mongodb/async/client/Subscription" >}}) and [`Observer`]({{< apiref "com/mongodb/async/client/Observer" >}}). The [`Observables`]({{< apiref "com/mongodb/async/client/Observables" >}}) helpers convert any callback-based operations into observable operations.

{{% note %}}
The interfaces are similar to `Publisher`, `Subscription` and `Subscriber` interfaces from the [reactive streams](http://www.reactive-streams.org/) JVM implementation.  However, we prefer the name `Observerable` to `Publisher` and `Observer` to `Subscriber` for readability purposes.
{{% /note %}}

## Observable
The [`Observable`]({{< apiref "com/mongodb/async/client/Observable" >}}) represents a MongoDB operation which emits its results to the `Observer` based on demand requested by the `Subscription` to the `Observable`. 

## Subscription

A [`Subscription`]({{< apiref "com/mongodb/async/client/Subscription" >}}) represents a one-to-one lifecycle of an `Observer` subscribing to an `Observable`.  A `Subscription` to an `Observable` can only be used by a single `Observer`.  The purpose of a `Subscription` is to control demand and to allow unsubscribing from the `Observable`.

## Observer

An [`Observer`]({{< apiref "com/mongodb/async/client/Observer" >}}) provides the mechanism for receiving push-based notifications from the `Observable`.  Demand for these events is signalled by its `Subscription`. On subscription to an `Observable` the `Observer` will be passed the `Subscription` via the `onSubscribe(Subscription subscription)`.
Demand for results is signaled via the `Subscription` and any results are passed to the `onNext(TResult result)` method.  If there is an error for any reason the `onError(Throwable e)` will be called and no more events passed to the `Observer`. Alternatively, when the `Observer` has consumed all the results from the `Observable` the `onComplete()` method will be called.

## Wrapping a MongoIterable

With the [`Observables`]({{< apiref "com/mongodb/async/client/Observables" >}}) factory creating an `Observable` from a `MongoIterable` is simple.

In the following example we iterate and print out all json forms of documents in a collection:

```java
    Observables.observe(collection.find()).subscribe(new Observer<Document>(){
        @Override
        void onSubscribe(final Subscription subscription) {
            System.out.println("Subscribed and requesting all documents");
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        void onNext(final Document document) {
            System.out.println(document.toJson());
        }

        @Override
        void onError(final Throwable e) {
            System.out.println("There was an error: " + e.getMessage());
        }

        @Override
        void onComplete() {
            System.out.println("Finished iterating all documents");
        }
    });
```

## Wrapping a callback-based method

Creating an `Observable` from any callback-based methods requires the wrapping of the method inside a [`Block`]({{< apiref "com/mongodb/Block" >}}). This allows the execution of the operation to be delayed, until demand is request by the `Subscription`.  The method *must* use the supplied callback to convert the results into an `Observable`. 

In the following example we print out the count of the number of documents in a collection:


```java
    Block<SingleResultCallback<Long>> operation = new Block<SingleResultCallback<Long>>() {
        @Override
        void apply(final SingleResultCallback<Long> callback) {
            collection.countDocuments(callback);
        }
    };

    // Or in Java 8 syntax:
    operation = (Block<SingleResultCallback<Long>>) collection::countDocuments;

    Observables.observe(operation).subscribe(new Observer<Long>(){
        @Override
        void onSubscribe(final Subscription subscription) {
            System.out.println("Subscribed and requesting the count");
            subscription.request(1);
        }

        @Override
        void onNext(final Long count) {
            System.out.println("The collection has " + count + " documents");
        }

        @Override
        void onError(final Throwable e) {
            System.out.println("There was an error: " + e.getMessage());
        }

        @Override
        void onComplete() {
            System.out.println("Finished");
        }
    });
```

## Back Pressure

In the following example, the `Subscription` is used to control demand when iterating an `Observable`. This is similar in concept to the `MongoIterable.forEach` method but allows demand-driven iteration:

 ```java
 Observables.observe(collection.find()).subscribe(new Observer<Document>(){
        private long batchSize = 10;
        private long seen = 0;
        private Subscription subscription;

         @Override
         void onSubscribe(final Subscription subscription) {
             this.subscription = subscription;
             subscription.request(batchSize);
         }

         @Override
         void onNext(final Document document) {
             System.out.println(document.toJson());
             seen += 1;
             if (seen == batchSize) {
                seen = 0;
                subscription.request(batchSize);
             }
         }

         @Override
         void onError(final Throwable e) {
             System.out.println("There was an error: " + e.getMessage());
         }

         @Override
         void onComplete() {
             System.out.println("Finished iterating all documents");
         }
     });
 ```
