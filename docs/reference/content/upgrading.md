+++
date = "2015-03-19T12:53:39-04:00"
title = "Upgrade Considerations"
[menu.main]
  identifier = "Upgrading"
  weight = 80
  pre = "<i class='fa fa-level-up'></i>"
+++

## Upgrading from 3.x

... TODO ...

## Upgrading from mongo-java-driver-reactivestreams 1.12

The main change to the MongoDB Reactive Streams Java Driver 1.12 driver is the removal of the `Success` type.

Breaking changes are as follows:

  * `Publisher<Success>` has been migrated to `Publisher<Void>`. 
    Please note that `onNext` will not be called just `onComplete` if the operation is successful or `onError` if there is an error.
  * Removal of deprecated methods

## Upgrading from mongo-scala-driver 2.7

As the mongodb-driver-async package was deprecated in 3.x. The 4.0 version of the MongoDB Scala Driver is now built upon the
mongo-java-driver-reactivestreams 4.0 driver. One major benefit is now the Scala driver is also a reactive streams driver.

Breaking changes are as follows:

  * `Observable` is now a reactive streams `Publisher` implementations
    `Observable` implicits extend any `Publisher` implementation and can be imported from `org.mongodb.scala._`
  * Completed type has now been removed. `Observable[Completed]` has been migrated to `Observable[Void]`. 
    Please note that `onNext` will not be called just `onComplete` if the operation is successful or `onError` if there is an error.
  * Removal of deprecated methods


## System Requirements

The minimum JVM is Java 8.

## Compatibility

The following table specifies the compatibility of the MongoDB Java driver for use with a specific version of MongoDB.

|Java Driver Version|MongoDB 2.6|MongoDB 3.0 |MongoDB 3.2|MongoDB 3.4|MongoDB 3.6|MongoDB 4.0|
|-------------------|-----------|------------|-----------|-----------|-----------|-----------|
|Version 3.9        |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
|Version 3.8        |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |
|Version 3.7        |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |     |
|Version 3.6        |  ✓  |  ✓  |  ✓  |  ✓  |  ✓  |     |
|Version 3.5        |  ✓  |  ✓  |  ✓  |  ✓  |     |     |
|Version 3.4        |  ✓  |  ✓  |  ✓  |  ✓  |     |     |
|Version 3.3        |  ✓  |  ✓  |  ✓  |     |     |     |
|Version 3.2        |  ✓  |  ✓  |  ✓  |     |     |     |
|Version 3.1        |  ✓  |  ✓  |     |     |     |     |
|Version 3.0        |  ✓  |  ✓  |     |     |     |     |
