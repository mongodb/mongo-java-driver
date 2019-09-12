+++
date = "2015-11-18T09:56:14Z"
title = "Upgrading"
[menu.main]
  weight = 95
  pre = "<i class='fa fa-wrench'></i>"
+++

## Upgrade

### 2.5.0

No expected upgrade issues from previous release.

### 2.4.0

#### ClientSession support for transactions

Due to the introduction of transaction support in MongoDB, the `ClientSession` type alias has been moved, so to support Async transactions.
The type alias has been updated to `com.mongodb.async.client.ClientSession`. Code will need to be recompiled against the underlying java
driver.

### 2.3.0

#### MongoClientSettings

The Mongo Java Driver 3.7.0 introduces a new `com.mongodb.MongoClientSettings` class to unify the settings across the sync and async drivers.
The legacy `com.mongodb.async.client.MongoClientSettings` has been deprecated. As such the `MongoClientSettings` alias now points to the
supported class and the `MongoClientSettings.builder()` helper points to the supported builder.

The legacy settings are still supported and can be imported from `com.mongodb.async.client.MongoClientSettings`. It is only required if you
need multiple credentials or custom `heartbeatSocketSettings`, both of which have been deprecated.

### 2.2.0

No expected upgrade issues from previous release.

### 2.1.0

No expected upgrade issues from previous release.

### 2.0.0


#### MongoCollection method default to collection type fix.
    
Previously, in the 1.x series `MongoCollection[T].find()` by default would return a `FindObservable[Document]` and not `FindObservable[T]`. 
While this was easy to work around by explicitly setting the type eg: `MongoCollection[T].find[T]()` we've bumped the version to 2.0.0 so 
that we can fix the API issue.

If you took advantage of the default type being `Document` you will need to update your code: `MongoCollection[T].find[Document]()`.

#### SingleObservable
    
The addition of the `SingleObservable` trait allows for easy identification of `Observables` that return only a single element. 
For a SingleObservables `toFuture()` will return a `Future[T]` instead of `Future[Seq[T]]`, any code relying on this will need to be 
updated to reflect the new result type.
