/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb.scala

import org.reflections.Reflections
import org.reflections.scanners.SubTypesScanner
import org.reflections.util.{ ClasspathHelper, ConfigurationBuilder, FilterBuilder }
import org.scalatest.Inspectors.forEvery

import java.lang.reflect.Modifier._
import scala.collection.JavaConverters._
import scala.reflect.runtime.currentMirror

class ApiAliasAndCompanionSpec extends BaseSpec {

  val classFilter = (f: Class[_ <: Object]) =>
    isPublic(f.getModifiers) && !f.getName.contains("$") && !f.getSimpleName.contains("Test")

  "The scala package" should "mirror the com.mongodb package and com.mongodb.reactivestreams.client" in {
    val packageName = "com.mongodb"
    val javaExclusions = Set(
      "Address",
      "AwsCredential",
      "BasicDBList",
      "BasicDBObject",
      "BasicDBObjectBuilder",
      "Block",
      "BSONTimestampCodec",
      "CausalConsistencyExamples",
      "ChangeStreamSamples",
      "ContextHelper",
      "ContextProvider",
      "DBObject",
      "DBObjectCodec",
      "DBObjectCodecProvider",
      "DBRef",
      "DBRefCodec",
      "DBRefCodecProvider",
      "DnsClient",
      "DnsClientProvider",
      "DocumentToDBRefTransformer",
      "Function",
      "FutureResultCallback",
      "InetAddressResolver",
      "InetAddressResolverProvider",
      "Jep395RecordCodecProvider",
      "KerberosSubjectProvider",
      "KotlinCodecProvider",
      "MongoClients",
      "NonNull",
      "NonNullApi",
      "Nullable",
      "Person",
      "ReadPreferenceHedgeOptions",
      "ReactiveContextProvider",
      "RequestContext",
      "ServerApi",
      "ServerCursor",
      "ServerSession",
      "SessionContext",
      "SingleResultCallback",
      "SubjectProvider",
      "TransactionExample",
      "UnixServerAddress",
      "SubscriberHelpers",
      "PublisherHelpers",
      "TargetDocument",
      "UpdatePrimer",
      "InsertPrimer",
      "IndexesPrimer",
      "QueryPrimer",
      "DocumentationSamples",
      "AggregatePrimer",
      "RemovePrimer",
      "SyncMongoClient",
      "SyncGridFSBucket",
      "SyncMongoDatabase",
      "SyncClientEncryption"
    )
    val scalaExclusions = Set(
      "BuildInfo",
      "BulkWriteResult",
      "ClientSessionImplicits",
      "Document",
      "Helpers",
      "internal",
      "Observable",
      "ObservableImplicits",
      "Observer",
      "package",
      "ReadConcernLevel",
      "SingleObservable",
      "Subscription"
    )

    val classFilter = (f: Class[_ <: Object]) => {
      isPublic(f.getModifiers) &&
      !f.getName.contains("$") &&
      !f.getSimpleName.contains("Spec") &&
      !f.getSimpleName.contains("Test") &&
      !f.getSimpleName.contains("Tour") &&
      !f.getSimpleName.contains("Fixture") &&
      !javaExclusions.contains(f.getSimpleName)
    }
    val filters = FilterBuilder.parse(
      """
        |-com.mongodb.annotations.*,
        |-com.mongodb.assertions.*,
        |-com.mongodb.binding.*,
        |-com.mongodb.bulk.*,
        |-com.mongodb.client.*,
        |-com.mongodb.connection.*,
        |-com.mongodb.crypt.*,
        |-com.mongodb.diagnostics.*,
        |-com.mongodb.event.*,
        |-com.mongodb.internal.*,
        |-com.mongodb.management.*,
        |-com.mongodb.operation.*,
        |-com.mongodb.selector.*,
        |-com.mongodb.kotlin.*,
        |-com.mongodb.test.*,
        |-com.mongodb.client.gridfs.*,
        |-com.mongodb.async.client.*,
        |-com.mongodb.async.client.gridfs.*,
        |-com.mongodb.async.client.internal.*,
        |-com.mongodb.async.client.vault.*,
        |-com.mongodb.reactivestreams.client.gridfs.*,
        |-com.mongodb.reactivestreams.client.internal.*,
        |-com.mongodb.reactivestreams.client.vault.*""".stripMargin
    )

    val exceptions = new Reflections(packageName)
      .getSubTypesOf(classOf[MongoException])
      .asScala
      .map(_.getSimpleName)
      .toSet +
      "MongoException" - "MongoGridFSException" - "MongoConfigurationException" - "MongoWriteConcernWithResponseException"

    val objects = new Reflections(
      new ConfigurationBuilder()
        .setUrls(ClasspathHelper.forPackage(packageName))
        .setScanners(new SubTypesScanner(false))
        .filterInputsBy(filters)
    ).getSubTypesOf(classOf[Object])
      .asScala
      .filter(classFilter)
      .map(_.getSimpleName.replace("Publisher", "Observable"))
      .toSet

    val wrapped = objects ++ exceptions

    val scalaPackageName = "org.mongodb.scala"
    val scalaObjects = new Reflections(scalaPackageName, new SubTypesScanner(false))
      .getSubTypesOf(classOf[Object])
      .asScala
      .filter(classFilter)
      .filter(f => f.getPackage.getName == scalaPackageName)
      .map(_.getSimpleName)
      .toSet
    val packageObjects =
      currentMirror.staticPackage(scalaPackageName).info.decls.filter(!_.isImplicit).map(_.name.toString).toSet
    val local = (scalaObjects ++ packageObjects) -- scalaExclusions

    diff(local, wrapped) shouldBe empty
  }

  it should "mirror parts of com.mongodb.connection in org.mongdb.scala.connection" in {
    val packageName = "com.mongodb.connection"
    val javaExclusions = Set(
      "AsyncCompletionHandler",
      "AsyncConnection",
      "AsynchronousSocketChannelStreamFactory",
      "BufferProvider",
      "Builder",
      "BulkWriteBatchCombiner",
      "ChangeEvent",
      "ChangeListener",
      "Cluster",
      "ClusterDescription",
      "ClusterFactory",
      "ClusterId",
      "Connection",
      "ConnectionDescription",
      "ConnectionId",
      "DefaultClusterFactory",
      "DefaultRandomStringGenerator",
      "QueryResult",
      "RandomStringGenerator",
      "Server",
      "ServerDescription",
      "ServerId",
      "ServerVersion",
      "SocketStreamFactory",
      "Stream",
      "SplittablePayload",
      "TopologyVersion"
    )

    val filters = FilterBuilder.parse("-com.mongodb.connection.netty.*")
    val wrapped = new Reflections(
      new ConfigurationBuilder()
        .setUrls(ClasspathHelper.forPackage(packageName))
        .setScanners(new SubTypesScanner(false))
        .filterInputsBy(filters)
    ).getSubTypesOf(classOf[Object])
      .asScala
      .filter(_.getPackage.getName == packageName)
      .filter(classFilter)
      .map(_.getSimpleName)
      .toSet -- javaExclusions

    val scalaPackageName = "org.mongodb.scala.connection"
    val scalaExclusions = Set(
      "package",
      "NettyStreamFactoryFactory",
      "NettyStreamFactoryFactoryBuilder",
      "AsynchronousSocketChannelStreamFactoryFactoryBuilder"
    )
    val local = currentMirror.staticPackage(scalaPackageName).info.decls.map(_.name.toString).toSet -- scalaExclusions

    diff(local, wrapped) shouldBe empty
  }

  it should "mirror all com.mongodb.client. into org.mongdb.scala." in {
    val packageName = "com.mongodb.client"

    val javaExclusions = Set(
      "ClientSession",
      "ConcreteCodecProvider",
      "Fixture",
      "ImmutableDocument",
      "ImmutableDocumentCodec",
      "ImmutableDocumentCodecProvider",
      "ListCollectionsObservable",
      "MongoChangeStreamCursor",
      "MongoClientFactory",
      "MongoClients",
      "MongoCursor",
      "MongoObservable",
      "Name",
      "NameCodecProvider",
      "SynchronousContextProvider",
      "TransactionBody",
      "FailPoint"
    )

    val wrapped = new Reflections(packageName, new SubTypesScanner(false))
      .getSubTypesOf(classOf[Object])
      .asScala
      .filter(_.getPackage.getName == packageName)
      .filter(classFilter)
      .map(_.getSimpleName.replace("Iterable", "Observable"))
      .toSet -- javaExclusions

    val scalaPackageName = "org.mongodb.scala"
    val local = new Reflections(scalaPackageName, new SubTypesScanner(false))
      .getSubTypesOf(classOf[Object])
      .asScala
      .filter(_.getPackage.getName == scalaPackageName)
      .filter((f: Class[_ <: Object]) => isPublic(f.getModifiers))
      .map(_.getSimpleName.replace("$", ""))
      .toSet

    forEvery(wrapped) { (className: String) =>
      local should contain(className)
    }
  }

  it should "mirror all com.mongodb.client.model in org.mongdb.scala.model" in {
    val javaExclusions = Set("ParallelCollectionScanOptions", "AggregationLevel")
    val packageName = "com.mongodb.client.model"

    val objectsAndEnums = new Reflections(packageName, new SubTypesScanner(false))
      .getSubTypesOf(classOf[Object])
      .asScala ++
      new Reflections(packageName, new SubTypesScanner(false)).getSubTypesOf(classOf[Enum[_]]).asScala

    val wrapped = objectsAndEnums
      .filter(_.getPackage.getName == packageName)
      .filter(classFilter)
      .map(_.getSimpleName)
      .toSet -- javaExclusions

    val scalaPackageName = "org.mongodb.scala.model"
    val localPackage = currentMirror.staticPackage(scalaPackageName).info.decls.map(_.name.toString).toSet
    val localObjects = new Reflections(scalaPackageName, new SubTypesScanner(false))
      .getSubTypesOf(classOf[Object])
      .asScala
      .filter(_.getPackage.getName == scalaPackageName)
      .filter(classFilter)
      .map(_.getSimpleName)
      .toSet
    val scalaExclusions = Set("package")
    val local = (localPackage ++ localObjects) -- scalaExclusions

    diff(local, wrapped) shouldBe empty
  }

  it should "mirror all com.mongodb.client.model.search in org.mongdb.scala.model.search" in {
    val packageName = "com.mongodb.client.model.search"
    val wrapped = new Reflections(packageName, new SubTypesScanner(false))
      .getSubTypesOf(classOf[Object])
      .asScala
      .filter(_.getPackage.getName == packageName)
      .filter(classFilter)
      .map(_.getSimpleName)
      .toSet
    val scalaPackageName = "org.mongodb.scala.model.search"
    val localPackage = currentMirror.staticPackage(scalaPackageName).info.decls.map(_.name.toString).toSet
    val localObjects = new Reflections(scalaPackageName, new SubTypesScanner(false))
      .getSubTypesOf(classOf[Object])
      .asScala
      .filter(_.getPackage.getName == scalaPackageName)
      .filter(classFilter)
      .map(_.getSimpleName)
      .toSet
    val local = localPackage ++ localObjects - "package"
    diff(local, wrapped) shouldBe empty
  }

  it should "mirror all com.mongodb.client.model.geojson in org.mongdb.scala.model.geojson" in {
    val packageName = "com.mongodb.client.model.geojson"
    val wrapped = new Reflections(packageName, new SubTypesScanner(false))
      .getSubTypesOf(classOf[Object])
      .asScala
      .filter(_.getPackage.getName == packageName)
      .filter(classFilter)
      .map(_.getSimpleName)
      .toSet ++ Set("GeoJsonObjectType", "CoordinateReferenceSystemType")

    val scalaPackageName = "org.mongodb.scala.model.geojson"
    val local = currentMirror.staticPackage(scalaPackageName).info.decls.map(_.name.toString).toSet - "package"

    diff(local, wrapped) shouldBe empty
  }

  it should "mirror all com.mongodb.client.result in org.mongdb.scala.result" in {
    val packageName = "com.mongodb.client.result"
    val wrapped = new Reflections(packageName, new SubTypesScanner(false))
      .getSubTypesOf(classOf[Object])
      .asScala
      .filter(_.getPackage.getName == packageName)
      .filter(classFilter)
      .map(_.getSimpleName)
      .toSet

    val scalaPackageName = "org.mongodb.scala.result"
    val local = currentMirror.staticPackage(scalaPackageName).info.decls.map(_.name.toString).toSet - "package"

    diff(local, wrapped) shouldBe empty
  }

  it should "mirror all com.mongodb.client.vault in org.mongdb.scala.vault" in {
    val packageName = "com.mongodb.reactivestreams.client.vault"
    val wrapped = new Reflections(packageName, new SubTypesScanner(false))
      .getSubTypesOf(classOf[Object])
      .asScala
      .filter(_.getPackage.getName == packageName)
      .filter(classFilter)
      .map(_.getSimpleName)
      .toSet

    val scalaPackageName = "org.mongodb.scala.vault"
    val localPackage = currentMirror.staticPackage(scalaPackageName).info.decls.map(_.name.toString).toSet
    val localObjects = new Reflections(scalaPackageName, new SubTypesScanner(false))
      .getSubTypesOf(classOf[Object])
      .asScala
      .filter(classFilter)
      .map(_.getSimpleName)
      .toSet
    val scalaExclusions = Set("package")
    val local = (localPackage ++ localObjects) -- scalaExclusions

    diff(local, wrapped) shouldBe empty
  }

  it should "mirror all com.mongodb.WriteConcern in org.mongodb.scala.WriteConcern" in {
    val notMirrored = Set(
      "SAFE",
      "serialVersionUID",
      "FSYNCED",
      "FSYNC_SAFE",
      "JOURNAL_SAFE",
      "REPLICAS_SAFE",
      "REPLICA_ACKNOWLEDGED",
      "NAMED_CONCERNS",
      "NORMAL",
      "majorityWriteConcern",
      "valueOf"
    )
    val wrapped =
      (classOf[com.mongodb.WriteConcern].getDeclaredMethods ++ classOf[com.mongodb.WriteConcern].getDeclaredFields)
        .filter(f => isStatic(f.getModifiers) && !notMirrored.contains(f.getName))
        .map(_.getName)
        .toSet

    val local = WriteConcern.getClass.getDeclaredMethods
      .filter(f => f.getName != "apply" && isPublic(f.getModifiers))
      .map(_.getName)
      .toSet

    diff(local, wrapped) shouldBe empty
  }

  it should "mirror com.mongodb.reactivestreams.client.gridfs in org.mongdb.scala.gridfs" in {
    val javaExclusions = Set("GridFSBuckets", "GridFSDownloadByNameOptions")
    val wrapped: Set[String] = Set("com.mongodb.reactivestreams.client.gridfs", "com.mongodb.client.gridfs.model")
      .flatMap(packageName =>
        new Reflections(packageName, new SubTypesScanner(false))
          .getSubTypesOf(classOf[Object])
          .asScala
          .filter(_.getPackage.getName == packageName)
          .filter(classFilter)
          .map(_.getSimpleName.replace("Publisher", "Observable"))
          .toSet
      ) -- javaExclusions + "MongoGridFSException"

    val scalaPackageName = "org.mongodb.scala.gridfs"
    val scalaExclusions = Set(
      "package",
      "AsyncOutputStream",
      "AsyncInputStream",
      "GridFSUploadStream",
      "GridFSDownloadStream"
    )

    val packageObjects =
      currentMirror.staticPackage(scalaPackageName).info.decls.filter(!_.isImplicit).map(_.name.toString).toSet
    val local = new Reflections(scalaPackageName, new SubTypesScanner(false))
      .getSubTypesOf(classOf[Object])
      .asScala
      .filter(classFilter)
      .filter(f => f.getPackage.getName == scalaPackageName)
      .map(_.getSimpleName)
      .toSet ++ packageObjects -- scalaExclusions

    diff(local, wrapped) shouldBe empty
  }

  def diff(a: Set[String], b: Set[String]): Set[String] = a.diff(b) ++ b.diff(a)
}
