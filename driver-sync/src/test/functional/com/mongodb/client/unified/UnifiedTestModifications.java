/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.unified;

import com.mongodb.ClusterFixture;
import org.opentest4j.AssertionFailedError;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.serverVersionLessThan;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.client.unified.UnifiedTestModifications.Modifier.IGNORE_EXTRA_EVENTS;
import static com.mongodb.client.unified.UnifiedTestModifications.Modifier.RETRY;
import static com.mongodb.client.unified.UnifiedTestModifications.Modifier.SKIP;
import static com.mongodb.client.unified.UnifiedTestModifications.Modifier.SLEEP_AFTER_CURSOR_CLOSE;
import static com.mongodb.client.unified.UnifiedTestModifications.Modifier.SLEEP_AFTER_CURSOR_OPEN;
import static com.mongodb.client.unified.UnifiedTestModifications.Modifier.WAIT_FOR_BATCH_CURSOR_CREATION;
import static java.lang.String.format;

public final class UnifiedTestModifications {
    public static void applyCustomizations(final TestDef def) {

        // change-streams
        def.skipNoncompliantReactive("error required from change stream initialization") // TODO-JAVA-5711 reason?
                .test("change-streams", "change-streams", "Test with document comment - pre 4.4");
        def.skipNoncompliantReactive("event sensitive tests. We can't guarantee the amount of GetMore commands sent in the reactive driver")
                .test("change-streams", "change-streams", "Test that comment is set on getMore")
                .test("change-streams", "change-streams", "Test that comment is not set on getMore - pre 4.4");
        def.modify(IGNORE_EXTRA_EVENTS)
                .test("change-streams", "change-streams", "Test with document comment")
                .test("change-streams", "change-streams", "Test with string comment");
        def.modify(SLEEP_AFTER_CURSOR_OPEN)
                .directory("change-streams");
        def.modify(WAIT_FOR_BATCH_CURSOR_CREATION)
                .test("change-streams", "change-streams-errors",
                        "Change Stream should error when an invalid aggregation stage is passed in")
                .test("change-streams", "change-streams-errors",
                        "The watch helper must not throw a custom exception when executed against a single server topology, but instead depend on a server error");

        // Client side encryption (QE)
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5675 Support QE with Client.bulkWrite")
                .file("client-side-encryption/tests/unified", "client bulkWrite with queryable encryption");

        // client-side-operation-timeout (CSOT)
        def.retry("Unified CSOT tests do not account for RTT which varies in TLS vs non-TLS runs")
                .whenFailureContains("timeout")
                .test("client-side-operations-timeout",
                        "timeoutMS behaves correctly for non-tailable cursors",
                        "timeoutMS is refreshed for getMore if timeoutMode is iteration - success");

        def.retry("Unified CSOT tests do not account for RTT which varies in TLS vs non-TLS runs")
                .whenFailureContains("timeout")
                .test("client-side-operations-timeout",
                        "timeoutMS behaves correctly for tailable non-awaitData cursors",
                        "timeoutMS is refreshed for getMore - success");

        def.retry("Unified CSOT tests do not account for RTT which varies in TLS vs non-TLS runs")
                .whenFailureContains("timeout")
                .test("client-side-operations-timeout",
                        "timeoutMS behaves correctly for tailable non-awaitData cursors",
                        "timeoutMS is refreshed for getMore - success");

        //TODO-invistigate
        /*
          As to the background connection pooling section:
         timeoutMS set at the MongoClient level MUST be used as the timeout for all commands sent as part of the handshake.
         We first configure a failpoint to block all hello/isMaster commands for 50 ms, then set timeoutMS = 10 ms on MongoClient
         and wait for awaitMinPoolSize = 1000. So that means the background thread tries to populate connections under a 10ms timeout
         cap while the failpoint blocks for 50ms, so all attempts effectively fail.
         */
        def.skipAccordingToSpec("background connection pooling section")
                .test("client-side-operations-timeout", "timeoutMS behaves correctly during command execution",
                        "short-circuit is not enabled with only 1 RTT measurement")
                .test("client-side-operations-timeout", "timeoutMS behaves correctly during command execution",
                        "command is not sent if RTT is greater than timeoutMS");
        def.skipNoncompliantReactive("No good way to fulfill tryNext() requirement with a Publisher<T>")
                .test("client-side-operations-timeout", "timeoutMS behaves correctly for tailable awaitData cursors",
                        "apply remaining timeoutMS if less than maxAwaitTimeMS");

        def.skipNoncompliantReactive("No good way to fulfill tryNext() requirement with a Publisher<T>")
                .test("client-side-operations-timeout", "timeoutMS behaves correctly for tailable awaitData cursors",
                        "apply maxAwaitTimeMS if less than remaining timeout");

        def.skipJira("https://jira.mongodb.org/browse/JAVA-5839")
                .test("client-side-operations-timeout", "timeoutMS behaves correctly for GridFS download operations",
                        "timeoutMS applied to entire download, not individual parts");

        def.skipJira("https://jira.mongodb.org/browse/JAVA-5491")
                .when(() -> !serverVersionLessThan(8, 3))
                .test("client-side-operations-timeout", "operations ignore deprecated timeout options if timeoutMS is set",
                        "socketTimeoutMS is ignored if timeoutMS is set - dropIndex on collection")
                .test("client-side-operations-timeout", "operations ignore deprecated timeout options if timeoutMS is set",
                        "wTimeoutMS is ignored if timeoutMS is set - dropIndex on collection")
                .test("client-side-operations-timeout", "operations ignore deprecated timeout options if timeoutMS is set",
                        "maxTimeMS is ignored if timeoutMS is set - dropIndex on collection")
                .test("client-side-operations-timeout", "operations ignore deprecated timeout options if timeoutMS is set",
                        "socketTimeoutMS is ignored if timeoutMS is set - dropIndexes on collection")
                .test("client-side-operations-timeout", "operations ignore deprecated timeout options if timeoutMS is set",
                        "wTimeoutMS is ignored if timeoutMS is set - dropIndexes on collection")
                .test("client-side-operations-timeout", "operations ignore deprecated timeout options if timeoutMS is set",
                        "maxTimeMS is ignored if timeoutMS is set - dropIndexes on collection")
                .test("client-side-operations-timeout", "timeoutMS can be overridden for a MongoDatabase",
                        "timeoutMS can be configured on a MongoDatabase - dropIndex on collection")
                .test("client-side-operations-timeout", "timeoutMS can be overridden for a MongoDatabase",
                        "timeoutMS can be set to 0 on a MongoDatabase - dropIndex on collection")
                .test("client-side-operations-timeout", "timeoutMS can be overridden for a MongoDatabase",
                        "timeoutMS can be configured on a MongoDatabase - dropIndexes on collection")
                .test("client-side-operations-timeout", "timeoutMS can be overridden for a MongoDatabase",
                        "timeoutMS can be set to 0 on a MongoDatabase - dropIndexes on collection")
                .test("client-side-operations-timeout", "timeoutMS can be overridden for a MongoCollection",
                        "timeoutMS can be configured on a MongoCollection - dropIndex on collection")
                .test("client-side-operations-timeout", "timeoutMS can be overridden for a MongoCollection",
                        "timeoutMS can be set to 0 on a MongoCollection - dropIndex on collection")
                .test("client-side-operations-timeout", "timeoutMS can be overridden for a MongoCollection",
                        "timeoutMS can be configured on a MongoCollection - dropIndexes on collection")
                .test("client-side-operations-timeout", "timeoutMS can be overridden for a MongoCollection",
                        "timeoutMS can be set to 0 on a MongoCollection - dropIndexes on collection")
                .test("client-side-operations-timeout", "timeoutMS can be overridden for an operation",
                        "timeoutMS can be configured for an operation - dropIndex on collection")
                .test("client-side-operations-timeout", "timeoutMS can be overridden for an operation",
                        "timeoutMS can be set to 0 for an operation - dropIndex on collection")
                .test("client-side-operations-timeout", "timeoutMS can be overridden for an operation",
                        "timeoutMS can be configured for an operation - dropIndexes on collection")
                .test("client-side-operations-timeout", "timeoutMS can be overridden for an operation",
                        "timeoutMS can be set to 0 for an operation - dropIndexes on collection")
                .test("client-side-operations-timeout", "timeoutMS can be configured on a MongoClient",
                        "timeoutMS can be configured on a MongoClient - dropIndex on collection")
                .test("client-side-operations-timeout", "timeoutMS can be configured on a MongoClient",
                        "timeoutMS can be set to 0 on a MongoClient - dropIndex on collection")
                .test("client-side-operations-timeout", "timeoutMS can be configured on a MongoClient",
                        "timeoutMS can be configured on a MongoClient - dropIndexes on collection")
                .test("client-side-operations-timeout", "timeoutMS can be configured on a MongoClient",
                        "timeoutMS can be set to 0 on a MongoClient - dropIndexes on collection")
                .test("client-side-operations-timeout", "operations ignore deprecated timeout options if timeoutMS is set",
                        "socketTimeoutMS is ignored if timeoutMS is set - dropIndex on collection")
                .test("client-side-operations-timeout", "operations ignore deprecated timeout options if timeoutMS is set",
                        "wTimeoutMS is ignored if timeoutMS is set - dropIndex on collection")
                .test("client-side-operations-timeout", "operations ignore deprecated timeout options if timeoutMS is set",
                        "maxTimeMS is ignored if timeoutMS is set - dropIndex on collection")
                .test("client-side-operations-timeout", "operations ignore deprecated timeout options if timeoutMS is set",
                        "socketTimeoutMS is ignored if timeoutMS is set - dropIndexes on collection")
                .test("client-side-operations-timeout", "operations ignore deprecated timeout options if timeoutMS is set",
                        "wTimeoutMS is ignored if timeoutMS is set - dropIndexes on collection")
                .test("client-side-operations-timeout", "operations ignore deprecated timeout options if timeoutMS is set",
                        "maxTimeMS is ignored if timeoutMS is set - dropIndexes on collection")
                .test("client-side-operations-timeout", "timeoutMS can be overridden for a MongoDatabase",
                        "timeoutMS can be configured on a MongoDatabase - dropIndex on collection")
                .test("client-side-operations-timeout", "timeoutMS can be overridden for a MongoDatabase",
                        "timeoutMS can be set to 0 on a MongoDatabase - dropIndex on collection")
                .test("client-side-operations-timeout", "timeoutMS can be overridden for a MongoDatabase",
                        "timeoutMS can be configured on a MongoDatabase - dropIndexes on collection")
                .test("client-side-operations-timeout", "timeoutMS can be overridden for a MongoDatabase",
                        "timeoutMS can be set to 0 on a MongoDatabase - dropIndexes on collection")
                .test("client-side-operations-timeout", "timeoutMS can be overridden for a MongoCollection",
                        "timeoutMS can be configured on a MongoCollection - dropIndex on collection")
                .test("client-side-operations-timeout", "timeoutMS can be overridden for a MongoCollection",
                        "timeoutMS can be set to 0 on a MongoCollection - dropIndex on collection")
                .test("client-side-operations-timeout", "timeoutMS can be overridden for a MongoCollection",
                        "timeoutMS can be configured on a MongoCollection - dropIndexes on collection")
                .test("client-side-operations-timeout", "timeoutMS can be overridden for a MongoCollection",
                        "timeoutMS can be set to 0 on a MongoCollection - dropIndexes on collection")
                .test("client-side-operations-timeout", "timeoutMS can be overridden for an operation",
                        "timeoutMS can be configured for an operation - dropIndex on collection")
                .test("client-side-operations-timeout", "timeoutMS can be overridden for an operation",
                        "timeoutMS can be set to 0 for an operation - dropIndex on collection")
                .test("client-side-operations-timeout", "timeoutMS can be overridden for an operation",
                        "timeoutMS can be configured for an operation - dropIndexes on collection")
                .test("client-side-operations-timeout", "timeoutMS can be overridden for an operation",
                        "timeoutMS can be set to 0 for an operation - dropIndexes on collection")
                .test("client-side-operations-timeout", "timeoutMS can be configured on a MongoClient",
                        "timeoutMS can be configured on a MongoClient - dropIndex on collection")
                .test("client-side-operations-timeout", "timeoutMS can be configured on a MongoClient",
                        "timeoutMS can be set to 0 on a MongoClient - dropIndex on collection")
                .test("client-side-operations-timeout", "timeoutMS can be configured on a MongoClient",
                        "timeoutMS can be configured on a MongoClient - dropIndexes on collection")
                .test("client-side-operations-timeout", "timeoutMS can be configured on a MongoClient",
                        "timeoutMS can be set to 0 on a MongoClient - dropIndexes on collection");

        // OpenTelemetry
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5991")
                .file("open-telemetry/tests", "operation find")
                .file("open-telemetry/tests", "operation find_one_and_update")
                .file("open-telemetry/tests", "operation update")
                .file("open-telemetry/tests", "operation bulk_write")
                .file("open-telemetry/tests", "operation drop collection")
                .file("open-telemetry/tests", "transaction spans")
                .file("open-telemetry/tests", "convenient transactions")
                .file("open-telemetry/tests", "operation atlas_search")
                .file("open-telemetry/tests", "operation insert")
                .file("open-telemetry/tests", "operation map_reduce")
                .file("open-telemetry/tests", "operation find without db.query.text")
                .file("open-telemetry/tests", "operation find_retries");

        def.skipAccordingToSpec("Micrometer tests expect the network transport to be tcp")
                .when(ClusterFixture::isUnixSocket)
                .directory("open-telemetry/tests");

        // TODO-JAVA-5712

        // collection-management

        def.skipNoncompliant("") // TODO-JAVA-5711 reason?
                .test("collection-management", "modifyCollection-pre_and_post_images",
                        "modifyCollection to changeStreamPreAndPostImages enabled");

        // command-logging-and-monitoring

        def.skipNoncompliant("The driver has a hack where getLastError command "
                        + "is executed as part of the handshake in order to "
                        + "get a connectionId even when the hello command "
                        + "response doesn't contain it.")
                .file("command-logging-and-monitoring/tests/logging", "pre-42-server-connection-id")
                .file("command-logging-and-monitoring/tests/monitoring", "pre-42-server-connection-id");

        def.skipNoncompliant("The driver doesn't reduce the batchSize for the getMore")
                .test("command-logging-and-monitoring/tests/monitoring", "find",
                        "A successful find event with a getmore and the server kills the cursor (<= 4.4)");

        // connection-monitoring-and-pooling
        def.skipNoncompliant("According to the test, we should clear the pool then close the connection. Our implementation"
                        + "immediately closes the failed connection, then clears the pool.")
                .test("connection-monitoring-and-pooling/tests/logging", "connection-logging",
                        "Connection checkout fails due to error establishing connection");


        def.skipNoncompliant("Driver does not support waitQueueSize or waitQueueMultiple options")
                .test("connection-monitoring-and-pooling/tests/logging", "connection-pool-options",
                        "waitQueueSize should be included in connection pool created message when specified")
                .test("connection-monitoring-and-pooling/tests/logging", "connection-pool-options",
                        "waitQueueMultiple should be included in connection pool created message when specified");

        // load-balancers

        def.modify(SLEEP_AFTER_CURSOR_OPEN)
                .test("load-balancers", "state change errors are correctly handled",
                        "only connections for a specific serviceId are closed when pools are cleared")
                .test("load-balancers", "state change errors are correctly handled", "stale errors are ignored")
                .test("load-balancers", "cursors are correctly pinned to connections for load-balanced clusters",
                        "pinned connections are returned when the cursor is drained")
                .test("load-balancers", "cursors are correctly pinned to connections for load-balanced clusters",
                        "pinned connections are returned to the pool when the cursor is closed")
                .test("load-balancers", "cursors are correctly pinned to connections for load-balanced clusters",
                        "no connection is pinned if all documents are returned in the initial batch")
                .test("load-balancers", "transactions are correctly pinned to connections for load-balanced clusters",
                        "a connection can be shared by a transaction and a cursor")
                .test("load-balancers", "wait queue timeout errors include details about checked out connections",
                        "wait queue timeout errors include cursor statistics");
        def.modify(SLEEP_AFTER_CURSOR_CLOSE)
                .test("load-balancers", "state change errors are correctly handled",
                        "only connections for a specific serviceId are closed when pools are cleared")
                .test("load-balancers", "cursors are correctly pinned to connections for load-balanced clusters",
                        "pinned connections are returned to the pool when the cursor is closed")
                .test("load-balancers", "cursors are correctly pinned to connections for load-balanced clusters",
                        "pinned connections are returned after a network error during a killCursors request")
                .test("load-balancers", "transactions are correctly pinned to connections for load-balanced clusters",
                        "a connection can be shared by a transaction and a cursor");
        def.skipNoncompliantReactive("Reactive streams driver can't implement "
                        + "these tests because the underlying cursor is closed "
                        + "on error, which  breaks assumption in the tests that "
                        + "closing the cursor is something that happens under "
                        + "user control")
                .test("load-balancers", "cursors are correctly pinned to connections for load-balanced clusters",
                        "pinned connections are not returned after an network error during getMore")
                .test("load-balancers", "cursors are correctly pinned to connections for load-balanced clusters",
                        "pinned connections are not returned to the pool after a non-network error on getMore");
        def.skipNoncompliantReactive("Reactive streams driver can't implement "
                        + "this test because there is no way to tell that a "
                        + "change stream cursor that has not yet received any "
                        + "results has even initiated the change stream")
                .test("load-balancers", "cursors are correctly pinned to connections for load-balanced clusters",
                        "change streams pin to a connection");

        // crud

        def.skipDeprecated(
                        "Deprecated count method removed, cf https://github.com/mongodb/mongo-java-driver/pull/1328#discussion_r1513641410")
                .test("crud", "count-empty", "Deprecated count with empty collection")
                .test("crud", "count-collation", "Deprecated count with collation")
                .test("crud", "count", "Deprecated count without a filter")
                .test("crud", "count", "Deprecated count with a filter")
                .test("crud", "count", "Deprecated count with skip and limit");
        def.skipUnknownReason("See downstream changes comment on https://jira.mongodb.org/browse/JAVA-4275")
                .test("crud", "findOneAndReplace-hint-unacknowledged", "Unacknowledged findOneAndReplace with hint string on 4.4+ server")
                .test("crud", "findOneAndReplace-hint-unacknowledged", "Unacknowledged findOneAndReplace with hint document on 4.4+ server")
                .test("crud", "findOneAndUpdate-hint-unacknowledged", "Unacknowledged findOneAndUpdate with hint string on 4.4+ server")
                .test("crud", "findOneAndUpdate-hint-unacknowledged", "Unacknowledged findOneAndUpdate with hint document on 4.4+ server")
                .test("crud", "findOneAndDelete-hint-unacknowledged", "Unacknowledged findOneAndDelete with hint string on 4.4+ server")
                .test("crud", "findOneAndDelete-hint-unacknowledged", "Unacknowledged findOneAndDelete with hint document on 4.4+ server");

        def.skipNoncompliant("https://jira.mongodb.org/browse/JAVA-5838")
                .when(() -> def.isReactive() && UnifiedTest.Language.KOTLIN.equals(def.getLanguage()))
                .file("crud", "findOne");

        def.skipNoncompliant("Scala Mono pulls the data and sets the batch size https://jira.mongodb.org/browse/JAVA-5838")
                .when(() -> UnifiedTest.Language.SCALA.equals(def.getLanguage()))
                .file("crud", "findOne");

        def.skipNoncompliant("Updates and Replace bulk operations are split in the java driver")
                .file("crud", "bulkWrite-comment");

        // gridfs

        def.skipDeprecated("contentType is deprecated in GridFS spec, and 4.x Java driver no longer supports it")
                .test("gridfs", "gridfs-upload", "upload when contentType is provided");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-4214")
                .test("gridfs", "gridfs-delete", "delete when files entry does not exist and there are orphaned chunks");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5677")
                .file("gridfs", "gridfs-rename");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5689")
                .file("gridfs", "gridfs-deleteByName")
                .file("gridfs", "gridfs-renameByName");

        // Skip all rawData based tests
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5830 rawData support only added to Go and Node")
                .file("collection-management", "listCollections-rawData")
                .file("crud", "aggregate-rawData")
                .file("crud", "aggregate-rawData")
                .file("crud", "BulkWrite deleteMany-rawData")
                .file("crud", "BulkWrite deleteOne-rawData")
                .file("crud", "BulkWrite replaceOne-rawData")
                .file("crud", "BulkWrite updateMany-rawData")
                .file("crud", "BulkWrite updateOne-rawData")
                .file("crud", "client bulkWrite delete-rawData")
                .file("crud", "client bulkWrite replaceOne-rawData")
                .file("crud", "client bulkWrite update-rawData")
                .file("crud", "count-rawData")
                .file("crud", "countDocuments-rawData")
                .file("crud", "db-aggregate-rawdata")
                .file("crud", "deleteMany-rawData")
                .file("crud", "deleteOne-rawData")
                .file("crud", "distinct-rawData")
                .file("crud", "estimatedDocumentCount-rawData")
                .file("crud", "find-rawData")
                .file("crud", "findOneAndDelete-rawData")
                .file("crud", "findOneAndReplace-rawData")
                .file("crud", "findOneAndUpdate-rawData")
                .file("crud", "insertMany-rawData")
                .file("crud", "insertOne-rawData")
                .file("crud", "replaceOne-rawData")
                .file("crud", "updateMany-rawData")
                .file("crud", "updateOne-rawData")
                .file("index-management", "index management-rawData");

        // retryable-reads

        def.modify(WAIT_FOR_BATCH_CURSOR_CREATION, IGNORE_EXTRA_EVENTS)
                //.testContains("retryable-reads", "ChangeStream")
                .test("retryable-reads", "retryable reads handshake failures",
                        "client.createChangeStream succeeds after retryable handshake network error")
                .test("retryable-reads", "retryable reads handshake failures",
                        "client.createChangeStream succeeds after retryable handshake server error (ShutdownInProgress)")
                .test("retryable-reads", "retryable reads handshake failures",
                        "database.createChangeStream succeeds after retryable handshake network error")
                .test("retryable-reads", "retryable reads handshake failures",
                        "database.createChangeStream succeeds after retryable handshake server error (ShutdownInProgress)")
                .test("retryable-reads", "retryable reads handshake failures",
                        "collection.createChangeStream succeeds after retryable handshake network error")
                .test("retryable-reads", "retryable reads handshake failures",
                        "collection.createChangeStream succeeds after retryable handshake server error (ShutdownInProgress)");
        def.modify(WAIT_FOR_BATCH_CURSOR_CREATION, IGNORE_EXTRA_EVENTS)
                .file("retryable-reads", "changeStreams-client.watch-serverErrors")
                .file("retryable-reads", "changeStreams-client.watch")
                .file("retryable-reads", "changeStreams-db.coll.watch-serverErrors")
                .file("retryable-reads", "changeStreams-db.coll.watch")
                .file("retryable-reads", "changeStreams-db.watch-serverErrors")
                .file("retryable-reads", "changeStreams-db.watch");
        def.skipDeprecated("Deprecated feature removed")
                .file("retryable-reads", "count")
                .file("retryable-reads", "count-serverErrors");
        def.skipDeprecated("Deprecated feature never implemented")
                .file("retryable-reads", "listDatabaseObjects")
                .file("retryable-reads", "listDatabaseObjects-serverErrors")
                .file("retryable-reads", "listCollectionObjects")
                .file("retryable-reads", "listCollectionObjects-serverErrors");

        // retryable-writes

        def.skipJira("https://jira.mongodb.org/browse/JAVA-5125")
                .when(() -> isSharded() && serverVersionLessThan(5, 0))
                //.testContains("retryable-writes", "succeeds after WriteConcernError")
                .test("retryable-writes", "bulkWrite-errorLabels", "BulkWrite succeeds after WriteConcernError ShutdownInProgress")
                .test("retryable-writes", "updateOne-errorLabels", "UpdateOne succeeds after WriteConcernError ShutdownInProgress")
                .test("retryable-writes", "deleteOne-errorLabels", "DeleteOne succeeds after WriteConcernError ShutdownInProgress")
                .test("retryable-writes", "insertOne-errorLabels", "InsertOne succeeds after WriteConcernError InterruptedAtShutdown")
                .test("retryable-writes", "insertOne-errorLabels",
                        "InsertOne succeeds after WriteConcernError InterruptedDueToReplStateChange")
                .test("retryable-writes", "insertOne-errorLabels", "InsertOne succeeds after WriteConcernError PrimarySteppedDown")
                .test("retryable-writes", "insertOne-errorLabels", "InsertOne succeeds after WriteConcernError ShutdownInProgress")
                .test("retryable-writes", "insertMany-errorLabels", "InsertMany succeeds after WriteConcernError ShutdownInProgress")
                .test("retryable-writes", "replaceOne-errorLabels", "ReplaceOne succeeds after WriteConcernError ShutdownInProgress")
                .test("retryable-writes", "findOneAndUpdate-errorLabels",
                        "FindOneAndUpdate succeeds after WriteConcernError ShutdownInProgress")
                .test("retryable-writes", "findOneAndDelete-errorLabels",
                        "FindOneAndDelete succeeds after WriteConcernError ShutdownInProgress")
                .test("retryable-writes", "findOneAndReplace-errorLabels",
                        "FindOneAndReplace succeeds after WriteConcernError ShutdownInProgress")
                //.testContains("retryable-writes", "succeeds after retryable writeConcernError")
                .test("retryable-writes", "retryable-writes insertOne serverErrors", "InsertOne succeeds after retryable writeConcernError")
                .test("retryable-writes", "retryable-writes bulkWrite serverErrors",
                        "BulkWrite succeeds after retryable writeConcernError in first batch");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5341")
                .when(() -> isDiscoverableReplicaSet() && serverVersionLessThan(4, 4))
                .test("retryable-writes", "retryable-writes insertOne serverErrors",
                        "RetryableWriteError label is added based on writeConcernError in pre-4.4 mongod response");

        // server-discovery-and-monitoring (SDAM)

        def.skipJira("https://jira.mongodb.org/browse/JAVA-5230")
                .test("server-discovery-and-monitoring", "serverMonitoringMode", "connect with serverMonitoringMode=auto >=4.4")
                .test("server-discovery-and-monitoring", "serverMonitoringMode", "connect with serverMonitoringMode=stream >=4.4");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5564")
                .test("server-discovery-and-monitoring", "serverMonitoringMode", "poll waits after successful heartbeat");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-4536")
                .file("server-discovery-and-monitoring", "interruptInUse");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5664")
                .file("server-discovery-and-monitoring", "pool-clear-application-error");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5664")
                .file("server-discovery-and-monitoring", "pool-clear-on-error-checkout");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5664")
                .file("server-discovery-and-monitoring", "pool-cleared-on-min-pool-size-population-error");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5949")
                .file("server-discovery-and-monitoring", "backpressure-network-error-fail-single");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5949")
                .file("server-discovery-and-monitoring", "backpressure-network-timeout-error-single");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5949")
                .file("server-discovery-and-monitoring", "backpressure-network-error-fail-replicaset");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5949")
                .file("server-discovery-and-monitoring", "backpressure-network-timeout-error-replicaset");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5949")
                .file("server-discovery-and-monitoring", "backpressure-server-description-unchanged-on-min-pool-size-population-error");

        // session tests
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5968")
                .test("sessions", "snapshot-sessions", "Find operation with snapshot and snapshot time");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5968")
                .test("sessions", "snapshot-sessions", "Distinct operation with snapshot and snapshot time");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5968")
                .test("sessions", "snapshot-sessions", "Aggregate operation with snapshot and snapshot time");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5968")
                .test("sessions", "snapshot-sessions", "countDocuments operation with snapshot and snapshot time");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5968")
                .test("sessions", "snapshot-sessions", "Mixed operation with snapshot and snapshotTime");

        // transactions

        def.skipDeprecated("Deprecated feature removed")
                .file("transactions", "count");
        def.skipDeprecated("Only affects 4.2, which is EOL, see https://github.com/mongodb/mongo-java-driver/pull/1310/files#r1491812405")
                .when(() -> serverVersionLessThan(4, 4) && isSharded())
                .test("transactions", "pin-mongos", "distinct")
                .test("transactions", "read-concern", "only first distinct includes readConcern")
                .test("transactions", "read-concern", "distinct ignores collection readConcern")
                .test("transactions", "reads", "distinct");
        def.skipNoncompliant("`MongoCluster.getWriteConcern`/`MongoCollection.getWriteConcern` are silently ignored in a transaction")
                .test("transactions", "client bulkWrite transactions",
                        "client bulkWrite with writeConcern in a transaction causes a transaction error");

        // valid-pass

        def.skipDeprecated("MongoDB releases prior to 4.4 incorrectly add "
                        + "errorLabels as a field within the writeConcernError "
                        + "document instead of as a top-level field. Rather "
                        + "than handle that in code, we skip the test on older "
                        + "server versions.")
                .when(() -> serverVersionLessThan(4, 4))
                .test("unified-test-format/tests/valid-pass", "poc-retryable-writes",
                        "InsertOne fails after multiple retryable writeConcernErrors");

        def.skipNoncompliant("The driver doesn't reduce the batchSize for the getMore")
                .test("unified-test-format/tests/valid-pass", "poc-command-monitoring",
                        "A successful find event with a getmore and the server kills the cursor (<= 4.4)");

        def.skipJira("https://jira.mongodb.org/browse/JAVA-5389")
                .file("unified-test-format/tests/valid-pass", "expectedEventsForClient-topologyDescriptionChangedEvent");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-4862")
                .file("unified-test-format/tests/valid-pass", "entity-commandCursor");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5631")
                .file("unified-test-format/tests/valid-pass", "kmsProviders-explicit_kms_credentials")
                .file("unified-test-format/tests/valid-pass", "kmsProviders-mixed_kms_credential_fields");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5672")
                .file("unified-test-format/tests/valid-pass", "operator-matchAsRoot");

        // valid fail

        def.skipJira("https://jira.mongodb.org/browse/JAVA-5672")
                .file("unified-test-format/tests/valid-fail", "operator-matchAsDocument");
    }

    private UnifiedTestModifications() {
    }

    public static TestDef testDef(final String dir, final String file, final String test, final boolean reactive,
                                  final UnifiedTest.Language language) {
        return new TestDef(dir, file, test, reactive, language);
    }

    public static final class TestDef {

        private final String dir;
        private final String file;
        private final String test;
        private final boolean reactive;
        private final UnifiedTest.Language language;

        private final List<Modifier> modifiers = new ArrayList<>();
        private Function<Throwable, Boolean> matchesThrowable;

        private TestDef(final String dir, final String file, final String test, final boolean reactive,
                        final UnifiedTest.Language language) {
            this.dir = assertNotNull(dir);
            this.file = assertNotNull(file);
            this.test = assertNotNull(test);
            this.reactive = reactive;
            this.language = assertNotNull(language);
        }

        @Override
        public String toString() {
            return "TestDef{"
                    + "modifiers=" + modifiers
                    + ", reactive=" + reactive
                    + ", test='" + test + '\''
                    + ", file='" + file + '\''
                    + ", dir='" + dir + '\''
                    + '}';
        }

        /**
         * Test is skipped because it is pending implementation, and there is
         * a Jira ticket tracking this which has more information.
         *
         * @param ticket reason for skipping the test; must start with a Jira URL
         */
        public TestApplicator skipJira(final String ticket) {
            assertTrue(ticket.startsWith("https://jira.mongodb.org/browse/JAVA-"));
            return new TestApplicator(this, ticket, SKIP);
        }

        /**
         * Test is skipped because the feature under test was deprecated, and
         * was removed in the Java driver.
         *
         * @param reason reason for skipping the test
         */
        public TestApplicator skipDeprecated(final String reason) {
            return new TestApplicator(this, reason, SKIP);
        }

        /**
         * Test is skipped because the Java driver cannot comply with the spec.
         *
         * @param reason reason for skipping the test
         */
        public TestApplicator skipNoncompliant(final String reason) {
            return new TestApplicator(this, reason, SKIP);
        }

        /**
         * Test is skipped because the Java Reactive driver cannot comply with the spec.
         *
         * @param reason reason for skipping the test
         */
        public TestApplicator skipNoncompliantReactive(final String reason) {
            return new TestApplicator(this, reason, SKIP)
                    .when(this::isReactive);
        }

        /**
         * The test is skipped, as specified. This should be paired with a
         * "when" clause.
         */
        public TestApplicator skipAccordingToSpec(final String reason) {
            return new TestApplicator(this, reason, SKIP);
        }

        /**
         * The test is skipped for an unknown reason.
         */
        public TestApplicator skipUnknownReason(final String reason) {
            return new TestApplicator(this, reason, SKIP);
        }

        /**
         * The test will be retried, for the reason provided
         */
        public TestApplicator retry(final String reason) {
            return new TestApplicator(this, reason, RETRY);
        }

        /**
         * The reactive test will be retried, for the reason provided
         */
        public TestApplicator retryReactive(final String reason) {
            return new TestApplicator(this, reason, RETRY)
                    .when(this::isReactive);
        }

        public TestApplicator modify(final Modifier... modifiers) {
            return new TestApplicator(this, null, modifiers);
        }

        public boolean isReactive() {
            return reactive;
        }

        public UnifiedTest.Language getLanguage() {
            return language;
        }

        public boolean wasAssignedModifier(final Modifier modifier) {
            return this.modifiers.contains(modifier);
        }

        public boolean matchesThrowable(final Throwable e) {
            if (matchesThrowable != null) {
                return matchesThrowable.apply(e);
            }
            return false;
        }
    }

    /**
     * Applies settings to the underlying test definition. Chainable.
     */
    public static final class TestApplicator {
        private final TestDef testDef;
        private Supplier<Boolean> precondition;
        private boolean matchWasPerformed = false;

        private final List<Modifier> modifiersToApply;
        private Function<Throwable, Boolean> matchesThrowable;

        private TestApplicator(
                final TestDef testDef,
                final String reason,
                final Modifier... modifiersToApply) {
            this.testDef = testDef;
            this.modifiersToApply = Arrays.asList(modifiersToApply);
            if (this.modifiersToApply.contains(SKIP) || this.modifiersToApply.contains(RETRY)) {
                assertNotNull(reason);
            }
        }

        private TestApplicator onMatch(final boolean match) {
            matchWasPerformed = true;
            if (precondition != null && !precondition.get()) {
                return this;
            }
            if (match) {
                this.testDef.modifiers.addAll(this.modifiersToApply);
                this.testDef.matchesThrowable = this.matchesThrowable;
            }
            return this;
        }

        /**
         * Applies to all tests in directory.
         *
         * @param dir the directory name
         * @return this
         */
        public TestApplicator directory(final String dir) {
            boolean match = (dir).equals(testDef.dir);
            return onMatch(match);
        }

        /**
         * Applies to all tests in file under the directory.
         *
         * @param dir  the directory name
         * @param file the test file's "description" field
         * @return this
         */
        public TestApplicator file(final String dir, final String file) {
            boolean match = (dir).equals(testDef.dir)
                    && file.equals(testDef.file);
            return onMatch(match);
        }

        /**
         * Applies to the test where dir, file, and test match.
         *
         * @param dir  the directory name
         * @param file the test file's "description" field
         * @param test the individual test's "description" field
         * @return this
         */
        public TestApplicator test(final String dir, final String file, final String test) {
            boolean match = testDef.dir.equals(dir)
                    && testDef.file.equals(file)
                    && testDef.test.equals(test);
            return onMatch(match);
        }

        /**
         * Utility method: emit replacement to standard out.
         *
         * @param dir      the directory name
         * @param fragment the substring to check in the test "description" field
         * @return this
         */
        public TestApplicator testContains(final String dir, final String fragment) {
            boolean match = (dir).equals(testDef.dir)
                    && testDef.test.contains(fragment);
            if (match) {
                System.out.printf(
                        "!!! REPLACE %s WITH: .test(\"%s\", \"%s\", \"%s\")%n",
                        fragment,
                        testDef.dir,
                        testDef.file,
                        testDef.test);
            }
            return this;
        }

        /**
         * Utility method: emit file info to standard out
         *
         * @param dir  the directory name
         * @param test the individual test's "description" field
         * @return this
         */
        public TestApplicator debug(final String dir, final String test) {
            boolean match = testDef.test.equals(test);
            if (match) {
                System.out.printf(
                        "!!! ADD: \"%s\", \"%s\", \"%s\"%n",
                        testDef.dir, testDef.file, test);
            }
            return this;
        }

        /**
         * Ensuing matching methods are applied only when the condition is met.
         * For example, if tests should only be skipped (or modified) on
         * sharded clusters, check for sharded in the condition.
         * Must be the first method called in the chain.
         *
         * @param precondition the condition; methods are no-op when false.
         * @return this
         */
        public TestApplicator when(final Supplier<Boolean> precondition) {
            if (this.precondition != null || this.matchWasPerformed) {
                throw new IllegalStateException("Condition must be specified first and once.");
            }
            this.precondition = precondition;
            return this;
        }

        /**
         * The modification, if it is a RETRY, will only be applied when the
         * failure message contains the provided message fragment. If an
         * {@code AssertionFailedError} occurs, and has a cause, the cause's
         * message will be checked. Otherwise, the throwable will be checked.
         */
        public TestApplicator whenFailureContains(final String messageFragment) {
            assertTrue(this.modifiersToApply.contains(RETRY),
                    format("Modifier %s was not specified before calling whenFailureContains", RETRY));
            this.matchesThrowable = (final Throwable e) -> {
                // inspect the cause for failed assertions with a cause
                if (e instanceof AssertionFailedError && e.getCause() != null) {
                    return e.getCause().getMessage().contains(messageFragment);
                } else {
                    return e.getMessage().contains(messageFragment);
                }
            };
            return this;
        }

    }

    public enum Modifier {
        /**
         * Reactive only.
         * The reactive driver produces extra getMore commands.
         * This will ignore all extra commands, including the getMores.
         */
        IGNORE_EXTRA_EVENTS,
        /**
         * Reactive only.
         */
        SLEEP_AFTER_CURSOR_OPEN,
        /**
         * Reactive only.
         */
        SLEEP_AFTER_CURSOR_CLOSE,
        /**
         * Reactive only.
         */
        WAIT_FOR_BATCH_CURSOR_CREATION,
        /**
         * Skip the test.
         */
        SKIP,
        /**
         * Ignore results and retry the test on failure. Will not repeat the
         * test if the test succeeds. Multiple copies of the test are used to
         * facilitate retries.
         */
        RETRY,
        /**
         * The test will be retried multiple times, without the results being
         * ignored. This is a helper that can be used, in patches, to check
         * if certain tests are (still) flaky.
         */
        FORCE_FLAKY,
    }
}
