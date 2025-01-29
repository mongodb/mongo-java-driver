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

import com.mongodb.assertions.Assertions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static com.mongodb.ClusterFixture.isDataLakeTest;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isServerlessTest;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.serverVersionLessThan;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.client.unified.UnifiedTestModifications.Modifier.IGNORE_EXTRA_EVENTS;
import static com.mongodb.client.unified.UnifiedTestModifications.Modifier.SKIP;
import static com.mongodb.client.unified.UnifiedTestModifications.Modifier.SLEEP_AFTER_CURSOR_CLOSE;
import static com.mongodb.client.unified.UnifiedTestModifications.Modifier.SLEEP_AFTER_CURSOR_OPEN;
import static com.mongodb.client.unified.UnifiedTestModifications.Modifier.WAIT_FOR_BATCH_CURSOR_CREATION;

public final class UnifiedTestModifications {
    public static void doSkips(final TestDef def) {

        // atlas-data-lake

        def.skipAccordingToSpec("Data lake tests should only run on data lake")
                .when(() -> !isDataLakeTest())
                .directory("atlas-data-lake-testing");

        // change-streams
        def.skipNoncompliantReactive("error required from change stream initialization") // TODO reason?
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
                .test("change-streams", "change-streams-errors", "Change Stream should error when an invalid aggregation stage is passed in")
                .test("change-streams", "change-streams-errors", "The watch helper must not throw a custom exception when executed against a single server topology, but instead depend on a server error");

        // client-side-operation-timeout (CSOT)

        // TODO

        // collection-management

        def.skipNoncompliant("") // TODO reason?
                .test("collection-management", "modifyCollection-pre_and_post_images", "modifyCollection to changeStreamPreAndPostImages enabled");

        // command-logging-and-monitoring

        def.skipNoncompliant("TODO")
                .when(() -> !def.isReactive() && isServerlessTest()) // TODO why reactive check?
                .directory("command-logging")
                .directory("command-monitoring");

        def.skipNoncompliant("The driver has a hack where getLastError command "
                        + "is executed as part of the handshake in order to "
                        + "get a connectionId even when the hello command "
                        + "response doesn't contain it.")
                .file("command-monitoring", "pre-42-server-connection-id")
                .file("command-logging", "pre-42-server-connection-id");

        // connection-monitoring-and-pooling

        // TODO reason, jira
        // added as part of https://jira.mongodb.org/browse/JAVA-4976 , but unknown Jira to complete
        // The implementation of the functionality related to clearing the connection pool before closing the connection
        // will be carried out once the specification is finalized and ready.
        def.skipUnknownReason("")
                .test("connection-monitoring-and-pooling/logging", "connection-logging", "Connection checkout fails due to error establishing connection");

        // load-balancers

        def.modify(SLEEP_AFTER_CURSOR_OPEN)
                .test("load-balancers", "state change errors are correctly handled", "only connections for a specific serviceId are closed when pools are cleared")
                .test("load-balancers", "state change errors are correctly handled", "stale errors are ignored")
                .test("load-balancers", "cursors are correctly pinned to connections for load-balanced clusters", "pinned connections are returned when the cursor is drained")
                .test("load-balancers", "cursors are correctly pinned to connections for load-balanced clusters", "pinned connections are returned to the pool when the cursor is closed")
                .test("load-balancers", "cursors are correctly pinned to connections for load-balanced clusters", "no connection is pinned if all documents are returned in the initial batch")
                .test("load-balancers", "transactions are correctly pinned to connections for load-balanced clusters", "a connection can be shared by a transaction and a cursor")
                .test("load-balancers", "wait queue timeout errors include details about checked out connections", "wait queue timeout errors include cursor statistics");
        def.modify(SLEEP_AFTER_CURSOR_CLOSE)
                .test("load-balancers", "state change errors are correctly handled", "only connections for a specific serviceId are closed when pools are cleared")
                .test("load-balancers", "cursors are correctly pinned to connections for load-balanced clusters", "pinned connections are returned to the pool when the cursor is closed")
                .test("load-balancers", "cursors are correctly pinned to connections for load-balanced clusters", "pinned connections are returned after a network error during a killCursors request")
                .test("load-balancers", "transactions are correctly pinned to connections for load-balanced clusters", "a connection can be shared by a transaction and a cursor");
        def.skipNoncompliantReactive("Reactive streams driver can't implement "
                        + "these tests because the underlying cursor is closed "
                        + "on error, which  breaks assumption in the tests that "
                        + "closing the cursor is something that happens under "
                        + "user control")
                .test("load-balancers", "cursors are correctly pinned to connections for load-balanced clusters", "pinned connections are not returned after an network error during getMore")
                .test("load-balancers", "cursors are correctly pinned to connections for load-balanced clusters", "pinned connections are not returned to the pool after a non-network error on getMore");
        def.skipNoncompliantReactive("Reactive streams driver can't implement "
                        + "this test because there is no way to tell that a "
                        + "change stream cursor that has not yet received any "
                        + "results has even initiated the change stream")
                .test("load-balancers", "cursors are correctly pinned to connections for load-balanced clusters", "change streams pin to a connection");

        // crud

        def.skipDeprecated("Deprecated count method removed, cf https://github.com/mongodb/mongo-java-driver/pull/1328#discussion_r1513641410")
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

        // gridfs

        def.skipDeprecated("contentType is deprecated in GridFS spec, and 4.x Java driver no longer supports it")
                .test("gridfs", "gridfs-upload", "upload when contentType is provided");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-4214")
                .test("gridfs", "gridfs-delete", "delete when files entry does not exist and there are orphaned chunks");

        // retryable-reads

        def.modify(WAIT_FOR_BATCH_CURSOR_CREATION, IGNORE_EXTRA_EVENTS)
                //.testContains("retryable-reads", "ChangeStream")
                .test("retryable-reads", "retryable reads handshake failures", "client.createChangeStream succeeds after retryable handshake network error")
                .test("retryable-reads", "retryable reads handshake failures", "client.createChangeStream succeeds after retryable handshake server error (ShutdownInProgress)")
                .test("retryable-reads", "retryable reads handshake failures", "database.createChangeStream succeeds after retryable handshake network error")
                .test("retryable-reads", "retryable reads handshake failures", "database.createChangeStream succeeds after retryable handshake server error (ShutdownInProgress)")
                .test("retryable-reads", "retryable reads handshake failures", "collection.createChangeStream succeeds after retryable handshake network error")
                .test("retryable-reads", "retryable reads handshake failures", "collection.createChangeStream succeeds after retryable handshake server error (ShutdownInProgress)");
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
                .test("retryable-writes", "insertOne-errorLabels", "InsertOne succeeds after WriteConcernError InterruptedDueToReplStateChange")
                .test("retryable-writes", "insertOne-errorLabels", "InsertOne succeeds after WriteConcernError PrimarySteppedDown")
                .test("retryable-writes", "insertOne-errorLabels", "InsertOne succeeds after WriteConcernError ShutdownInProgress")
                .test("retryable-writes", "insertMany-errorLabels", "InsertMany succeeds after WriteConcernError ShutdownInProgress")
                .test("retryable-writes", "replaceOne-errorLabels", "ReplaceOne succeeds after WriteConcernError ShutdownInProgress")
                .test("retryable-writes", "findOneAndUpdate-errorLabels", "FindOneAndUpdate succeeds after WriteConcernError ShutdownInProgress")
                .test("retryable-writes", "findOneAndDelete-errorLabels", "FindOneAndDelete succeeds after WriteConcernError ShutdownInProgress")
                .test("retryable-writes", "findOneAndReplace-errorLabels", "FindOneAndReplace succeeds after WriteConcernError ShutdownInProgress")
                //.testContains("retryable-writes", "succeeds after retryable writeConcernError")
                .test("retryable-writes", "retryable-writes insertOne serverErrors", "InsertOne succeeds after retryable writeConcernError")
                .test("retryable-writes", "retryable-writes bulkWrite serverErrors", "BulkWrite succeeds after retryable writeConcernError in first batch");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5341")
                .when(() -> isDiscoverableReplicaSet() && serverVersionLessThan(4, 4))
                .test("retryable-writes", "retryable-writes insertOne serverErrors", "RetryableWriteError label is added based on writeConcernError in pre-4.4 mongod response");

        // server-discovery-and-monitoring (SDAM)

        def.skipJira("https://jira.mongodb.org/browse/JAVA-5230")
                .test("server-discovery-and-monitoring", "serverMonitoringMode", "connect with serverMonitoringMode=auto >=4.4")
                .test("server-discovery-and-monitoring", "serverMonitoringMode", "connect with serverMonitoringMode=stream >=4.4");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-4770")
                .file("server-discovery-and-monitoring", "standalone-logging")
                .file("server-discovery-and-monitoring", "replicaset-logging")
                .file("server-discovery-and-monitoring", "sharded-logging")
                .file("server-discovery-and-monitoring", "loadbalanced-logging");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5229")
                .file("server-discovery-and-monitoring", "standalone-emit-topology-description-changed-before-close")
                .file("server-discovery-and-monitoring", "replicaset-emit-topology-description-changed-before-close")
                .file("server-discovery-and-monitoring", "sharded-emit-topology-description-changed-before-close")
                .file("server-discovery-and-monitoring", "loadbalanced-emit-topology-description-changed-before-close");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5564")
                .test("server-discovery-and-monitoring", "serverMonitoringMode", "poll waits after successful heartbeat");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-4536")
                .file("server-discovery-and-monitoring", "interruptInUse");

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
                .test("valid-pass", "poc-retryable-writes", "InsertOne fails after multiple retryable writeConcernErrors");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5389")
                .file("valid-pass", "expectedEventsForClient-topologyDescriptionChangedEvent");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-4862")
                .file("valid-pass", "entity-commandCursor");
        def.skipJira("https://jira.mongodb.org/browse/JAVA-5631")
                .file("valid-pass", "kmsProviders-explicit_kms_credentials")
                .file("valid-pass", "kmsProviders-mixed_kms_credential_fields");
    }

    private UnifiedTestModifications() {}

    public static TestDef testDef(final String dir, final String file, final String test, final boolean reactive) {
        return new TestDef(dir, file, test, reactive);
    }

    public static final class TestDef {
        private final String dir;
        private final String file;
        private final String test;
        private final boolean reactive;

        private final List<Modifier> modifiers = new ArrayList<>();

        private TestDef(final String dir, final String file, final String test, final boolean reactive) {
            this.dir = assertNotNull(dir);
            this.file = assertNotNull(file);
            this.test = assertNotNull(test);
            this.reactive = reactive;
        }

        /**
         * Test is skipped because it is pending implementation, and there is
         * a Jira ticket tracking this which has more information.
         *
         * @param ticket reason for skipping the test; must start with a Jira URL
         */
        public TestApplicator skipJira(final String ticket) {
            Assertions.assertTrue(ticket.startsWith("https://jira.mongodb.org/browse/JAVA-"));
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

        public TestApplicator modify(final Modifier... modifiers) {
            return new TestApplicator(this, null, modifiers);
        }

        public boolean isReactive() {
            return reactive;
        }

        public boolean wasAssignedModifier(final Modifier modifier) {
            return this.modifiers.contains(modifier);
        }
    }

    /**
     * Applies settings to the underlying test definition. Chainable.
     */
    public static final class TestApplicator {
        private final TestDef testDef;
        private final List<Modifier> modifiersToApply;
        private Supplier<Boolean> precondition;
        private boolean matchWasPerformed = false;

        private TestApplicator(
                final TestDef testDef,
                final String reason,
                final Modifier... modifiersToApply) {
            this.testDef = testDef;
            this.modifiersToApply = Arrays.asList(modifiersToApply);
            if (this.modifiersToApply.contains(SKIP)) {
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
            }
            return this;
        }

        /**
         * Applies to all tests in directory.
         * @param dir the directory name
         * @return this
         */
        public TestApplicator directory(final String dir) {
            boolean match = ("unified-test-format/" + dir).equals(testDef.dir);
            return onMatch(match);
        }

        /**
         * Applies to all tests in file under the directory.
         * @param dir the directory name
         * @param file the test file's "description" field
         * @return this
         */
        public TestApplicator file(final String dir, final String file) {
            boolean match = ("unified-test-format/" + dir).equals(testDef.dir)
                    && file.equals(testDef.file);
            return onMatch(match);
        }

        /**
         * Applies to the test where dir, file, and test match.
         * @param dir the directory name
         * @param file the test file's "description" field
         * @param test the individual test's "description" field
         * @return this
         */
        public TestApplicator test(final String dir, final String file, final String test) {
            boolean match = testDef.dir.equals("unified-test-format/" + dir)
                    && testDef.file.equals(file)
                    && testDef.test.equals(test);
            return onMatch(match);
        }

        /**
         * Utility method: emit replacement to standard out.
         * @param dir the directory name
         * @param fragment the substring to check in the test "description" field
         * @return this
         */
        public TestApplicator testContains(final String dir, final String fragment) {
            boolean match = ("unified-test-format/" + dir).equals(testDef.dir)
                    && testDef.test.contains(fragment);
            if (match) {
                System.out.printf(
                        "!!! REPLACE %s WITH: .test(\"%s\", \"%s\", \"%s\")%n",
                        fragment,
                        testDef.dir.replace("unified-test-format/", ""),
                        testDef.file,
                        testDef.test);
            }
            return this;
        }

        /**
         * Utility method: emit file info to standard out
         * @param dir the directory name
         * @param test the individual test's "description" field
         * @return this
         */
        public TestApplicator test(final String dir, final String test) {
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
         * serverless, check for serverless in the condition.
         * Must be the first method called in the chain.
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
    }
}
