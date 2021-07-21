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

package com.mongodb.client;

import com.mongodb.MongoException;
import com.mongodb.client.internal.ClientSessionClock;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

// See https://github.com/mongodb/specifications/tree/master/source/transactions-convenient-api/tests/README.rst#prose-tests
public class WithTransactionProseTest extends DatabaseTestCase {
    private static final long START_TIME_MS = 1L;
    private static final long ERROR_GENERATING_INTERVAL = 121000L;

    @Before
    @Override
    public void setUp() {
        assumeTrue(canRunTests());
        super.setUp();

        // create the collection before starting transactions
        collection.insertOne(Document.parse("{ _id : 0 }"));
    }

    //
    // Test that the callback raises a custom exception or error that does not include either UnknownTransactionCommitResult or
    // TransientTransactionError error labels. The callback will execute using withTransaction and assert that the callback's error
    // bypasses any retry logic within withTransaction and is propagated to the caller of withTransaction.
    //
    @Test
    public void testCallbackRaisesCustomError() {
        final String exceptionMessage = "NotTransientOrUnknownError";
        ClientSession session = client.startSession();
        try {
            session.withTransaction(new TransactionBody<Void>() {
                @Override
                public Void execute() {
                    throw new MongoException(exceptionMessage);
                }
            });
            // should not get here
            fail("Test should have thrown an exception.");
        } catch (MongoException e) {
            assertEquals(exceptionMessage, e.getMessage());
        } finally {
            session.close();
        }
    }

    //
    // Test that the callback that returns a custom value (e.g. boolean, string, object). Execute this callback using withTransaction
    // and assert that the callback's return value is propagated to the caller of withTransaction.
    //
    @Test
    public void testCallbackReturnsValue() {
        ClientSession session = client.startSession();
        final String msg = "Inserted document";
        try {
            String returnValueFromCallback = session.withTransaction(new TransactionBody<String>() {
                @Override
                public String execute() {
                    collection.insertOne(Document.parse("{ _id : 1 }"));
                    return msg;
                }
            });
            assertEquals(msg, returnValueFromCallback);
        } finally {
            session.close();
        }
    }

    //
    // If the callback raises an error with the TransientTransactionError label and the retry timeout has been exceeded, withTransaction
    // should propagate the error to its caller.
    //
    @Test
    public void testRetryTimeoutEnforcedTransientTransactionError() {
        final String errorMessage = "transient transaction error";

        ClientSession session = client.startSession();
        ClientSessionClock.INSTANCE.setTime(START_TIME_MS);
        try {
            session.withTransaction(new TransactionBody<Void>() {
                @Override
                public Void execute() {
                ClientSessionClock.INSTANCE.setTime(ERROR_GENERATING_INTERVAL);
                    MongoException e = new MongoException(112, errorMessage);
                    e.addLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL);
                    throw e;
                }
            });
            fail("Test should have thrown an exception.");
        } catch (RuntimeException e) {
            assertEquals(errorMessage, e.getMessage());
            assertTrue(((MongoException) e).getErrorLabels().contains(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL));
        } finally {
            session.close();
        }
    }

    //
    // If committing raises an error with the UnknownTransactionCommitResult label, the error is not a write concern timeout, and the
    // retry timeout has been exceeded, withTransaction should propagate the error to its caller.
    //
    @Test
    public void testRetryTimeoutEnforcedUnknownTransactionCommit() {
        final MongoDatabase failPointAdminDb = client.getDatabase("admin");
        failPointAdminDb.runCommand(
                Document.parse("{'configureFailPoint': 'failCommand', 'mode': {'times': 2}, "
                        + "'data': {'failCommands': ['commitTransaction'], 'errorCode': 91, 'closeConnection': false}}"));

        final ClientSession session = client.startSession();
        ClientSessionClock.INSTANCE.setTime(START_TIME_MS);
        try {
            session.withTransaction(new TransactionBody<Void>() {
                @Override
                public Void execute() {
                    ClientSessionClock.INSTANCE.setTime(ERROR_GENERATING_INTERVAL);
                    collection.insertOne(session, new Document("_id", 2));
                    return null;
                }
            });
            fail("Test should have thrown an exception.");
        } catch (RuntimeException e) {
            assertEquals(91, ((MongoException) e).getCode());
            assertTrue(((MongoException) e).getErrorLabels().contains(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL));
        } finally {
            session.close();
            failPointAdminDb.runCommand(Document.parse("{'configureFailPoint': 'failCommand', 'mode': 'off'}"));
        }
    }

    //
    // If committing raises an error with the TransientTransactionError label and the retry timeout has been exceeded, withTransaction
    // should propagate the error to its caller. This case may occur if the commit was internally retried against a new primary after
    // a failover and the second primary returned a NoSuchTransaction error response.
    //
    @Test
    public void testRetryTimeoutEnforcedTransientTransactionErrorOnCommit() {
        final MongoDatabase failPointAdminDb = client.getDatabase("admin");
        failPointAdminDb.runCommand(
                Document.parse("{'configureFailPoint': 'failCommand', 'mode': {'times': 2}, "
                        + "'data': {'failCommands': ['commitTransaction'], 'errorCode': 251, 'codeName': 'NoSuchTransaction', "
                        + "'errmsg': 'Transaction 0 has been aborted', 'closeConnection': false}}"));

        final ClientSession session = client.startSession();
        ClientSessionClock.INSTANCE.setTime(START_TIME_MS);
        try {
            session.withTransaction(new TransactionBody<Void>() {
                @Override
                public Void execute() {
                    ClientSessionClock.INSTANCE.setTime(ERROR_GENERATING_INTERVAL);
                    collection.insertOne(session, Document.parse("{ _id : 1 }"));
                    return null;
                }
            });
            fail("Test should have thrown an exception.");
        } catch (RuntimeException e) {
            assertEquals(251, ((MongoException) e).getCode());
            assertTrue(((MongoException) e).getErrorLabels().contains(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL));
        } finally {
            session.close();
            failPointAdminDb.runCommand(Document.parse("{'configureFailPoint': 'failCommand', 'mode': 'off'}"));
        }
    }

    private boolean canRunTests() {
        if (isSharded()) {
            return serverVersionAtLeast(4, 2);
        } else if (isDiscoverableReplicaSet()) {
            return serverVersionAtLeast(4, 0);
        } else {
            return false;
        }
    }
}
