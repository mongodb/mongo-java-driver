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

package com.mongodb.internal.async.client;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClientException;
import com.mongodb.async.FutureResultCallback;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.serverVersionLessThan;
import static org.junit.Assume.assumeTrue;

public class TransactionFailureTest extends DatabaseTestCase {
    public TransactionFailureTest() {
    }

    @Before
    public void setUp() {
        assumeTrue(canRunTests());
        super.setUp();
    }

    @Test(expected = MongoClientException.class)
    public void testTransactionFails() throws InterruptedException {
        final ClientSession clientSession = createSession();

        try {
            clientSession.startTransaction();

            FutureResultCallback<Void> futureResultCallback = new FutureResultCallback<Void>();
            collection.insertOne(clientSession, Document.parse("{_id: 1, a: 1}"), futureResultCallback);
            futureResultCallback.get();
        } finally {
            clientSession.close();
        }
    }

    private ClientSession createSession() {
        final ClientSessionOptions options = ClientSessionOptions.builder().build();
        return new MongoOperation<ClientSession>() {
            @Override
            public void execute() {
                client.startSession(options, getCallback());
            }
        }.get();
    }


    private boolean canRunTests() {
        return serverVersionLessThan("4.0")
                || (serverVersionLessThan("4.1.0") && isSharded());
    }
}
