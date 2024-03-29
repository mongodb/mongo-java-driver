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

import com.mongodb.MongoClientException;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.serverVersionLessThan;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TransactionFailureTest extends DatabaseTestCase {
    public TransactionFailureTest() {
    }

    @BeforeEach
    public void setUp() {
        assumeTrue(canRunTests());
        super.setUp();
    }

    @Test
    public void testTransactionFails() {
        try (ClientSession clientSession = client.startSession()) {
            clientSession.startTransaction();
            assertThrows(MongoClientException.class, () -> collection.insertOne(clientSession, Document.parse("{_id: 1, a: 1}")));
        }
    }

    private boolean canRunTests() {
        return serverVersionLessThan(4, 0)
                || (serverVersionLessThan(4, 2) && isSharded());
    }
}
