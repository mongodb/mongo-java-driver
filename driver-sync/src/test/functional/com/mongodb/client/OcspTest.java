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

package com.mongodb.client;

import com.mongodb.MongoTimeoutException;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.Before;
import org.junit.Test;

import static com.mongodb.ClusterFixture.getOcspShouldSucceed;
import static java.security.Security.getProperty;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

public class OcspTest {
    @Before
    public void setUp() {
        assumeTrue(canRunTests());
    }

    @Test
    public void testTLS() {
        String uri = "mongodb://localhost/?serverSelectionTimeoutMS=2000&tls=true";
        try (MongoClient client = MongoClients.create(uri)) {
            client.getDatabase("admin").runCommand(new BsonDocument("ping", new BsonInt32(1)));
        } catch (MongoTimeoutException e) {
            if (getOcspShouldSucceed()) {
                fail("Unexpected exception when using OCSP with tls=true: " + e);
            }
        }
    }

    private boolean canRunTests() {
        return getProperty("ocsp.enable") != null && getProperty("ocsp.enable").equals("true");
    }
}
