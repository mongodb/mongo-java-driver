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

package com.mongodb.embedded.client;

import com.mongodb.MongoClientException;
import com.sun.jna.Native;
import org.junit.After;
import org.junit.Before;

import static com.mongodb.embedded.client.Fixture.getMongoClient;
import static java.lang.String.format;
import static org.junit.Assume.assumeTrue;

public class DatabaseTestCase {


    @Before
    public void setUp() {
        try {
            getMongoClient();
        } catch (MongoClientException e) {
            assumeTrue(format("Could not create a MongoClient: %s", e.getMessage()), false);
        }
        Native.setProtected(true);
    }

    @After
    public void tearDown() {
    }
}
