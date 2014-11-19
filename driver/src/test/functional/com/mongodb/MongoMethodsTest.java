/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb;

import org.junit.Test;

import java.net.UnknownHostException;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;

public class MongoMethodsTest extends DatabaseTestCase {
    @Test
    @SuppressWarnings("deprecation") // This is for testing the old API, so it will use deprecated methods
    public void shouldGetDatabaseNames() throws UnknownHostException {
        try {
            getClient().getDB("test1").getCollection("test").insert(new BasicDBObject("a", 1));
            getClient().getDB("test2").getCollection("test").insert(new BasicDBObject("a", 1));

            assertThat(getClient().getDatabaseNames(), hasItems("test1", "test2"));
        } finally {
            getClient().dropDatabase("test1");
            getClient().dropDatabase("test2");
        }
    }
}
