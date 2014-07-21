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

package com.mongodb.client;

import com.mongodb.MongoQueryFailureException;
import org.junit.Test;
import org.mongodb.Document;

import static java.util.Arrays.asList;

public class MongoFindTest extends DatabaseTestCase {
    @Test(expected = MongoQueryFailureException.class)
    public void shouldThrowQueryFailureException() {
        collection.insert(new Document("loc", asList(0.0, 0.0)));
        collection.find(new Document("loc", new Document("$near", asList(0.0, 0.0)))).getOne();
    }
}
