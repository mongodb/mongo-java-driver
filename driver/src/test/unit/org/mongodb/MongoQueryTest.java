/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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
 *
 */

package org.mongodb;

import org.junit.Test;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoQuery;

import static org.junit.Assert.assertEquals;

public class MongoQueryTest {
    @Test
    public void testNumberToReturn() {
        MongoQuery query = new MongoFind();
        assertEquals(0, query.getNumberToReturn());

        query = new MongoFind().limit(-10);
        assertEquals(-10, query.getNumberToReturn());

        query = new MongoFind().batchSize(10);
        assertEquals(10, query.getNumberToReturn());

        query = new MongoFind().limit(10);
        assertEquals(10, query.getNumberToReturn());

        query = new MongoFind().limit(10).batchSize(15);
        assertEquals(10, query.getNumberToReturn());

        query = new MongoFind().limit(10).batchSize(-15);
        assertEquals(10, query.getNumberToReturn());

        query = new MongoFind().limit(10).batchSize(7);
        assertEquals(7, query.getNumberToReturn());

        query = new MongoFind().limit(10).batchSize(-7);
        assertEquals(-7, query.getNumberToReturn());
    }
}
