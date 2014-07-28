/*
 * Copyright (c) 2008 - 2014 MongoDB, Inc.
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

import category.ReplicaSet;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.UnknownHostException;

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ReadPreference.secondary;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class DBOldTest extends DatabaseTestCase {
    @Test
    @Category(ReplicaSet.class)
    public void testRequestPinning() throws UnknownHostException {
        assumeTrue(isDiscoverableReplicaSet());
        database.requestStart();
        try {
            DBCursor cursorBefore = collection.find().setReadPreference(secondary());
            cursorBefore.hasNext();

            for (int i = 0; i < 100; i++) {
                DBCursor cursorAfter = collection.find().setReadPreference(secondary()).batchSize(-1);
                cursorAfter.hasNext();
                assertEquals(cursorBefore.getServerAddress(), cursorAfter.getServerAddress());
            }

        } finally {
            database.requestDone();
        }
    }
    
    @Test
    public void testInsertUpdatesUnsafe() throws Exception {
        database.requestStart();
        try {
            BasicDBObject query = new BasicDBObject();
            BasicDBObject update = new BasicDBObject("$inc", new BasicDBObject("radius", 1D));
            DBCollection circle = database.getCollection("circle");
            circle.update(query, update, true, false, WriteConcern.UNACKNOWLEDGED);
            assertEquals(1, circle.getCount());
        } finally {
            database.requestDone();
        }
    }
    
}
