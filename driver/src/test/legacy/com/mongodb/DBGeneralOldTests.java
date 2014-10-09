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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests aspect of the DB - not really driver tests.  Should be migrated into the appropriate unit or functional tests.
 */
public class DBGeneralOldTests extends DatabaseTestCase {
    @Test
    public void testGetCollectionNames() {
        String name = "testGetCollectionNames";
        DBCollection c = database.getCollection(name);
        c.drop();
        assertFalse(database.getCollectionNames().contains(name));
        c.save(new BasicDBObject("x", 1));
        assertTrue(database.getCollectionNames().contains(name));

    }

    @Test
    public void testRename() {
        String namea = "testRenameA";
        String nameb = "testRenameB";
        DBCollection firstCollection = database.getCollection(namea);
        DBCollection secondCollection = database.getCollection(nameb);

        firstCollection.drop();
        secondCollection.drop();

        assertEquals(0, firstCollection.find().count());
        assertEquals(0, secondCollection.find().count());

        firstCollection.save(new BasicDBObject("x", 1));
        assertEquals(1, firstCollection.find().count());
        assertEquals(0, secondCollection.find().count());

        DBCollection renamedFirstCollection = firstCollection.rename(nameb);
        assertEquals(0, firstCollection.find().count());
        assertEquals(1, secondCollection.find().count());
        assertEquals(1, renamedFirstCollection.find().count());

        assertEquals(secondCollection.getName(), renamedFirstCollection.getName());
    }
}
