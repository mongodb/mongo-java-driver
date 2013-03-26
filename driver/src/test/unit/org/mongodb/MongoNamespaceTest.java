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
 */

package org.mongodb;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MongoNamespaceTest {
    @Test(expected = IllegalArgumentException.class)
    public void testNullDatabaseName() {
        new MongoNamespace(null, "test");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullCollectionName() {
        new MongoNamespace("test", null);
    }

    @Test
    public void testGetters() {
        MongoNamespace namespace = new MongoNamespace("db", "coll");
        assertEquals("db", namespace.getDatabaseName());
        assertEquals("coll", namespace.getCollectionName());
        assertEquals("db.coll", namespace.getFullName());
        assertEquals("db.coll", MongoNamespace.asNamespaceString("db", "coll"));
    }

    @Test
    public void testEqualsAndHashCode() {
        MongoNamespace namespace1 = new MongoNamespace("db1", "coll1");
        MongoNamespace namespace2 = new MongoNamespace("db1", "coll1");
        MongoNamespace namespace3 = new MongoNamespace("db2", "coll1");
        MongoNamespace namespace4 = new MongoNamespace("db1", "coll2");

        assertFalse(namespace1.equals(new Object()));
        assertTrue(namespace1.equals(namespace1));
        assertTrue(namespace1.equals(namespace2));
        assertFalse(namespace1.equals(namespace3));
        assertFalse(namespace1.equals(namespace4));

        assertEquals(97917362, namespace1.hashCode());
    }
}
