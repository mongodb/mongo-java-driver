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

import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.assertEquals;

public class DocumentTest {
    @Test
    public void testTypedGetMethods() {
        final Date date = new Date();
        final ObjectId objectId = new ObjectId();
        Document doc = new Document()
                .append("int", 1).append("long", 2L).append("double", 3.0).append("string", "hi").append("boolean", true)
                .append("objectId", objectId).append("date", date);

        assertEquals(Integer.valueOf(1), doc.getInteger("int"));
        assertEquals(Long.valueOf(2L), doc.getLong("long"));
        assertEquals(Double.valueOf(3.0), doc.getDouble("double"));
        assertEquals("hi", doc.getString("string"));
        assertEquals(Boolean.TRUE, doc.getBoolean("boolean"));
        assertEquals(objectId, doc.getObjectId("objectId"));
        assertEquals(date, doc.getDate("date"));

        assertEquals(objectId, doc.get("objectId", ObjectId.class));
    }
}
