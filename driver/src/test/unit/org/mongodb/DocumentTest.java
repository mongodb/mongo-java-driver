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
import org.mongodb.json.JSONMode;
import org.mongodb.json.JSONParseException;

import java.util.Date;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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

    @Test
    public void testValueOfMethod() {
        final String json = "{ \"int\" : 1, \"string\" : \"abc\" }";
        final Document document = Document.valueOf(json);
        assertNotNull(document);
        assertEquals(2, document.keySet().size());
        assertEquals(Integer.valueOf(1), document.getInteger("int"));
        assertEquals("abc", document.getString("string"));
    }

    @Test
    public void testValueOfMethodWithMode() {
        final String json = "{\"regex\" : /abc/im }";
        final Pattern expectedPattern = Pattern.compile("abc", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

        final Document document = Document.valueOf(json, JSONMode.JavaScript);
        assertNotNull(document);
        assertEquals(1, document.keySet().size());

        final Pattern actualPattern = (Pattern) document.get("regex");

        assertEquals(expectedPattern.flags(), actualPattern.flags());
        assertEquals(expectedPattern.pattern(), actualPattern.pattern());
    }

    @Test(expected = JSONParseException.class)
    public void testValueOfMethodWithInvalidInput() {
        final String json = "{ \"int\" : 1, \"string\" : }";
        Document.valueOf(json);
    }

    @Test
    public void testToStringMethod() {
        final Document document = new Document()
                .append("int", 1)
                .append("string", "abc");

        assertEquals("{ \"int\" : 1, \"string\" : \"abc\" }", document.toString());
    }
}
