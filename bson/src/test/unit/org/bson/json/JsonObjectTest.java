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

package org.bson.json;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.codecs.BsonCodecProvider;
import org.bson.codecs.JsonObjectCodecProvider;
import org.junit.Test;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class JsonObjectTest {

    @Test(expected = IllegalArgumentException.class)
    public void testNull() {
        new JsonObject(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testArray() {
        new JsonObject("['A', 'B', 'C']");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSpaceInvalidObject() {
        new JsonObject(" ['A']");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLineFeedInvalidObject() {
        new JsonObject("\nvalue");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCarriageReturnInvalidObject() {
        new JsonObject("\r123");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHorizontalTabInvalidObject() {
        new JsonObject("\t123");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOnlyWhitespace() {
        new JsonObject("    \t\n  \r  ");
    }

    @Test
    public void testSpaceValidObject() {
        String json = "   {hello: 2}";
        assertEquals(new JsonObject(json).getJson(), json);
    }

    @Test
    public void testLineFeedValidObject() {
        String json = "\n{hello: 2}";
        assertEquals(new JsonObject(json).getJson(), json);
    }

    @Test
    public void testCarriageReturnValidObject() {
        String json = "\r{hello: 2}";
        assertEquals(new JsonObject(json).getJson(), json);
    }

    @Test
    public void testHorizontalTabValidObject() {
        String json = "\t{hello: 2}";
        assertEquals(new JsonObject(json).getJson(), json);
    }

    @Test
    public void testLeadingAndTrailingWhitespace() {
        String json = "\n\t\r {hello: 2} \n";
        assertEquals(new JsonObject(json).getJson(), json);
    }

    @Test
    public void testEqualsAndHashCode() {
        JsonObject j1 = new JsonObject("{hello: 1}");
        JsonObject j2 = new JsonObject("{hello: 1}");
        JsonObject j3 = new JsonObject("{world: 2}");

        assertEquals(j1, j1);
        assertEquals(j1, j2);
        assertEquals(j2, j1);
        assertNotEquals(j1, j3);
        assertNotEquals(j3, j1);
        assertNotEquals(null, j1);
        assertNotEquals("{hello: 1}", j1);

        assertEquals(j1.hashCode(), j1.hashCode());
        assertEquals(j1.hashCode(), j2.hashCode());
    }

    @Test
    public void testGetJson() {
        JsonObject j1 = new JsonObject("{hello: 1}");
        assertEquals(j1.getJson(), "{hello: 1}");
    }

    @Test
    public void testToBsonDocument() {
        JsonObject j1 = new JsonObject("{hello: 1}");
        BsonDocument b1 = new BsonDocument("hello", new BsonInt32(1));
        assertEquals(j1.toBsonDocument(null, fromProviders(new JsonObjectCodecProvider(), new BsonCodecProvider())), b1);
    }
}
