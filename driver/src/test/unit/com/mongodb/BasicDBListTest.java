/*
 * Copyright (c) 2008-2016 MongoDB, Inc.
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

import static com.mongodb.MongoClient.getDefaultCodecRegistry;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;
import org.junit.Test;

public class BasicDBListTest {

    @Test
    public void testParse() {
        BasicDBList target = new BasicDBList();
        target.addAll(Arrays.asList(1, 2));

        BasicDBList document = BasicDBList.parse("[ 1, 2 ]");
        assertEquals(target, document);

        document = BasicDBList.parse("[ 1, 2 ]", getDefaultCodecRegistry().get(BasicDBList.class));
        assertEquals(target, document);

        target = new BasicDBList();
        target.addAll(Arrays.asList("string1", "string2"));

        document = BasicDBList.parse("[ 'string1', 'string2' ]");
        assertEquals(target, document);

        target = new BasicDBList();
        target.add(123L);
        target.add(new ObjectId("5522d5d12cf8fb556a991f45"));

        document = BasicDBList.parse("[ NumberLong(123), ObjectId('5522d5d12cf8fb556a991f45') ]");
        assertEquals(target, document);
    }

    @Test
    public void testToJson() {
        BasicDBList list = new BasicDBList();
        list.add(new ObjectId("5522d5d12cf8fb556a991f45"));
        list.add(3);
        list.add("abc");

        assertEquals("[{ \"$oid\" : \"5522d5d12cf8fb556a991f45\" }, 3, \"abc\"]", list.toJson());
        assertEquals("[ObjectId(\"5522d5d12cf8fb556a991f45\"), 3, \"abc\"]",
                     list.toJson(new JsonWriterSettings(JsonMode.SHELL)));

        assertEquals("[{ \"$oid\" : \"5522d5d12cf8fb556a991f45\" }, 3, \"abc\"]",
                     list.toJson(getDefaultCodecRegistry().get(BasicDBList.class)));

        assertEquals("[ObjectId(\"5522d5d12cf8fb556a991f45\"), 3, \"abc\"]",
                     list.toJson(new JsonWriterSettings(JsonMode.SHELL), getDefaultCodecRegistry().get(BasicDBList.class)));
    }

}
