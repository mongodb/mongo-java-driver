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

package com.mongodb;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DefaultDBDecoderTest extends DatabaseTestCase {

    @Test
    public void testDecodingDBRef() {
        DBObject dbObject = new BasicDBObject("r", new DBRef("test", 1));
        byte[] bytes = {37, 0, 0, 0, 3, 114, 0, 29, 0, 0, 0, 2, 36, 114, 101, 102, 0, 5, 0, 0, 0, 116, 101, 115, 116, 0, 16, 36, 105, 100,
                        0, 1, 0, 0, 0, 0, 0};
        DBObject o = new DefaultDBDecoder().decode(bytes, collection);
        assertEquals(dbObject, o);
    }

    @Test
    public void testTypeMapping() {
        collection.setObjectClass(MyDBObject.class);
        collection.setInternalClass("a", AnotherDBObject.class);
        byte[] bytes = {20, 0, 0, 0, 3, 97, 0, 12, 0, 0, 0, 16, 105, 0, 1, 0, 0, 0, 0, 0};
        DBObject object = new DefaultDBDecoder().decode(bytes, collection);
        assertEquals(MyDBObject.class, object.getClass());
        assertEquals(AnotherDBObject.class, object.get("a").getClass());
    }

    @SuppressWarnings("serial")
    public static class MyDBObject extends BasicDBObject {

    }

    @SuppressWarnings("serial")
    public static class AnotherDBObject extends BasicDBObject {

    }
}
