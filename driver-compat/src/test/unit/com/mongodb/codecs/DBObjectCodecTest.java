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

package com.mongodb.codecs;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DatabaseTestCase;
import org.bson.BSON;
import org.bson.Transformer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DBObjectCodecTest extends DatabaseTestCase {

    @Test
    public void testTransformers() {
        try {
            collection.save(new BasicDBObject("_id", 1).append("x", 1.1));
            assertEquals(Double.class, collection.findOne().get("x").getClass());

            BSON.addEncodingHook(Double.class, new Transformer() {
                public Object transform(final Object o) {
                    return o.toString();
                }
            });

            collection.save(new BasicDBObject("_id", 1).append("x", 1.1));
            assertEquals(String.class, collection.findOne().get("x").getClass());

            BSON.clearAllHooks();
            collection.save(new BasicDBObject("_id", 1).append("x", 1.1));
            assertEquals(Double.class, collection.findOne().get("x").getClass());

            BSON.addDecodingHook(Double.class, new Transformer() {
                public Object transform(final Object o) {
                    return o.toString();
                }
            });
            assertEquals(String.class, collection.findOne().get("x").getClass());
            BSON.clearAllHooks();
            assertEquals(Double.class, collection.findOne().get("x").getClass());
        } finally {
            BSON.clearAllHooks();
        }
    }

    @Test
    public void testDBListEncoding() {
        final BasicDBList list = new BasicDBList();
        list.add(new BasicDBObject("a", 1).append("b", true));
        list.add(new BasicDBObject("c", "string").append("d", 0.1));
        collection.save(new BasicDBObject("l", list));
        assertEquals(list, collection.findOne().get("l"));
    }
}
