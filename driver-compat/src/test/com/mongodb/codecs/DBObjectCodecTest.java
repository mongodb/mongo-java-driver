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
import com.mongodb.DBObject;
import com.mongodb.DatabaseTestCase;
import com.mongodb.TypeMapper;
import org.bson.BSON;
import org.bson.Transformer;
import org.junit.Test;
import org.mongodb.codecs.PrimitiveCodecs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DBObjectCodecTest extends DatabaseTestCase {

    @Test
    public void testGetters() {
        final PrimitiveCodecs codecs = PrimitiveCodecs.createDefault();
        final DBObjectCodec codec = new DBObjectCodec(database, codecs, new TypeMapper(BasicDBObject.class));
        assertEquals(database, codec.getDb());

        assertEquals(codecs, codec.getPrimitiveCodecs());
    }

    @Test
    public void testPathToClassMap() {
        final HashMap<String, Class<? extends DBObject>> stringPathToClassMap
                = new HashMap<String, Class<? extends DBObject>>();
        stringPathToClassMap.put("a", NestedOneDBObject.class);
        stringPathToClassMap.put("a.b", NestedTwoDBObject.class);
        final DBObjectCodec codec = new DBObjectCodec(database,
                PrimitiveCodecs.createDefault(),
                new TypeMapper(TopLevelDBObject.class, stringPathToClassMap)
        );
        final Map<List<String>, Class<? extends DBObject>> pathToClassMap
                = new HashMap<List<String>, Class<? extends DBObject>>();
        pathToClassMap.put(new ArrayList<String>(), TopLevelDBObject.class);
        pathToClassMap.put(Arrays.asList("a"), NestedOneDBObject.class);
        pathToClassMap.put(Arrays.asList("a", "b"), NestedTwoDBObject.class);
    }

    @Test
    public void testPathToClassMapDecoding() {
        collection.setObjectClass(TopLevelDBObject.class);
        collection.setInternalClass("a", NestedOneDBObject.class);
        collection.setInternalClass("a.b", NestedTwoDBObject.class);

        final DBObject doc = new TopLevelDBObject()
                .append("a", new NestedOneDBObject()
                        .append("b", Arrays.asList(new NestedTwoDBObject()))
                        .append("c", new BasicDBObject()));
        collection.save(doc);
        assertEquals(doc, collection.findOne());
    }

    @Test
    public void testTransformers() {
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
    }

    @Test
    public void testDBListEncoding() {
        final BasicDBList list = new BasicDBList();
        list.add(new BasicDBObject("a", 1).append("b", true));
        list.add(new BasicDBObject("c", "string").append("d", 0.1));
        collection.save(new BasicDBObject("l", list));
        assertEquals(list, collection.findOne().get("l"));
    }

    public static class TopLevelDBObject extends BasicDBObject {
        private static final long serialVersionUID = 7029929727222305692L;
    }

    public static class NestedOneDBObject extends BasicDBObject {
        private static final long serialVersionUID = -5821458746671670383L;
    }

    public static class NestedTwoDBObject extends BasicDBObject {
        private static final long serialVersionUID = 5243874721805359328L;
    }
}
