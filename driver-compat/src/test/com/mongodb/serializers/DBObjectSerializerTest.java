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

package com.mongodb.serializers;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DatabaseTestCase;
import org.bson.BSON;
import org.bson.Transformer;
import org.junit.Test;
import org.mongodb.serialization.PrimitiveSerializers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DBObjectSerializerTest extends DatabaseTestCase {

    @Test
    public void testGetters() {
        final PrimitiveSerializers serializers = PrimitiveSerializers.createDefault();
        final DBObjectSerializer serializer = new DBObjectSerializer(database, serializers,
                                                              BasicDBObject.class,
                                                              new HashMap<String, Class<? extends DBObject>>());
        assertEquals(database, serializer.getDb());
        assertEquals(BasicDBObject.class, serializer.getTopLevelClass());
        assertEquals(serializers, serializer.getPrimitiveSerializers());
        final HashMap<List<String>, Class<? extends DBObject>> expected
        = new HashMap<List<String>, Class<? extends DBObject>>();
        expected.put(new ArrayList<String>(), BasicDBObject.class);
        assertEquals(expected, serializer.getPathToClassMap());
    }

    @Test
    public void testPathToClassMap() {
        final HashMap<String, Class<? extends DBObject>> stringPathToClassMap
        = new HashMap<String, Class<? extends DBObject>>();
        stringPathToClassMap.put("a", NestedOneDBObject.class);
        stringPathToClassMap.put("a.b", NestedTwoDBObject.class);
        final DBObjectSerializer serializer = new DBObjectSerializer(database, PrimitiveSerializers.createDefault(),
                                                              TopLevelDBObject.class, stringPathToClassMap);
        final Map<List<String>, Class<? extends DBObject>> pathToClassMap
        = new HashMap<List<String>, Class<? extends DBObject>>();
        pathToClassMap.put(new ArrayList<String>(), TopLevelDBObject.class);
        pathToClassMap.put(Arrays.asList("a"), NestedOneDBObject.class);
        pathToClassMap.put(Arrays.asList("a", "b"), NestedTwoDBObject.class);
        assertEquals(pathToClassMap, serializer.getPathToClassMap());
    }

    @Test
    public void testPathToClassMapDeserialization() {
        collection.setObjectClass(TopLevelDBObject.class);
        collection.setInternalClass("a", NestedOneDBObject.class);
        collection.setInternalClass("a.b", NestedTwoDBObject.class);

        final DBObject doc = new TopLevelDBObject().append("a", new NestedOneDBObject().append("b", new NestedTwoDBObject())
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

    //CHECKSTYLE:OFF
    public static class TopLevelDBObject extends BasicDBObject {
        private static final long serialVersionUID = 7029929727222305692L;

        @Override
        public boolean equals(final Object o) {
            return getClass() == o.getClass() && super.equals(o);
        }
    }

    public static class NestedOneDBObject extends BasicDBObject {
        private static final long serialVersionUID = -5821458746671670383L;

        @Override
        public boolean equals(final Object o) {
            return getClass() == o.getClass() && super.equals(o);
        }
    }

    public static class NestedTwoDBObject extends BasicDBObject {
        private static final long serialVersionUID = 5243874721805359328L;

        @Override
        public boolean equals(final Object o) {
            return getClass() == o.getClass() && super.equals(o);
        }
    }
    //CHECKSTYLE:ON
}
