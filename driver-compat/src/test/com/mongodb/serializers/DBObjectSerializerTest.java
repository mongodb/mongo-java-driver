/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package com.mongodb.serializers;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClientTestBase;
import org.junit.Test;
import org.mongodb.serialization.PrimitiveSerializers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DBObjectSerializerTest extends MongoClientTestBase {

    @Test
    public void testGetters() {
        PrimitiveSerializers serializers = PrimitiveSerializers.createDefault();
        DBObjectSerializer serializer = new DBObjectSerializer(getDB(), serializers,
                                                               BasicDBObject.class,
                                                               new HashMap<String, Class<? extends DBObject>>());
        assertEquals(getDB(), serializer.getDb());
        assertEquals(BasicDBObject.class, serializer.getTopLevelClass());
        assertEquals(serializers, serializer.getPrimitiveSerializers());
        final HashMap<List<String>, Class<? extends DBObject>> expected = new HashMap<List<String>, Class<? extends DBObject>>();
        expected.put(new ArrayList<String>(), BasicDBObject.class);
        assertEquals(expected, serializer.getPathToClassMap());
    }

    @Test
    public void testPathToClassMap() {
        final HashMap<String, Class<? extends DBObject>> stringPathToClassMap = new HashMap<String, Class<? extends DBObject>>();
        stringPathToClassMap.put("a", NestedOneDBObject.class);
        stringPathToClassMap.put("a.b", NestedTwoDBObject.class);
        DBObjectSerializer serializer = new DBObjectSerializer(getDB(), PrimitiveSerializers.createDefault(),
                                                               TopLevelDBObject.class, stringPathToClassMap);
        Map<List<String>, Class<? extends DBObject>> pathToClassMap = new HashMap<List<String>, Class<? extends DBObject>>();
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

        DBObject doc = new TopLevelDBObject().append("a", new NestedOneDBObject().append("b", new NestedTwoDBObject()).append("c", new BasicDBObject()));
        collection.save(doc);
        assertEquals(doc, collection.findOne());
    }

    public static class TopLevelDBObject extends BasicDBObject {
        private static final long serialVersionUID = 7029929727222305692L;

        @Override
        public boolean equals(final Object o) {
            if (getClass() != o.getClass()) {
                return false;
            }
            return super.equals(o);
        }
    }

    public static class NestedOneDBObject extends BasicDBObject {
        private static final long serialVersionUID = -5821458746671670383L;
        @Override
        public boolean equals(final Object o) {
            if (getClass() != o.getClass()) {
                return false;
            }
            return super.equals(o);
        }
    }

    public static class NestedTwoDBObject extends BasicDBObject {
        private static final long serialVersionUID = 5243874721805359328L;
        @Override
        public boolean equals(final Object o) {
            if (getClass() != o.getClass()) {
                return false;
            }
            return super.equals(o);
        }
    }
}
