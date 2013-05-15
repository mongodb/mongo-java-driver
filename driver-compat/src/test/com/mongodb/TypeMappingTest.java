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

package com.mongodb;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TypeMappingTest {

    @Test
    public void testGetters() {
        final TypeMapping typeMapping = new TypeMapping(BasicDBObject.class);
        assertEquals(BasicDBObject.class, typeMapping.getTopLevelClass());
        final HashMap<List<String>, Class<? extends DBObject>> expected
                = new HashMap<List<String>, Class<? extends DBObject>>();
        expected.put(new ArrayList<String>(), BasicDBObject.class);
        assertEquals(expected, typeMapping.getPathToClassMap());
    }

    @Test
    public void testPathToClassMap() {
        final Map<String, Class<? extends DBObject>> stringPathToClassMap
                = new HashMap<String, Class<? extends DBObject>>();
        stringPathToClassMap.put("a", NestedOneDBObject.class);
        stringPathToClassMap.put("a.b", NestedTwoDBObject.class);

        final TypeMapping typeMapping = new TypeMapping(TopLevelDBObject.class, stringPathToClassMap);

        final Map<List<String>, Class<? extends DBObject>> pathToClassMap
                = new HashMap<List<String>, Class<? extends DBObject>>();
        pathToClassMap.put(new ArrayList<String>(), TopLevelDBObject.class);
        pathToClassMap.put(Arrays.asList("a"), NestedOneDBObject.class);
        pathToClassMap.put(Arrays.asList("a", "b"), NestedTwoDBObject.class);
        assertEquals(pathToClassMap, typeMapping.getPathToClassMap());
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
