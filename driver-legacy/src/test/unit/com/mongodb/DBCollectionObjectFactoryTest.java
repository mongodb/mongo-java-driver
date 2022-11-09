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

import org.bson.BSONObject;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class DBCollectionObjectFactoryTest {

    private DBCollectionObjectFactory factory;

    @Before
    public void setUp() {
        factory = new DBCollectionObjectFactory();
    }

    @Test
    public void testDefaultTopLevelClass() {
        assertThat(factory.getInstance(), instanceOf(DBObject.class));
    }

    @Test
    public void testDefaultInternalClass() {
        assertThat(factory.getInstance(asList("a", "b", "c")), instanceOf(DBObject.class));
    }

    @Test
    public void testTopLevelClassWhenSet() {
        factory = factory.update(TopLevelDBObject.class);
        assertThat(factory.getInstance(), instanceOf(TopLevelDBObject.class));
    }

    @Test
    public void testEmptyPath() {
        factory = factory.update(TopLevelDBObject.class);
        assertThat(factory.getInstance(Collections.emptyList()), instanceOf(TopLevelDBObject.class));
    }

    @Test
    public void testInternalClassWhenTopLevelSet() {
        factory = factory.update(TopLevelDBObject.class);
        assertThat(factory.getInstance(asList("a", "b", "c")), instanceOf(DBObject.class));
    }

    @Test
    public void testSeveralInternalClassesSet() {
        factory = factory.update(NestedOneDBObject.class, asList("a", "b"));
        factory = factory.update(NestedTwoDBObject.class, asList("a", "c"));

        assertThat(factory.getInstance(asList("a", "b")), instanceOf(NestedOneDBObject.class));
        assertThat(factory.getInstance(asList("a", "c")), instanceOf(NestedTwoDBObject.class));
    }

    @Test
    public void testThatNullObjectClassRevertsToDefault() {
        factory = factory.update(MyDBObject.class, singletonList("a")).update(null);
        assertThat(factory.getInstance(), Matchers.instanceOf(BasicDBObject.class));
        assertThat(factory.getInstance(singletonList("a")), instanceOf(MyDBObject.class));

        factory = factory.update(null, singletonList("a"));
        assertThat(factory.getInstance(), Matchers.instanceOf(BasicDBObject.class));
        assertThat(factory.getInstance(singletonList("a")), instanceOf(BasicDBObject.class));
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

    @SuppressWarnings("rawtypes")
    public static class MyDBObject extends HashMap<String, Object> implements DBObject {

        private static final long serialVersionUID = -8540791504402368127L;

        @Override
        public void markAsPartialObject() {
        }

        @Override
        public boolean isPartialObject() {
            return false;
        }

        @Override
        public void putAll(final BSONObject o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putAll(final Map m) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object get(final String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map toMap() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object removeField(final String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsField(final String s) {
            throw new UnsupportedOperationException();
        }
    }
}
