/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

public class DBObjectFactoryTest {

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
        assertThat(factory.getInstance(Arrays.asList("a", "b", "c")), instanceOf(DBObject.class));
    }

    @Test
    public void testTopLevelClassWhenSet() {
        factory = factory.update(TopLevelDBObject.class);
        assertThat(factory.getInstance(), instanceOf(TopLevelDBObject.class));
    }

    @Test
    public void testEmptyPath() {
        factory = factory.update(TopLevelDBObject.class);
        assertThat(factory.getInstance(Collections.<String>emptyList()), instanceOf(TopLevelDBObject.class));
    }

    @Test
    public void testInternalClassWhenTopLevelSet() {
        factory = factory.update(TopLevelDBObject.class);
        assertThat(factory.getInstance(Arrays.asList("a", "b", "c")), instanceOf(DBObject.class));
    }

    @Test
    public void testSeveralInternalClassesSet() {
        factory = factory.update(NestedOneDBObject.class, Arrays.asList("a", "b"));
        factory = factory.update(NestedTwoDBObject.class, Arrays.asList("a", "c"));

        assertThat(factory.getInstance(Arrays.asList("a", "b")), instanceOf(NestedOneDBObject.class));
        assertThat(factory.getInstance(Arrays.asList("a", "c")), instanceOf(NestedTwoDBObject.class));
    }

    @Test
    public void testReflectionObject() {
        factory = factory.update(Tweet.class);
        assertThat(factory.getInstance(), instanceOf(Tweet.class));
    }

    @Test
    public void testReflectionNestedObject() {
        factory = factory.update(Tweet.class);
        assertThat(factory.getInstance(Arrays.asList("Next")), instanceOf(Tweet.class));
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

    public static class Tweet extends ReflectionDBObject {
        private String message;
        private Tweet next;

        public Tweet getNext() {
            return next;
        }

        public void setNext(final Tweet next) {
            this.next = next;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(final String message) {
            this.message = message;
        }
    }

}
