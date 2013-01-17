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
 */

package com.google.code.morphia.query;

import com.google.code.morphia.MappingTest.BaseEntity;
import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Entity;
import org.junit.Assert;
import org.junit.Test;

public class StringPatternQueriesTest extends TestBase {
    @Entity
    static class E extends BaseEntity {
        private final String name;

        public E(final String name) {
            this.name = name;
        }

        protected E() {
            name = null;
        }
    }

    @Test
    public void testStartsWith() throws Exception {

        ds.save(new E("A"), new E("a"), new E("Ab"), new E("ab"), new E("c"));

        Assert.assertEquals(2, ds.createQuery(E.class).field("name").startsWith("a").countAll());
        Assert.assertEquals(4, ds.createQuery(E.class).field("name").startsWithIgnoreCase("a").countAll());
        Assert.assertEquals(4, ds.createQuery(E.class).field("name").startsWithIgnoreCase("A").countAll());
    }

    @Test
    public void testEndsWith() throws Exception {

        ds.save(new E("bxA"), new E("xba"), new E("xAb"), new E("xab"), new E("xcB"), new E("aba"));

        Assert.assertEquals(2, ds.createQuery(E.class).field("name").endsWith("b").countAll());
        Assert.assertEquals(3, ds.createQuery(E.class).field("name").endsWithIgnoreCase("b").countAll());
    }

    @Test
    public void testContains() throws Exception {

        ds.save(new E("xBA"), new E("xa"), new E("xAb"), new E("xab"), new E("xcB"), new E("aba"));

        Assert.assertEquals(3, ds.createQuery(E.class).field("name").contains("b").countAll());
        Assert.assertEquals(5, ds.createQuery(E.class).field("name").containsIgnoreCase("b").countAll());
    }

}
