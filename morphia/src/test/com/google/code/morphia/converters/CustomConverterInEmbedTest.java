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

/**
 *
 */
package com.google.code.morphia.converters;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.testutil.TestEntity;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Uwe Schaefer
 */
@SuppressWarnings("rawtypes")
public class CustomConverterInEmbedTest extends TestBase {

    public static class E1 extends TestEntity {
        private static final long serialVersionUID = 1L;
        private final List<Foo> foo = new LinkedList<Foo>();
    }

    public static class E2 extends TestEntity {
        private static final long serialVersionUID = 1L;
        private final Map<String, Foo> foo = new HashMap<String, Foo>();
    }

    // unknown type to convert
    public static class Foo {
        private String string;

        Foo() {
        }

        public Foo(final String string) {
            this.string = string;
        }

        @Override
        public String toString() {
            return string;
        }
    }

    public static class FooConverter extends TypeConverter implements SimpleValueConverter {

        private boolean done;

        public FooConverter() {
            super(Foo.class);
        }

        @Override
        public Object decode(final Class targetClass, final Object fromDBObject, final MappedField optionalExtraInfo) {
            return new Foo((String) fromDBObject);
        }

        @Override
        public Object encode(final Object value, final MappedField optionalExtraInfo) {
            done = true;
            return value.toString();
        }

        public boolean didConversion() {
            return done;
        }
    }

    //FIXME issue 101

    @Test
    public void testConversionInList() throws Exception {
        final FooConverter fc = new FooConverter();
        morphia.getMapper().getConverters().addConverter(fc);
        final E1 e = new E1();
        e.foo.add(new Foo("bar"));
        ds.save(e);
        org.junit.Assert.assertTrue(fc.didConversion());
    }

    @Test
    public void testConversionInMap() throws Exception {
        final FooConverter fc = new FooConverter();
        morphia.getMapper().getConverters().addConverter(fc);
        E2 e = new E2();
        e.foo.put("bar", new Foo("bar"));
        ds.save(e);

        org.junit.Assert.assertTrue(fc.didConversion());

        e = ds.find(E2.class).get();
        org.junit.Assert.assertNotNull(e.foo);
        org.junit.Assert.assertFalse(e.foo.isEmpty());
        org.junit.Assert.assertTrue(e.foo.containsKey("bar"));
        org.junit.Assert.assertEquals(e.foo.get("bar").string, "bar");
    }

}
