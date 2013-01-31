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

/**
 *
 */
package com.google.code.morphia.converters;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.testutil.TestEntity;
import org.junit.Test;

/**
 * @author Uwe Schaefer
 */
public class CustomConverterDefaultTest extends TestBase {

    private static class E extends TestEntity {
        private static final long serialVersionUID = 1L;

        // FIXME issue 100 :
        // http://code.google.com/p/morphia/issues/detail?id=100
        // check default inspection: if not declared as property,
        // morphia fails due to defaulting to embedded and expecting a non-arg
        // constructor.
        //
        // @Property
        private Foo foo;

    }

    // unknown type to convert
    private static class Foo {
        private final String string;

        public Foo(final String string) {
            this.string = string;
        }

        @Override
        public String toString() {
            return string;
        }
    }

    @SuppressWarnings("rawtypes")
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

    @Test
    public void testConversion() throws Exception {
        final FooConverter fc = new FooConverter();
        morphia.getMapper().getConverters().addConverter(fc);
        morphia.map(E.class);
        E e = new E();
        e.foo = new Foo("test");
        ds.save(e);

        org.junit.Assert.assertTrue(fc.didConversion());

        e = ds.find(E.class).get();
        org.junit.Assert.assertNotNull(e.foo);
        org.junit.Assert.assertEquals(e.foo.string, "test");

    }

}
