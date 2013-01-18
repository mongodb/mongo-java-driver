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
package com.google.code.morphia.mapping.validation.fieldrules;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.PreSave;
import com.google.code.morphia.annotations.Property;
import com.google.code.morphia.annotations.Transient;
import com.google.code.morphia.mapping.validation.ConstraintViolationException;
import com.google.code.morphia.testutil.AssertedFailure;
import com.google.code.morphia.testutil.TestEntity;
import com.mongodb.DBObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class PropertyAndEmbeddedTest extends TestBase {
    static class E extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Embedded("myFunkyR")
        private final R r = new R();

        @Transient
        private String document;

        @PreSave
        public void preSave(final DBObject o) {
            document = (String) ((DBObject) o.get("myFunkyR")).get("foo");
        }
    }

    static class E2 extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Embedded
        @Property("myFunkyR")
        private String s;
    }

    static class R {
        private final String foo = "bar";
    }

    @Test
    public void testCheck() {
        final E e = new E();
        ds.save(e);

        assertEquals("bar", e.document);

        new AssertedFailure(ConstraintViolationException.class) {
            public void thisMustFail() throws Throwable {
                morphia.map(E2.class);
            }
        };
    }
}
