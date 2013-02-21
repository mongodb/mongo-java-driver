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
package com.google.code.morphia.mapping.validation.fieldrules;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.PreSave;
import com.google.code.morphia.annotations.Serialized;
import com.google.code.morphia.annotations.Transient;
import com.google.code.morphia.testutil.TestEntity;
import com.mongodb.DBObject;
import org.junit.Test;

import static java.nio.charset.Charset.defaultCharset;
import static org.junit.Assert.assertEquals;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class SerializedNameTest extends TestBase {
    public static class E extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Serialized("changedName")
        private final byte[] b = "foo".getBytes(defaultCharset());

        @PreSave
        public void preSave(final DBObject o) {
            document = new String((byte[]) o.get("changedName"), defaultCharset());
        }

        @Transient
        private String document;
    }

    @Test
    public void testCheck() {
        final E e = new E();
        ds.save(e);

        assertEquals("foo", e.document);
    }
}
