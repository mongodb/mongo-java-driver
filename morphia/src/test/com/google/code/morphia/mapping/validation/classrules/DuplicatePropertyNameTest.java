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
package com.google.code.morphia.mapping.validation.classrules;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Property;
import com.google.code.morphia.mapping.validation.ConstraintViolationException;
import com.google.code.morphia.testutil.AssertedFailure;
import org.junit.Test;

import java.util.Map;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class DuplicatePropertyNameTest extends TestBase {
    @Entity
    static class DuplicatedPropertyName {
        @Id
        private String id;

        @Property(value = "value")
        private String content1;
        @Property(value = "value")
        private String content2;
    }

    @Entity
    static class DuplicatedPropertyName2 {
        @Id
        private String id;

        @Embedded(value = "value")
        private Map<String, Integer> content1;
        @Property(value = "value")
        private String content2;
    }

    @Entity
    static class Super {
        private String foo;
    }

    static class Extends extends Super {
        private String foo;
    }

    @Test
    public void testDuplicatedPropertyName() throws Exception {
        new AssertedFailure(ConstraintViolationException.class) {
            public void thisMustFail() {
                morphia.map(DuplicatedPropertyName.class);
            }
        };
        new AssertedFailure(ConstraintViolationException.class) {
            public void thisMustFail() {
                morphia.map(DuplicatedPropertyName2.class);
            }
        };
        new AssertedFailure(ConstraintViolationException.class) {
            public void thisMustFail() {
                morphia.map(Extends.class);
            }
        };
    }

}
