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
import com.google.code.morphia.annotations.Version;
import com.google.code.morphia.mapping.validation.ConstraintViolationException;
import com.google.code.morphia.testutil.AssertedFailure;
import com.google.code.morphia.testutil.TestEntity;
import org.junit.Test;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class MultipleVersionsTest extends TestBase {

    static class Fail1 extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Version
        private long v1;
        @Version
        private long v2;
    }

    static class OK1 extends TestEntity {
        private static final long serialVersionUID = 1L;
        @Version
        private long v1;
    }

    @Test
    public void testCheck() {
        new AssertedFailure(ConstraintViolationException.class) {
            public void thisMustFail() {
                morphia.map(Fail1.class);
            }
        };
        morphia.map(OK1.class);
    }

}
