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
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.mapping.MappingException;
import com.google.code.morphia.testutil.AssertedFailure;
import org.bson.types.ObjectId;
import org.junit.Test;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class NonStaticInnerClassTest extends TestBase {

    static class Valid {
        @Id
        private ObjectId id;
    }

    class InValid {
        @Id
        private ObjectId id;
    }

    @Test
    public void testValidInnerClass() throws Exception {
        morphia.map(Valid.class);
    }

    @Test
    public void testInValidInnerClass() throws Exception {
        new AssertedFailure(MappingException.class) {
            @Override
            protected void thisMustFail() {
                morphia.map(InValid.class);
            }
        };
    }
}
