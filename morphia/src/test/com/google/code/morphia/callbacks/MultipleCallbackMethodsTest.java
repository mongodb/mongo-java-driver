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

package com.google.code.morphia.callbacks;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.PrePersist;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

public class MultipleCallbackMethodsTest extends TestBase {
    abstract static class CallbackAbstractEntity {
        @Id
        private final String id = new ObjectId().toHexString();

        public String getId() {
            return id;
        }

        int foo = 0;

        @PrePersist
        void prePersist1() {
            foo++;
        }

        @PrePersist
        void prePersist2() {
            foo++;
        }
    }

    static class SomeEntity extends CallbackAbstractEntity {

    }

    @Test
    public void testMultipleCallbackAnnotation() throws Exception {
        final SomeEntity entity = new SomeEntity();
        ds.save(entity);
        Assert.assertEquals(2, entity.foo);
    }
}