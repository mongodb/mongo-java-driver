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
package com.google.code.morphia.callbacks;

import com.google.code.morphia.AbstractEntityInterceptor;
import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.PrePersist;
import com.google.code.morphia.mapping.Mapper;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class EntityInterceptorMomentTest extends TestBase {

    static class E {
        @Id
        private final ObjectId id = new ObjectId();

        private boolean called = false;

        @PrePersist
        void entityCallback() {
            called = true;
        }
    }

    public static class Interceptor extends AbstractEntityInterceptor {

        public void prePersist(final Object ent, final DBObject dbObj, final Mapper mapr) {
            Assert.assertTrue(((E) ent).called);
        }

    }

    @Test
    public void testGlobalEntityInterceptorWorksAfterEntityCallback() {
        morphia.map(E.class);
        morphia.getMapper().addInterceptor(new Interceptor());

        ds.save(new E());
    }
}
