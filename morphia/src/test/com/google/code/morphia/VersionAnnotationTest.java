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

package com.google.code.morphia;

import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Version;
import org.bson.types.ObjectId;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ConcurrentModificationException;

/**
 * @author Scott Hernandez
 */
public class VersionAnnotationTest extends TestBase {

    private static class B {
        @Id
        private ObjectId id = new ObjectId();
        @Version
        private long version;
    }

    @Ignore
    @Test(expected = ConcurrentModificationException.class)
    public void testVersion() throws Exception {
        final B b1 = new B();
        try {
            ds.save(b1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        final B b2 = new B();
        b2.id = b1.id;
        ds.save(b2);
    }

}