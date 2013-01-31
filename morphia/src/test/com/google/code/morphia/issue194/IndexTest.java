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

package com.google.code.morphia.issue194;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Indexed;
import com.mongodb.MongoException;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

public class IndexTest extends TestBase {

    static class E1 {
        @Id
        private ObjectId id;
        @Indexed(name = "NAME", unique = true)
        private String name;

        public E1() {
        }

        public void setName(final String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setId(final ObjectId id) {
            this.id = id;
        }

        public ObjectId getId() {
            return id;
        }
    }

    @Entity
    static class E2 {
        @Id
        private ObjectId id;
        @Indexed(name = "NAME", unique = true)
        private String name;

        public E2() {
        }

        public void setName(final String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setId(final ObjectId id) {
            this.id = id;
        }

        public ObjectId getId() {
            return id;
        }
    }

    @Before
    public void setUp() {
        super.setUp();
        morphia.map(E1.class);
        morphia.map(E2.class);
        ds.ensureIndexes();
        ds.ensureCaps();
    }

    @Test(expected = MongoException.DuplicateKey.class)
    public void testDuplicate1() {
        final String name = "J. Doe";

        final E1 ent11 = new E1();
        ent11.setName(name);
        ds.save(ent11);

        final E1 ent12 = new E1();
        ent12.setName(name);
        ds.save(ent12);

    }

    @Test(expected = MongoException.DuplicateKey.class)
    public void testDuplicate2() {
        final String name = "J. Doe";

        final E2 ent21 = new E2();
        ent21.setName(name);
        ds.save(ent21);

        final E2 ent22 = new E2();
        ent22.setName(name);
        ds.save(ent22);
    }
}
