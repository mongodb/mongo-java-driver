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

package com.google.code.morphia.issue241;

import com.google.code.morphia.DatastoreImpl;
import com.google.code.morphia.Morphia;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.dao.BasicDAO;
import com.mongodb.Mongo;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;


/**
 * Unit test for testing morphia mappings with generics.
 */
public class MappingTest {

    private final Morphia morphia = new Morphia();

    private DatastoreImpl datastore;

    @Before
    public void setUp() throws UnknownHostException {
        final Mongo mongo = new Mongo();
        datastore = new DatastoreImpl(morphia, mongo, "MY_DB");
    }

    @After
    public void tearDown() {
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testMapping() {
        final BasicDAO<Message, ObjectId> messageDAO = new BasicDAO<Message, ObjectId>(Message.class, datastore);
        Assert.assertNotNull(messageDAO);
    }

    @SuppressWarnings("unused")
    @Entity
    private static class Message<U extends User> {

        @Id
        private ObjectId id;
        private U user;

        public U getUser() {
            return user;
        }

        public void setUser(final U user) {
            this.user = user;
        }
    }

    @Entity
    private static class User {
        @Id
        private ObjectId id;

        @Override
        public boolean equals(final Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final User other = (User) obj;
            if (this.id != other.id && (this.id == null || !this.id.equals(other.id))) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 97 * hash + (this.id != null ? this.id.hashCode() : 0);
            return hash;
        }
    }
}
