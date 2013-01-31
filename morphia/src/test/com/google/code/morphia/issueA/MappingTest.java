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

package com.google.code.morphia.issueA;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Id;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.junit.Test;


/**
 * Test from email to mongodb-users list.
 */
public class MappingTest extends TestBase {

    @Test
    public void testMapping() {
        morphia.map(ClassLevelThree.class);
        final ClassLevelThree sp = new ClassLevelThree();

        //Old way
        final DBObject wrapObj = morphia.toDBObject(sp);  //the error points here from the user
        ds.getDB().getCollection("testColl").save(wrapObj);

        //better way
        ds.save(sp);
    }

    private interface InterfaceOne<K> {
        K getK();
    }

    private class ClassLevelOne<K> implements InterfaceOne<K>, Cloneable {
        private K k;

        public K getK() {
            return k;
        }
    }

    private class ClassLevelTwo extends ClassLevelOne<String> {

    }

    private static class ClassLevelThree {
        @Id
        private ObjectId id;

        private String name;

        @Embedded
        private ClassLevelTwo value;
    }

}
