/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package org.mongodb;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class DBRefTest {

    @Test
    public void shouldBeEqualIfBothDBRefObjectsHaveTheSameValues() {
        DBRef dbRef = new DBRef("theId", "theNamespace");
        DBRef dbRefToCompare = new DBRef("theId", "theNamespace");

        assertThat(dbRef.equals(dbRefToCompare), is(true));
        assertThat(dbRef, is(dbRefToCompare));
    }

    @Test
    public void testSerialization() throws Exception {
        DBRef dbRef = new DBRef(42, "col");

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

        objectOutputStream.writeObject(dbRef);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        DBRef dbRef2 = (DBRef) objectInputStream.readObject();

        assertEquals(dbRef, dbRef2);
    }
}
