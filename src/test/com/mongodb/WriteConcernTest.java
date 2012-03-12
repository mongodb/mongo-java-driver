// WriteConcernSerializationTest.java

/**
 *      Copyright (C) 2010 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.mongodb;

import com.mongodb.util.TestCase;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;

public class WriteConcernTest extends TestCase {

    @Test
    public void testEqualityAndHashCode() {
        Assert.assertEquals(new WriteConcern(new String("majority")), new WriteConcern(new String("majority")));
        Assert.assertEquals(new WriteConcern(new String("majority")).hashCode(), new WriteConcern(new String("majority")).hashCode());
        Assert.assertNotEquals(new WriteConcern(new String("majority")), new WriteConcern(1));
        Assert.assertNotEquals(new WriteConcern(new String("majority")).hashCode(), new WriteConcern(1).hashCode());
    }

    @Test
    public void testSerializeWriteConcern() throws IOException, ClassNotFoundException {
        WriteConcern writeConcern = WriteConcern.SAFE;

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

        objectOutputStream.writeObject(writeConcern);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        WriteConcern object2 = (WriteConcern) objectInputStream.readObject();

        Assert.assertEquals(1, object2.getW());
        Assert.assertEquals(false, object2.getFsync());
        Assert.assertEquals(false, object2.getJ());
        Assert.assertEquals(false, object2.getContinueOnErrorForInsert());
    }

    @Test
    public void testSerializeMajorityWriteConcern() throws IOException, ClassNotFoundException {
        WriteConcern writeConcern = WriteConcern.MAJORITY;

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

        objectOutputStream.writeObject(writeConcern);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        WriteConcern.Majority object2 = (WriteConcern.Majority) objectInputStream.readObject();

        Assert.assertEquals("majority", object2.getWString());
        Assert.assertEquals(false, object2.getFsync());
        Assert.assertEquals(false, object2.getJ());
        Assert.assertEquals(false, object2.getContinueOnErrorForInsert());
    }
}
