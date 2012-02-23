// BSONTypeSerializableTest.java

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
package org.bson;

import org.bson.types.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class BSONTypeSerializableTest extends Assert {

    @Test
    public void testSerializeMinKey() throws Exception {
        MinKey key = new MinKey();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

        objectOutputStream.writeObject(key);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        MinKey key2 = (MinKey) objectInputStream.readObject();
    }

    @Test
    public void testSerializeMaxKey() throws Exception {
        MaxKey key = new MaxKey();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

        objectOutputStream.writeObject(key);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        MaxKey key2 = (MaxKey) objectInputStream.readObject();
    }

    @Test
    public void testSerializeBinary() throws Exception {
        Binary binary = new Binary((byte)0x00 , "hello world".getBytes());

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

        objectOutputStream.writeObject(binary);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        Binary binary2 = (Binary) objectInputStream.readObject();
        
        Assert.assertEquals(binary.getData(), binary2.getData());
        Assert.assertEquals(binary.getType(), binary2.getType());
    }

    @Test
    public void testSerializeBSONTimestamp() throws Exception {
        BSONTimestamp object = new BSONTimestamp(100, 100);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

        objectOutputStream.writeObject(object);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        BSONTimestamp object2 = (BSONTimestamp) objectInputStream.readObject();

        Assert.assertEquals(object.getTime(), object2.getTime());
        Assert.assertEquals(object.getInc(), object2.getInc());
    }

    @Test
    public void testSerializeCode() throws Exception {
        Code object = new Code("function() {}");

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

        objectOutputStream.writeObject(object);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        Code object2 = (Code) objectInputStream.readObject();

        Assert.assertEquals(object.getCode(), object2.getCode());
    }

    @Test
    public void testSerializeCodeWScope() throws Exception {
        BSONObject scope = new BasicBSONObject("t", 1);
        CodeWScope object = new CodeWScope("function() {}", scope);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

        objectOutputStream.writeObject(object);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        CodeWScope object2 = (CodeWScope) objectInputStream.readObject();

        Assert.assertEquals(object.getCode(), object2.getCode());
        Assert.assertEquals(object.getScope().get("t"), object2.getScope().get("t"));
    }

    @Test
    public void testSerializeSymbol() throws Exception {
        Symbol object = new Symbol("symbol");

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

        objectOutputStream.writeObject(object);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        Symbol object2 = (Symbol) objectInputStream.readObject();

        Assert.assertEquals(object.getSymbol(), object2.getSymbol());
    }

    @Test
    public void testSerializeObjectID() throws Exception {
        ObjectId object = new ObjectId("001122334455667788990011");

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

        objectOutputStream.writeObject(object);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        ObjectId object2 = (ObjectId) objectInputStream.readObject();

        Assert.assertEquals(object.toString(), object2.toString());
    }
}
