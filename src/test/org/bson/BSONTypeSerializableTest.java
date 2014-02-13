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

// BSONTypeSerializableTest.java
package org.bson;

import com.mongodb.DBRef;
import com.mongodb.util.TestCase;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class BSONTypeSerializableTest extends TestCase {

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
        
        assertArrayEquals(binary.getData(), binary2.getData());
        assertEquals(binary.getType(), binary2.getType());
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

        assertEquals(object.getTime(), object2.getTime());
        assertEquals(object.getInc(), object2.getInc());
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

        assertEquals(object.getCode(), object2.getCode());
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

        assertEquals(object.getCode(), object2.getCode());
        assertEquals(object.getScope().get("t"), object2.getScope().get("t"));
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

        assertEquals(object.getSymbol(), object2.getSymbol());
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

        assertEquals(object.toString(), object2.toString());
    }

    @Test
    public void testSerializeDBRef() throws Exception {
        DBRef dbRef = new DBRef(getDatabase(), "col", 42);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

        objectOutputStream.writeObject(dbRef);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        DBRef dbRef2 = (DBRef) objectInputStream.readObject();

        assertEquals(dbRef, dbRef2);
    }
}
