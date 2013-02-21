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

// BSONTypeSerializableTest.java
package org.bson;

import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.junit.Test;
import org.bson.types.Document;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static java.nio.charset.Charset.defaultCharset;
import static org.junit.Assert.assertEquals;

public class BSONTypeSerializableTest {

    @Test
    public void testSerializeMinKey() throws Exception {
        final MinKey key = new MinKey();

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

        objectOutputStream.writeObject(key);

        final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        final ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        final MinKey key2 = (MinKey) objectInputStream.readObject();
    }

    @Test
    public void testSerializeMaxKey() throws Exception {
        final MaxKey key = new MaxKey();

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

        objectOutputStream.writeObject(key);

        final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        final ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        final MaxKey key2 = (MaxKey) objectInputStream.readObject();
    }

    @Test
    public void testSerializeBinary() throws Exception {
        final Binary binary = new Binary((byte) 0x00, "hello world".getBytes(defaultCharset()));

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

        objectOutputStream.writeObject(binary);

        final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        final ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        final Binary binary2 = (Binary) objectInputStream.readObject();

        assertEquals(binary, binary2);
    }

    @Test
    public void testSerializeBSONTimestamp() throws Exception {
        final BSONTimestamp object = new BSONTimestamp(100, 100);

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

        objectOutputStream.writeObject(object);

        final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        final ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        final BSONTimestamp object2 = (BSONTimestamp) objectInputStream.readObject();

        assertEquals(object.getTime(), object2.getTime());
        assertEquals(object.getInc(), object2.getInc());
    }

    @Test
    public void testSerializeCode() throws Exception {
        final Code object = new Code("function() {}");

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

        objectOutputStream.writeObject(object);

        final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        final ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        final Code object2 = (Code) objectInputStream.readObject();

        assertEquals(object.getCode(), object2.getCode());
    }

    @Test
    public void testSerializeCodeWScope() throws Exception {
        final Document scope = new Document("t", 1);
        final CodeWScope object = new CodeWScope("function() {}", scope);

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

        objectOutputStream.writeObject(object);

        final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        final ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        final CodeWScope object2 = (CodeWScope) objectInputStream.readObject();

        assertEquals(object.getCode(), object2.getCode());
        assertEquals(object.getScope().get("t"), object2.getScope().get("t"));
    }

    @Test
    public void testSerializeSymbol() throws Exception {
        final Symbol object = new Symbol("symbol");

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

        objectOutputStream.writeObject(object);

        final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        final ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        final Symbol object2 = (Symbol) objectInputStream.readObject();

        assertEquals(object.getSymbol(), object2.getSymbol());
    }

    @Test
    public void testSerializeObjectID() throws Exception {
        final ObjectId object = new ObjectId("001122334455667788990011");

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

        objectOutputStream.writeObject(object);

        final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        final ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        final ObjectId object2 = (ObjectId) objectInputStream.readObject();

        assertEquals(object, object2);
    }
}
