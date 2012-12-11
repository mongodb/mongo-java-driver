/**
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
 *
 */

package org.mongodb;

import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.bson.BsonType;
import org.bson.io.PooledByteBufferOutput;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.bson.util.BufferPool;
import org.bson.util.PowerOfTwoByteBufferPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.io.MongoChannel;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.GetMore;
import org.mongodb.operation.MongoInsert;
import org.mongodb.protocol.MongoGetMoreMessage;
import org.mongodb.protocol.MongoInsertMessage;
import org.mongodb.protocol.MongoQueryMessage;
import org.mongodb.protocol.MongoReplyMessage;
import org.mongodb.serialization.BinarySerializer;
import org.mongodb.serialization.BsonSerializationOptions;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.Serializers;
import org.mongodb.serialization.serializers.DateSerializer;
import org.mongodb.serialization.serializers.DoubleSerializer;
import org.mongodb.serialization.serializers.IntegerSerializer;
import org.mongodb.serialization.serializers.LongSerializer;
import org.mongodb.serialization.serializers.MongoDocumentSerializer;
import org.mongodb.serialization.serializers.ObjectIdSerializer;
import org.mongodb.serialization.serializers.StringSerializer;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MongoChannelTest  {
    MongoChannel channel;
    Serializers serializers;
    BufferPool<ByteBuffer> bufferPool;

    @Before
    public void setUp() throws UnknownHostException, SocketException {
        bufferPool = new PowerOfTwoByteBufferPool(24);
        channel = new MongoChannel(new ServerAddress("localhost", 27017), bufferPool);
        serializers = new Serializers();
        serializers.register(MongoDocument.class, BsonType.DOCUMENT, new MongoDocumentSerializer(serializers));
        serializers.register(ObjectId.class, BsonType.OBJECT_ID, new ObjectIdSerializer());
        serializers.register(Integer.class, BsonType.INT32, new IntegerSerializer());
        serializers.register(Long.class, BsonType.INT64, new LongSerializer());
        serializers.register(String.class, BsonType.STRING, new StringSerializer());
        serializers.register(Double.class, BsonType.DOUBLE, new DoubleSerializer());
        serializers.register(Binary.class, BsonType.BINARY, new BinarySerializer());
        serializers.register(Date.class, BsonType.DATE_TIME, new DateSerializer());
    }

    @After
    public void tearDown() {
        channel.close();
    }

    static class Concrete {
        ObjectId id;
        String str;
        int i;
        long l;
        double d;
        long date;

        public Concrete(final ObjectId id, final String str, final int i, final long l, final double d, final long date) {
            this.id = id;
            this.str = str;
            this.i = i;
            this.l = l;
            this.d = d;
            this.date = date;
        }

        public Concrete() {

        }

        @Override
        public String toString() {
            return "Concrete{" +
                    "id=" + id +
                    ", str='" + str + '\'' +
                    ", i=" + i +
                    ", l=" + l +
                    ", d=" + d +
                    ", date=" + date +
                    '}';
        }
    }

    static class ConcreteSerializer implements Serializer {

        @Override
        public void serialize(final BSONWriter bsonWriter, final Class clazz, final Object value, final BsonSerializationOptions options) {
            Concrete c = (Concrete) value;
            bsonWriter.writeStartDocument();
            {
                bsonWriter.writeObjectId("_id", c.id);
                bsonWriter.writeString("str", c.str);
                bsonWriter.writeInt32("i", c.i);
                bsonWriter.writeInt64("l", c.l);
                bsonWriter.writeDouble("d", c.d);
                bsonWriter.writeDateTime("date", c.date);
            }
            bsonWriter.writeEndDocument();
        }

        @Override
        public Object deserialize(final BSONReader reader, final Class clazz, final BsonSerializationOptions options) {
            Concrete c = new Concrete();
            reader.readStartDocument();
            {
                c.id = reader.readObjectId("_id");
                c.str = reader.readString("str");
                c.i = reader.readInt32("i");
                c.l = reader.readInt64("l");
                c.d = reader.readDouble("d");
                c.date = reader.readDateTime("date");

            }
            reader.readEndDocument();
            return c;
        }
    }

    @Test
    public void concreteClassTest() throws IOException {
        serializers.register(Concrete.class, BsonType.DOCUMENT, new ConcreteSerializer());

        Concrete c = new Concrete(new ObjectId(), "hi mom", 42, 42L, 42.0, new Date().getTime());

        MongoInsertMessage<Concrete> insertMessage = new MongoInsertMessage<Concrete>("test.concrete",
                new MongoInsert<Concrete>(c).writeConcern(WriteConcern.UNACKNOWLEDGED), Concrete.class,
                new PooledByteBufferOutput(bufferPool), serializers);

        channel.sendMessage(insertMessage);

        final MongoQueryFilterDocument filter = new MongoQueryFilterDocument("i", 42);

        MongoQueryMessage queryMessage = new MongoQueryMessage("test.concrete", new MongoFind(filter).readPreference(ReadPreference.primary()),
                new PooledByteBufferOutput(bufferPool), serializers);

        channel.sendMessage(queryMessage);


        MongoReplyMessage<Concrete> replyMessage = channel.receiveMessage(serializers, Concrete.class);
        System.out.println(replyMessage.getDocuments());
    }

    @Test
    public void sendMessageTest() throws IOException, InterruptedException {
        dropCollection("sendMessageTest");

        long startTime = System.nanoTime();
        int iterations = 10000;
        byte[] bytes = new byte[8192];
        for (int i = 0; i < iterations; i++) {
            if ((i > 0) && (i % 10000 == 0)) {
                System.out.println("i: " + i);
            }

            MongoDocument doc1 = new MongoDocument();
            doc1.put("_id", new ObjectId());
            doc1.put("str", "hi mom");
            doc1.put("int", 42);
            doc1.put("long", 42L);
            doc1.put("double", 42.0);
            doc1.put("date", new Date());
            doc1.put("binary", new Binary(bytes));

            MongoInsertMessage<MongoDocument> message = new MongoInsertMessage<MongoDocument>("MongoConnectionTest.sendMessageTest",
                    new MongoInsert<MongoDocument>(doc1).writeConcern(WriteConcern.ACKNOWLEDGED), MongoDocument.class,
                    new PooledByteBufferOutput(bufferPool), serializers);

            channel.sendMessage(message);

        }

        long endTime = System.nanoTime();
        double elapsedTime = (endTime - startTime) / (double) 1000000000;
        System.out.println("Time: " + elapsedTime + " sec");
        System.out.println(iterations / elapsedTime + " messages/sec");
        System.out.flush();

        System.out.println();

        System.out.println("Query time...");

        final MongoQueryFilterDocument filter = new MongoQueryFilterDocument();

        startTime = endTime;

        MongoQueryMessage queryMessage = new MongoQueryMessage("MongoConnectionTest.sendMessageTest",
                new MongoFind(filter).batchSize(4000000).readPreference(ReadPreference.primary()),
                new PooledByteBufferOutput(bufferPool), serializers);

        channel.sendMessage(queryMessage);

        int totalDocuments = 0;

        MongoReplyMessage<MongoDocument> replyMessage = channel.receiveMessage(serializers, MongoDocument.class);
        totalDocuments += replyMessage.getNumberReturned();
        System.out.println(" Initial: " + replyMessage.getDocuments().size() + " documents, " + replyMessage.getMessageLength() + " bytes");

        while (replyMessage.getCursorId() != 0) {
            MongoGetMoreMessage getMoreMessage = new MongoGetMoreMessage("MongoConnectionTest.sendMessageTest",
                    new GetMore(replyMessage.getCursorId(), 0), new PooledByteBufferOutput(bufferPool));

            channel.sendMessage(getMoreMessage);

            replyMessage = channel.receiveMessage(serializers, MongoDocument.class);
            totalDocuments += replyMessage.getNumberReturned();
            System.out.println(" Get more: " + replyMessage.getDocuments().size() + " documents, " + replyMessage.getMessageLength() + " bytes");
        }

        System.out.println("Total: " + totalDocuments);

        System.out.println();

        endTime = System.nanoTime();
        elapsedTime = (endTime - startTime) / (double) 1000000000;
        System.out.println("Time: " + elapsedTime + " sec");
        System.out.println(iterations / elapsedTime + " documents/sec");
        System.out.flush();
        Thread.sleep(1000);
    }

    @Test
    public void receiveMessageTest() throws IOException {
        dropCollection("sendMessageTest");

        List<MongoDocument> documents = insertTwoDocuments();

        long startTime = System.nanoTime();
        for (int i = 0; i < 1; i++) {
            if (i % 10000 == 0)
                System.out.println("i: " + i);


            final MongoQueryFilterDocument filter = new MongoQueryFilterDocument("int", 42);

            MongoQueryMessage message = new MongoQueryMessage("MongoConnectionTest.sendMessageTest",
                    new MongoFind(filter).readPreference(ReadPreference.primary()),
                    new PooledByteBufferOutput(bufferPool), serializers);

            channel.sendMessage(message);

            MongoReplyMessage<MongoDocument> replyMessage = channel.receiveMessage(serializers, MongoDocument.class);

//            assertEquals(replyMessage.getDocuments().size(), 101);
        }

        long endTime = System.nanoTime();
        System.out.println((endTime - startTime) / 1000000);
        System.out.flush();

    }

    private void dropCollection(String collectionName) throws IOException {
        final MongoCommandDocument filter = new MongoCommandDocument("drop", collectionName);

        MongoQueryMessage message = new MongoQueryMessage("MongoConnectionTest.$cmd",
                new MongoFind(filter).batchSize(-1).readPreference(ReadPreference.primary()),
                new PooledByteBufferOutput(bufferPool), serializers);
        channel.sendMessage(message);

        MongoReplyMessage<MongoDocument> replyMessage = channel.receiveMessage(serializers, MongoDocument.class);

        assertEquals(1, replyMessage.getDocuments().size());
    }

    private List<MongoDocument> insertTwoDocuments() throws IOException {
        List<MongoDocument> documents = new ArrayList<MongoDocument>();

        Map<String, Object> doc1 = new MongoDocument();
        doc1.put("_id", new ObjectId());
        doc1.put("str", "hi mom");
        doc1.put("int", 42);

//        Map<String, Object> doc2 = new LinkedHashMap<String, Object>();
//        doc2.put("_id", new ObjectId());
//        doc2.put("str", "hi dad");
//        doc2.put("int", 0);


        MongoInsertMessage<MongoDocument> message = new MongoInsertMessage<MongoDocument>("MongoConnectionTest.sendMessageTest",
                new MongoInsert<MongoDocument>(documents).writeConcern(WriteConcern.UNACKNOWLEDGED), MongoDocument.class,
                new PooledByteBufferOutput(bufferPool), serializers);


        message.addDocument(MongoDocument.class, doc1, serializers);
//        message.addDocument(MongoDocument.class, doc2, serializers);

        channel.sendMessage(message);

        return documents;
    }
}
