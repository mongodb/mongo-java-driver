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

package org.mongodb;

import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.bson.io.PooledByteBufferOutput;
import org.bson.types.Binary;
import org.bson.types.Document;
import org.bson.types.ObjectId;
import org.bson.util.BufferPool;
import org.bson.util.PowerOfTwoByteBufferPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.io.MongoChannel;
import org.mongodb.operation.GetMore;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoInsert;
import org.mongodb.protocol.MongoGetMoreMessage;
import org.mongodb.protocol.MongoInsertMessage;
import org.mongodb.protocol.MongoQueryMessage;
import org.mongodb.protocol.MongoReplyMessage;
import org.mongodb.serialization.BsonSerializationOptions;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.serializers.DocumentSerializer;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MongoChannelTest {
    private MongoChannel channel;
    private PrimitiveSerializers primitiveSerializers;
    private BufferPool<ByteBuffer> bufferPool;

    @Before
    public void setUp() throws UnknownHostException, SocketException {
        bufferPool = new PowerOfTwoByteBufferPool(24);
        primitiveSerializers = PrimitiveSerializers.createDefault();
        channel = new MongoChannel(new ServerAddress("localhost", 27017), bufferPool,
                                   new DocumentSerializer(primitiveSerializers));
    }

    @After
    public void tearDown() {
        channel.close();
    }

    static class Concrete {
        private ObjectId id;
        private String str;
        private int i;
        private long l;
        private double d;
        private long date;

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

    static class ConcreteSerializer implements Serializer<Concrete> {

        @Override
        public void serialize(final BSONWriter bsonWriter, final Concrete c, final BsonSerializationOptions options) {
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
        public Concrete deserialize(final BSONReader reader, final BsonSerializationOptions options) {
            final Concrete c = new Concrete();
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

        @Override
        public Class<Concrete> getSerializationClass() {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    public void concreteClassTest() throws IOException {
        final Concrete c = new Concrete(new ObjectId(), "hi mom", 42, 42L, 42.0, new Date().getTime());

        final MongoInsertMessage<Concrete> insertMessage = new MongoInsertMessage<Concrete>("test.concrete",
                new MongoInsert<Concrete>(c).writeConcern(WriteConcern.UNACKNOWLEDGED),
                new PooledByteBufferOutput(bufferPool), new ConcreteSerializer());

        channel.sendOneWayMessage(insertMessage);

        final QueryFilterDocument filter = new QueryFilterDocument("i", 42);

        final MongoQueryMessage queryMessage = new MongoQueryMessage("test.concrete", new MongoFind(filter).readPreference(ReadPreference.primary()),
                new PooledByteBufferOutput(bufferPool), new DocumentSerializer(primitiveSerializers));

        final MongoReplyMessage<Concrete> replyMessage =  channel.sendQueryMessage(queryMessage, new ConcreteSerializer());
        System.out.println(replyMessage.getDocuments());
    }

    @Test
    public void sendMessageTest() throws IOException, InterruptedException {
        dropCollection("sendMessageTest");

        long startTime = System.nanoTime();
        final int iterations = 10000;
        final byte[] bytes = new byte[8192];
        for (int i = 0; i < iterations; i++) {
            if ((i > 0) && (i % 10000 == 0)) {
                System.out.println("i: " + i);
            }

            final Document doc1 = new Document();
            doc1.put("_id", new ObjectId());
            doc1.put("str", "hi mom");
            doc1.put("int", 42);
            doc1.put("long", 42L);
            doc1.put("double", 42.0);
            doc1.put("date", new Date());
            doc1.put("binary", new Binary(bytes));

            final MongoInsertMessage<Document> message = new MongoInsertMessage<Document>("MongoConnectionTest.sendMessageTest",
                    new MongoInsert<Document>(doc1).writeConcern(WriteConcern.ACKNOWLEDGED),
                    new PooledByteBufferOutput(bufferPool), new DocumentSerializer(primitiveSerializers));

            channel.sendOneWayMessage(message);

        }

        long endTime = System.nanoTime();
        double elapsedTime = (endTime - startTime) / (double) 1000000000;
        System.out.println("Time: " + elapsedTime + " sec");
        System.out.println(iterations / elapsedTime + " messages/sec");
        System.out.flush();

        System.out.println();

        System.out.println("Query time...");

        final QueryFilterDocument filter = new QueryFilterDocument();

        startTime = endTime;

        final MongoQueryMessage queryMessage = new MongoQueryMessage("MongoConnectionTest.sendMessageTest",
                new MongoFind(filter).batchSize(4000000).readPreference(ReadPreference.primary()),
                new PooledByteBufferOutput(bufferPool), new DocumentSerializer(primitiveSerializers));

        int totalDocuments = 0;

        MongoReplyMessage<Document> replyMessage = channel.sendQueryMessage(queryMessage, new DocumentSerializer(primitiveSerializers));
        totalDocuments += replyMessage.getReplyHeader().getNumberReturned();
        System.out.println(" Initial: " + replyMessage.getDocuments().size() + " documents, " + replyMessage.getReplyHeader().getMessageLength() + " bytes");

        while (replyMessage.getReplyHeader().getCursorId() != 0) {
            final MongoGetMoreMessage getMoreMessage = new MongoGetMoreMessage("MongoConnectionTest.sendMessageTest",
                    new GetMore(replyMessage.getReplyHeader().getCursorId(), 0), new PooledByteBufferOutput(bufferPool));

            replyMessage = channel.sendGetMoreMessage(getMoreMessage, new DocumentSerializer(primitiveSerializers));
            totalDocuments += replyMessage.getReplyHeader().getNumberReturned();
            System.out.println(" Get more: " + replyMessage.getDocuments().size() + " documents, " + replyMessage.getReplyHeader().getMessageLength() + " bytes");
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

        insertDocuments();

        final QueryFilterDocument filter = new QueryFilterDocument("int", 42);

        final MongoQueryMessage message = new MongoQueryMessage("MongoConnectionTest.sendMessageTest",
                                                                new MongoFind(filter).readPreference(
                                                                        ReadPreference.primary()),
                                                                new PooledByteBufferOutput(bufferPool),
                                                                new DocumentSerializer(primitiveSerializers));

        final MongoReplyMessage<Document> replyMessage = channel.sendQueryMessage(message, new DocumentSerializer(
                primitiveSerializers));

        assertEquals(1, replyMessage.getDocuments().size());
    }

    private void dropCollection(final String collectionName) throws IOException {
        final CommandDocument filter = new CommandDocument("drop", collectionName);

        final MongoQueryMessage message = new MongoQueryMessage("MongoConnectionTest.$cmd",
                new MongoFind(filter).batchSize(-1).readPreference(ReadPreference.primary()),
                new PooledByteBufferOutput(bufferPool), new DocumentSerializer(primitiveSerializers));
        final MongoReplyMessage<Document> replyMessage = channel.sendQueryMessage(message, new DocumentSerializer(primitiveSerializers));

        assertEquals(1, replyMessage.getDocuments().size());
    }

    private List<Document> insertDocuments() throws IOException {
        final List<Document> documents = new ArrayList<Document>();

        final Document doc1 = new Document();
        doc1.put("_id", new ObjectId());
        doc1.put("str", "hi mom");
        doc1.put("int", 42);

        documents.add(doc1);

        final MongoInsertMessage<Document> message = new MongoInsertMessage<Document>("MongoConnectionTest.sendMessageTest",
                new MongoInsert<Document>(documents).writeConcern(WriteConcern.UNACKNOWLEDGED),
                new PooledByteBufferOutput(bufferPool), new DocumentSerializer(primitiveSerializers));


        message.addDocument(doc1, new DocumentSerializer(primitiveSerializers));

        channel.sendOneWayMessage(message);

        return documents;
    }
}
