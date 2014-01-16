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

// DBTCPConnectorTest.java

package com.mongodb;

import com.mongodb.util.TestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Testing functionality of database TCP connector.  The structure of this class is a bit unusual,
 * as it creates its own MongoClient, yet still extends TestCase, which has its own.
 */
public class DBTCPConnectorTest extends TestCase {

    private static MongoClient _mongoClient;
    private static DB _db;
    private static DBCollection _collection;
    private DBTCPConnector _connector;

    @BeforeClass
    public static void beforeClass() throws UnknownHostException {
        if (isStandalone(getMongoClient())) {
            _mongoClient = new MongoClient();
        }
        else {
            _mongoClient = new MongoClient(Arrays.asList(new ServerAddress("localhost:27017"), new ServerAddress("localhost:27018")));
        }
        _db = _mongoClient.getDB(getDatabase().getName());
        _collection = _db.getCollection(DBTCPConnectorTest.class.getName());
    }

    @AfterClass
    public static void afterClass() {
        _mongoClient.close();
    }

    @Before
    public void beforeMethod() throws UnknownHostException {
        _connector = new DBTCPConnector(_mongoClient);
        _connector.start();
    }

    @After
    public void afterMethod() {
        _connector.close();
    }

    /**
     * Test request reservation
     */
    @Test
    public void testRequestReservation() {
        final DBTCPConnector.MyPort myPort = _connector.getMyPort();
        assertNull(myPort.getPinnedRequestStatusForThread());
        _connector.requestStart();
        try {
            assertNull(myPort.getPinnedRequestPortForThread());
            assertNotNull(myPort.getPinnedRequestStatusForThread());
            _connector.requestDone();
            assertNull(myPort.getPinnedRequestStatusForThread());
        } finally {
            _connector.requestDone();
        }
        assertNull(myPort.getPinnedRequestPortForThread());
    }

    /**
     * Tests that same connections is used for sequential writes
     */
    @Test
    public void testConnectionReservationForWrites() {
        DBTCPConnector.MyPort myPort = _connector.getMyPort();
        _connector.requestStart();
        try {
            _connector.say(_db, createOutMessageForInsert(), WriteConcern.SAFE);
            assertNotNull(myPort.getPinnedRequestStatusForThread());
            Connection requestPort = myPort.getPinnedRequestPortForThread();
            _connector.say(_db, createOutMessageForInsert(), WriteConcern.SAFE);
            assertEquals(requestPort, myPort.getPinnedRequestPortForThread());
        } finally {
            _connector.requestDone();
        }
    }

    /**
     * Tests that same connections is used for write followed by read
     */
    @Test
    public void testConnectionReservationForWriteThenRead() {
        DBTCPConnector.MyPort myPort = _connector.getMyPort();
        _connector.requestStart();
        try {
            _connector.say(_db, createOutMessageForInsert(), WriteConcern.SAFE);
            Connection requestPort = myPort.getPinnedRequestPortForThread();
            _connector.call(_db, _collection,
                    OutMessage.query(_collection, 0, 0, -1, new BasicDBObject(), new BasicDBObject(), Bytes.MAX_OBJECT_SIZE),
                    null, 0);
            assertEquals(requestPort, myPort.getPinnedRequestPortForThread());
        } finally {
            _connector.requestDone();
        }
    }

    /**
     * Test that request port changes when read is followed by write with connection reservation
     */
    @Test
    public void testConnectionReservationForReadThenWrite() {
        if (!isReplicaSet(cleanupMongo)) {
            return;
        }

        DBTCPConnector.MyPort myPort = _connector.getMyPort();
        _connector.requestStart();
        try {
            _connector.call(_db, _collection,
                    OutMessage.query(_collection, 0, 0, -1, new BasicDBObject(), new BasicDBObject(), ReadPreference.secondary(),
                                     DefaultDBEncoder.FACTORY.create()),
                    null, 0, ReadPreference.secondary(), null);
            Connection requestPort = myPort.getPinnedRequestPortForThread();
            _connector.say(_db, createOutMessageForInsert(), WriteConcern.SAFE);
            assertNotEquals(requestPort, myPort.getPinnedRequestPortForThread());
            DBTCPConnector.PinnedRequestStatus status = myPort.getPinnedRequestStatusForThread();
            assertEquals(_connector.getReplicaSetStatus().getMaster(), myPort.getPinnedRequestPortForThread().serverAddress());
        } finally {
            _connector.requestDone();
        }
    }

    /**
     * Tests that same connections is used for sequential reads
     */
    @Test
    public void testConnectionReservationForReads() {
        DBTCPConnector.MyPort myPort = _connector.getMyPort();
        _connector.requestStart();
        try {
            _connector.call(_db, _collection,
                    OutMessage.query(_collection, 0, 0, -1, new BasicDBObject(), new BasicDBObject(), Bytes.MAX_OBJECT_SIZE),
                    null, 0);
            assertNotNull(myPort.getPinnedRequestPortForThread());
        } finally {
            _connector.requestDone();
        }
    }


    private OutMessage createOutMessageForInsert() {
        OutMessage om = OutMessage.insert(_collection, new DefaultDBEncoder(), WriteConcern.NONE);
        om.putObject( new BasicDBObject() );

        return om;
    }
}
