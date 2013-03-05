// DBTCPConnectorTest.java

/**
 *      Copyright (C) 2011 10gen Inc.
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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * Testing functionality of database TCP connector.  The structure of this class is a bit unusual,
 * as it creates its own MongoClient, yet still extends TestCase, which has its own.
 */
public class DBTCPConnectorTest extends TestCase {

    private MongoClient _mongoClient;
    private DB _db;
    private DBCollection _collection;
    private DBTCPConnector _connector;

    @BeforeClass
    public void beforeClass() throws UnknownHostException {
        _mongoClient = new MongoClient(Arrays.asList(new ServerAddress("localhost:27017"), new ServerAddress("localhost:27018")));
        cleanupDB = "com_mongodb_DBTCPConnectorTest";
        _db = _mongoClient.getDB(cleanupDB);
        _collection = _db.getCollection("testCol");
    }

    @AfterClass
    public void afterClass() {
        _mongoClient.close();
        _connector.close();
    }

    @BeforeMethod
    public void beforeMethod() throws UnknownHostException {
        _connector = new DBTCPConnector(_mongoClient);
        _connector.start();
    }

    /**
     * Test request reservation
     */
    @Test
    public void testRequestReservation() {
        assertEquals(false, _connector.getMyPort()._inRequest);
        _connector.requestStart();
        try {
            assertNull(_connector.getMyPort()._requestPort);
            assertEquals(true, _connector.getMyPort()._inRequest);
            _connector.requestDone();
            assertEquals(false, _connector.getMyPort()._inRequest);
        } finally {
            _connector.requestDone();
        }
    }

    /**
     * Tests that same connections is used for sequential writes
     */
    @Test
    public void testConnectionReservationForWrites() {
        _connector.requestStart();
        try {
            _connector.say(_db, createOutMessageForInsert(), WriteConcern.SAFE);
            assertNotNull(_connector.getMyPort()._requestPort);
            DBPort requestPort = _connector.getMyPort()._requestPort;
            _connector.say(_db, createOutMessageForInsert(), WriteConcern.SAFE);
            assertEquals(requestPort, _connector.getMyPort()._requestPort);
        } finally {
            _connector.requestDone();
        }
    }

    /**
     * Tests that same connections is used for write followed by read
     */
    @Test
    public void testConnectionReservationForWriteThenRead() {
        _connector.requestStart();
        try {
            _connector.say(_db, createOutMessageForInsert(), WriteConcern.SAFE);
            DBPort requestPort = _connector.getMyPort()._requestPort;
            _connector.call(_db, _collection,
                    OutMessage.query(_collection, 0, 0, -1, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary()),
                    null, 0);
            assertEquals(requestPort, _connector.getMyPort()._requestPort);
        } finally {
            _connector.requestDone();
        }
    }

    /**
     * Test that request port changes when read is followed by write with connection reservation
     */
    @Test
    public void testConnectionReservationForReadThenWrite() {
        if (isStandalone(cleanupMongo)) {
            return;
        }

        _connector.requestStart();
        try {
            _connector.call(_db, _collection,
                    OutMessage.query(_collection, 0, 0, -1, new BasicDBObject(), new BasicDBObject(), ReadPreference.secondary()),
                    null, 0, ReadPreference.secondary(), null);
            DBPort requestPort = _connector.getMyPort()._requestPort;
            _connector.say(_db, createOutMessageForInsert(), WriteConcern.SAFE);
            assertNotEquals(requestPort, _connector.getMyPort()._requestPort);
            assertEquals(_connector.getReplicaSetStatus().getMaster(), _connector.getMyPort()._requestPort.serverAddress());
        } finally {
            _connector.requestDone();
        }
    }

    /**
     * Tests that same connections is used for sequential reads
     */
    @Test
    public void testConnectionReservationForReads() {
        _connector.requestStart();
        try {
            _connector.call(_db, _collection,
                    OutMessage.query(_collection, 0, 0, -1, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary()),
                    null, 0);
            assertNotNull(_connector.getMyPort()._requestPort);
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
