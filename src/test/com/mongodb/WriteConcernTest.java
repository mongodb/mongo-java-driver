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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.UnknownHostException;

public class WriteConcernTest extends TestCase {

    @Test
    public void testEqualityAndHashCode() {
        Assert.assertEquals(new WriteConcern("majority"), new WriteConcern("majority"));
        Assert.assertEquals(new WriteConcern("majority").hashCode(), new WriteConcern("majority").hashCode());
        Assert.assertNotEquals(new WriteConcern("majority"), new WriteConcern(1));
        Assert.assertNotEquals(new WriteConcern("majority").hashCode(), new WriteConcern(1).hashCode());

        Assert.assertEquals(new WriteConcern(1), WriteConcern.ACKNOWLEDGED);
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
        Assert.assertEquals(false, object2.getContinueOnError());
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
        Assert.assertEquals(false, object2.getContinueOnError());
    }

    @Test
    public void testCheckLastError() {
        Assert.assertFalse(WriteConcern.NONE.callGetLastError());
        Assert.assertFalse(WriteConcern.NORMAL.callGetLastError());
        Assert.assertFalse(WriteConcern.UNACKNOWLEDGED.callGetLastError());
        Assert.assertTrue(WriteConcern.SAFE.callGetLastError());
        Assert.assertTrue(WriteConcern.ACKNOWLEDGED.callGetLastError());
        Assert.assertTrue(WriteConcern.FSYNC_SAFE.callGetLastError());
        Assert.assertTrue(WriteConcern.JOURNAL_SAFE.callGetLastError());
        Assert.assertFalse(WriteConcern.ERRORS_IGNORED.callGetLastError());
        Assert.assertTrue(WriteConcern.JOURNALED.callGetLastError());
        Assert.assertTrue(WriteConcern.FSYNCED.callGetLastError());
        Assert.assertTrue(WriteConcern.REPLICA_ACKNOWLEDGED.callGetLastError());
        Assert.assertTrue(WriteConcern.MAJORITY.callGetLastError());
        Assert.assertTrue(WriteConcern.REPLICAS_SAFE.callGetLastError());
        Assert.assertTrue(new WriteConcern("custom").callGetLastError());
        Assert.assertFalse(new WriteConcern(0, 1000).callGetLastError());
        Assert.assertFalse(new WriteConcern(0, 0, true, false).callGetLastError());
        Assert.assertFalse(new WriteConcern(0, 0, false, true).callGetLastError());
    }

    @Test
    public void testW() {
        Assert.assertEquals(-1, WriteConcern.NONE.getW());
        Assert.assertEquals(0, WriteConcern.NORMAL.getW());
        Assert.assertEquals(0, WriteConcern.UNACKNOWLEDGED.getW());
        Assert.assertEquals(1, WriteConcern.SAFE.getW());
        Assert.assertEquals(1, WriteConcern.ACKNOWLEDGED.getW());
        Assert.assertEquals(1, WriteConcern.FSYNC_SAFE.getW());
        Assert.assertEquals(1, WriteConcern.JOURNAL_SAFE.getW());
        Assert.assertEquals(-1, WriteConcern.ERRORS_IGNORED.getW());
        Assert.assertEquals(1, WriteConcern.JOURNALED.getW());
        Assert.assertEquals(1, WriteConcern.FSYNCED.getW());
        Assert.assertEquals(2, WriteConcern.REPLICA_ACKNOWLEDGED.getW());
        Assert.assertEquals("majority", WriteConcern.MAJORITY.getWString());
        Assert.assertEquals(2, WriteConcern.REPLICAS_SAFE.getW());
        Assert.assertEquals("custom", new WriteConcern("custom").getWString());
    }

    @Test
    public void testRaiseNetworkErrors() {
        Assert.assertFalse(WriteConcern.NONE.raiseNetworkErrors());
        Assert.assertTrue(WriteConcern.NORMAL.raiseNetworkErrors());
        Assert.assertTrue(WriteConcern.UNACKNOWLEDGED.raiseNetworkErrors());
        Assert.assertTrue(WriteConcern.SAFE.raiseNetworkErrors());
        Assert.assertTrue(WriteConcern.ACKNOWLEDGED.raiseNetworkErrors());
        Assert.assertTrue(WriteConcern.FSYNC_SAFE.raiseNetworkErrors());
        Assert.assertTrue(WriteConcern.JOURNAL_SAFE.raiseNetworkErrors());
        Assert.assertFalse(WriteConcern.ERRORS_IGNORED.raiseNetworkErrors());
        Assert.assertTrue(WriteConcern.JOURNALED.raiseNetworkErrors());
        Assert.assertTrue(WriteConcern.FSYNCED.raiseNetworkErrors());
        Assert.assertTrue(WriteConcern.REPLICA_ACKNOWLEDGED.raiseNetworkErrors());
        Assert.assertTrue(WriteConcern.MAJORITY.raiseNetworkErrors());
        Assert.assertTrue(WriteConcern.REPLICAS_SAFE.raiseNetworkErrors());
        Assert.assertTrue(new WriteConcern("custom").raiseNetworkErrors());
    }

    @Test
    public void testGetLastErrorCommand() {
        assertEquals(new BasicDBObject("getlasterror", 1), WriteConcern.UNACKNOWLEDGED.getCommand());
        assertEquals(new BasicDBObject("getlasterror", 1), WriteConcern.ACKNOWLEDGED.getCommand());
        assertEquals(new BasicDBObject("getlasterror", 1), new WriteConcern(1).getCommand());
        assertEquals(new BasicDBObject("getlasterror", 1).append("wtimeout", 1000), new WriteConcern(0, 1000).getCommand());
        assertEquals(new BasicDBObject("getlasterror", 1).append("fsync", true), new WriteConcern(0, 0, true, false).getCommand());
        assertEquals(new BasicDBObject("getlasterror", 1).append("j", true), new WriteConcern(0, 0, false, true).getCommand());
    }


    // integration test to ensure that server doesn't mind a getlasterror command with wtimeout but no w.
    @Test
    public void testGetLastError() throws UnknownHostException {
        MongoClient mc = new MongoClient();
        DB db = mc.getDB("WriteConcernTest");
        DBCollection collection = db.getCollection("testGetLastError");
        try {
            WriteConcern wc = new WriteConcern(0, 1000);
            WriteResult res = collection.insert(new BasicDBObject(), wc);
            Assert.assertTrue(res.getLastError().ok());
        } finally {
            db.dropDatabase();
            mc.close();
        }
    }
}
