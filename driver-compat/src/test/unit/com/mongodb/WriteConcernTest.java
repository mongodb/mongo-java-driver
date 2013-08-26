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

package com.mongodb;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class WriteConcernTest {
    @Test
    public void testConstructors() {
        WriteConcern wc = new WriteConcern();
        assertEquals(0, wc.getW());
        assertEquals(0, wc.getWtimeout());
        assertEquals(false, wc.getFsync());
        assertEquals(false, wc.getJ());
        assertEquals(false, wc.getContinueOnError());

        wc = new WriteConcern(1);
        assertEquals(1, wc.getW());
        assertEquals(0, wc.getWtimeout());
        assertEquals(false, wc.getFsync());
        assertEquals(false, wc.getJ());
        assertEquals(false, wc.getContinueOnError());

        wc = new WriteConcern("majority");
        assertEquals("majority", wc.getWString());
        assertEquals(0, wc.getWtimeout());
        assertEquals(false, wc.getFsync());
        assertEquals(false, wc.getJ());
        assertEquals(false, wc.getContinueOnError());

        wc = new WriteConcern(1, 10);
        assertEquals(1, wc.getW());
        assertEquals(10, wc.getWtimeout());
        assertEquals(false, wc.getFsync());
        assertEquals(false, wc.getJ());
        assertEquals(false, wc.getContinueOnError());

        wc = new WriteConcern(true);
        assertEquals(1, wc.getW());
        assertEquals(0, wc.getWtimeout());
        assertEquals(true, wc.getFsync());
        assertEquals(false, wc.getJ());
        assertEquals(false, wc.getContinueOnError());

        wc = new WriteConcern(1, 10, true);
        assertEquals(1, wc.getW());
        assertEquals(10, wc.getWtimeout());
        assertEquals(true, wc.getFsync());
        assertEquals(false, wc.getJ());
        assertEquals(false, wc.getContinueOnError());

        wc = new WriteConcern(1, 10, false, true);
        assertEquals(1, wc.getW());
        assertEquals(10, wc.getWtimeout());
        assertEquals(false, wc.getFsync());
        assertEquals(true, wc.getJ());
        assertEquals(false, wc.getContinueOnError());

        wc = new WriteConcern(1, 10, false, true, true);
        assertEquals(1, wc.getW());
        assertEquals(10, wc.getWtimeout());
        assertEquals(false, wc.getFsync());
        assertEquals(true, wc.getJ());
        assertEquals(true, wc.getContinueOnError());

        wc = new WriteConcern("dc1", 10, false, true);
        assertEquals("dc1", wc.getWString());
        assertEquals(10, wc.getWtimeout());
        assertEquals(false, wc.getFsync());
        assertEquals(true, wc.getJ());
        assertEquals(false, wc.getContinueOnError());

        wc = new WriteConcern("dc1", 10, false, true, true);
        assertEquals("dc1", wc.getWString());
        assertEquals(10, wc.getWtimeout());
        assertEquals(false, wc.getFsync());
        assertEquals(true, wc.getJ());
        assertEquals(true, wc.getContinueOnError());
    }

    @Test
    public void testConstants() {
        assertEquals(new WriteConcern(org.mongodb.WriteConcern.ERRORS_IGNORED), WriteConcern.ERRORS_IGNORED);
        assertEquals(new WriteConcern(org.mongodb.WriteConcern.UNACKNOWLEDGED), WriteConcern.UNACKNOWLEDGED);
        assertEquals(new WriteConcern(org.mongodb.WriteConcern.ACKNOWLEDGED), WriteConcern.ACKNOWLEDGED);
        assertEquals(new WriteConcern(org.mongodb.WriteConcern.FSYNCED), WriteConcern.FSYNCED);
        assertEquals(new WriteConcern(org.mongodb.WriteConcern.JOURNALED), WriteConcern.JOURNALED);
        assertEquals(new WriteConcern(org.mongodb.WriteConcern.REPLICA_ACKNOWLEDGED),
                    WriteConcern.REPLICA_ACKNOWLEDGED);
        assertEquals(new WriteConcern("majority"), WriteConcern.MAJORITY);

        assertEquals(WriteConcern.ERRORS_IGNORED, WriteConcern.NONE);
        assertEquals(WriteConcern.UNACKNOWLEDGED, WriteConcern.NORMAL);
        assertEquals(WriteConcern.ACKNOWLEDGED, WriteConcern.SAFE);
        assertEquals(WriteConcern.FSYNCED, WriteConcern.FSYNC_SAFE);
        assertEquals(WriteConcern.JOURNALED, WriteConcern.JOURNAL_SAFE);
        assertEquals(WriteConcern.REPLICA_ACKNOWLEDGED, WriteConcern.REPLICAS_SAFE);
    }

    @Test
    public void testGetters() {
        WriteConcern wc = new WriteConcern("dc1", 10, true, true, true);
        assertEquals(true, wc.fsync());
        assertEquals(true, wc.callGetLastError());
        assertEquals(true, wc.raiseNetworkErrors());
        assertEquals("dc1", wc.getWObject());
        assertEquals(new BasicDBObject("getlasterror", 1).append("w", "dc1").
                                                                            append("wtimeout", 10).append("fsync", true)
                                                         .append("j", true), wc.getCommand());

        wc = new WriteConcern(-1, 10, false, true, true);
        assertEquals(false, wc.fsync());
        assertEquals(false, wc.callGetLastError());
        assertEquals(false, wc.raiseNetworkErrors());
        assertEquals(-1, wc.getWObject());
    }

    @Test
    public void testEquals() {
        assertTrue(WriteConcern.ACKNOWLEDGED.equals(WriteConcern.ACKNOWLEDGED));
        assertFalse(WriteConcern.ACKNOWLEDGED.equals(null));
        assertFalse(WriteConcern.ACKNOWLEDGED.equals(WriteConcern.UNACKNOWLEDGED));
        assertFalse(WriteConcern.ACKNOWLEDGED.equals(org.mongodb.WriteConcern.ACKNOWLEDGED));
        assertFalse(new WriteConcern("majority").equals(new WriteConcern.Majority()));
    }

    @Test
    public void testHashCode() {
        assertEquals(WriteConcern.ACKNOWLEDGED.hashCode(), org.mongodb.WriteConcern.ACKNOWLEDGED.hashCode());
    }

    @Test
    public void testToString() {
        assertEquals(WriteConcern.ACKNOWLEDGED.toString(), org.mongodb.WriteConcern.ACKNOWLEDGED.toString());
    }

    @Test
    public void testToNew() {
        assertEquals(WriteConcern.ACKNOWLEDGED.toNew(), org.mongodb.WriteConcern.ACKNOWLEDGED);
    }

    @Test
    public void testValueOf() {
        assertEquals(WriteConcern.ACKNOWLEDGED, WriteConcern.valueOf("ACKNOWLEDGED"));
        assertEquals(WriteConcern.ACKNOWLEDGED, WriteConcern.valueOf("acknowledged"));
        assertNull(WriteConcern.valueOf("blahblah"));
    }

    @Test
    public void testContinueOnErrorForInsert() {
        assertTrue(WriteConcern.ACKNOWLEDGED.continueOnError(true).getContinueOnError());
        assertFalse(new WriteConcern(1, 0, false, false, true).continueOnError(
                false)
                                                              .getContinueOnError());
    }

    @Test
    public void testMajorityWriteConcern() {
        WriteConcern.Majority wc = new WriteConcern.Majority();
        assertEquals("majority", wc.getWString());
        assertEquals(0, wc.getWtimeout());
        assertEquals(false, wc.getFsync());
        assertEquals(false, wc.getJ());
        assertEquals(false, wc.getContinueOnError());

        wc = new WriteConcern.Majority(10, true, true);
        assertEquals("majority", wc.getWString());
        assertEquals(10, wc.getWtimeout());
        assertEquals(true, wc.getFsync());
        assertEquals(true, wc.getJ());
        assertEquals(false, wc.getContinueOnError());

        assertEquals(new WriteConcern.Majority(10, true, true), WriteConcern.majorityWriteConcern(10, true, true));
    }
}
