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

package org.mongodb;


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
        assertEquals(false, wc.getContinueOnErrorForInsert());

        wc = new WriteConcern(1);
        assertEquals(1, wc.getW());
        assertEquals(0, wc.getWtimeout());
        assertEquals(false, wc.getFsync());
        assertEquals(false, wc.getJ());
        assertEquals(false, wc.getContinueOnErrorForInsert());

        wc = new WriteConcern("majority");
        assertEquals("majority", wc.getWString());
        assertEquals(0, wc.getWtimeout());
        assertEquals(false, wc.getFsync());
        assertEquals(false, wc.getJ());
        assertEquals(false, wc.getContinueOnErrorForInsert());

        wc = new WriteConcern(1, 10);
        assertEquals(1, wc.getW());
        assertEquals(10, wc.getWtimeout());
        assertEquals(false, wc.getFsync());
        assertEquals(false, wc.getJ());
        assertEquals(false, wc.getContinueOnErrorForInsert());

        wc = new WriteConcern(true);
        assertEquals(1, wc.getW());
        assertEquals(0, wc.getWtimeout());
        assertEquals(true, wc.getFsync());
        assertEquals(false, wc.getJ());
        assertEquals(false, wc.getContinueOnErrorForInsert());

        wc = new WriteConcern(1, 10, true);
        assertEquals(1, wc.getW());
        assertEquals(10, wc.getWtimeout());
        assertEquals(true, wc.getFsync());
        assertEquals(false, wc.getJ());
        assertEquals(false, wc.getContinueOnErrorForInsert());

        wc = new WriteConcern(1, 10, false, true);
        assertEquals(1, wc.getW());
        assertEquals(10, wc.getWtimeout());
        assertEquals(false, wc.getFsync());
        assertEquals(true, wc.getJ());
        assertEquals(false, wc.getContinueOnErrorForInsert());

        wc = new WriteConcern(1, 10, false, true, true);
        assertEquals(1, wc.getW());
        assertEquals(10, wc.getWtimeout());
        assertEquals(false, wc.getFsync());
        assertEquals(true, wc.getJ());
        assertEquals(true, wc.getContinueOnErrorForInsert());

        wc = new WriteConcern("dc1", 10, false, true);
        assertEquals("dc1", wc.getWString());
        assertEquals(10, wc.getWtimeout());
        assertEquals(false, wc.getFsync());
        assertEquals(true, wc.getJ());
        assertEquals(false, wc.getContinueOnErrorForInsert());

        wc = new WriteConcern("dc1", 10, false, true, true);
        assertEquals("dc1", wc.getWString());
        assertEquals(10, wc.getWtimeout());
        assertEquals(false, wc.getFsync());
        assertEquals(true, wc.getJ());
        assertEquals(true, wc.getContinueOnErrorForInsert());
    }

    @Test
    public void testGetters() {
        WriteConcern wc = new WriteConcern("dc1", 10, true, true, true);
        assertEquals(true, wc.getFsync());
        assertEquals(true, wc.callGetLastError());
        assertEquals(true, wc.raiseNetworkErrors());
        assertEquals("dc1", wc.getWObject());
        wc = new WriteConcern(-1, 10, false, true, true);
        assertEquals(false, wc.getFsync());
        assertEquals(false, wc.callGetLastError());
        assertEquals(false, wc.raiseNetworkErrors());
        assertEquals(-1, wc.getWObject());
    }

    @Test
    public void testHashCode() {
        assertEquals(923521, org.mongodb.WriteConcern.ACKNOWLEDGED.hashCode());
    }

    @Test
    public void testToString() {
        assertEquals("WriteConcern{w=2, wtimeout=100, fsync=true, j=true, continueOnErrorForInsert=true}",
                     new WriteConcern(2, 100, true, true, true).toString());
    }

    @Test
    public void testWithMethods() {
        assertEquals(WriteConcern.ACKNOWLEDGED, WriteConcern.UNACKNOWLEDGED.withW(1));
        assertEquals(new WriteConcern("dc1"), WriteConcern.UNACKNOWLEDGED.withW("dc1"));

        assertEquals(WriteConcern.FSYNCED, WriteConcern.ACKNOWLEDGED.withFsync(true));
        assertEquals(new WriteConcern("dc1", 0, true, false), new WriteConcern("dc1").withFsync(true));

        assertEquals(WriteConcern.JOURNALED, WriteConcern.ACKNOWLEDGED.withJ(true));
        assertEquals(new WriteConcern("dc1", 0, false, true), new WriteConcern("dc1").withJ(true));

        assertEquals(new WriteConcern(1, 0, false, false, true), WriteConcern.ACKNOWLEDGED.withContinueOnErrorForInsert(true));
        assertEquals(new WriteConcern("dc1", 0, false, false, true), new WriteConcern("dc1").withContinueOnErrorForInsert(true));
    }

    @Test
    public void testCommand() {
        assertEquals(new Document("getlasterror", 1), WriteConcern.UNACKNOWLEDGED.getCommand());
        assertEquals(new Document("getlasterror", 1), WriteConcern.ACKNOWLEDGED.getCommand());
        assertEquals(new Document("getlasterror", 1).append("w", 2), WriteConcern.REPLICA_ACKNOWLEDGED.getCommand());
        assertEquals(new Document("getlasterror", 1).append("w", "majority"), new WriteConcern("majority").getCommand());
        assertEquals(new Document("getlasterror", 1).append("wtimeout",
                                                                   100), new WriteConcern(1, 100).getCommand());
        assertEquals(new Document("getlasterror", 1).append("fsync", true), WriteConcern.FSYNCED.getCommand());
        assertEquals(new Document("getlasterror", 1).append("j", true), WriteConcern.JOURNALED.getCommand());
    }

    @Test
    public void testEquals() {
        assertTrue(WriteConcern.ACKNOWLEDGED.equals(WriteConcern.ACKNOWLEDGED));
        assertFalse(WriteConcern.ACKNOWLEDGED.equals(null));
        assertFalse(WriteConcern.ACKNOWLEDGED.equals(WriteConcern.UNACKNOWLEDGED));
        assertFalse(new WriteConcern(1, 0, false, false, true).equals(new WriteConcern(1, 0, false, false, false)));
        assertFalse(new WriteConcern(1, 0, false, false).equals(new WriteConcern(1, 0, false, true)));
        assertFalse(new WriteConcern(1, 0, false, false).equals(new WriteConcern(1, 0, true, false)));
        assertFalse(new WriteConcern(1, 0).equals(new WriteConcern(1, 1)));
    }


    @Test
    public void testConstants() {
        assertEquals(new WriteConcern(-1), WriteConcern.ERRORS_IGNORED);
        assertEquals(new WriteConcern(1), WriteConcern.ACKNOWLEDGED);
        assertEquals(new WriteConcern(0), WriteConcern.UNACKNOWLEDGED);
        assertEquals(new WriteConcern(1, 0, true), WriteConcern.FSYNCED);
        assertEquals(new WriteConcern(1, 0, false, true), WriteConcern.JOURNALED);
        assertEquals(new WriteConcern(2), WriteConcern.REPLICA_ACKNOWLEDGED);
    }

    @Test
    public void testValueOf() {
        assertEquals(WriteConcern.ACKNOWLEDGED, WriteConcern.valueOf("ACKNOWLEDGED"));
        assertEquals(WriteConcern.ACKNOWLEDGED, WriteConcern.valueOf("acknowledged"));
        assertNull(WriteConcern.valueOf("blahblah"));
    }

}
