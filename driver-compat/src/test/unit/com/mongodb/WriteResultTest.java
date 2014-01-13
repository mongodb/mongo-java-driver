/*
 * Copyright (c) 2008 MongoDB, Inc.
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

import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WriteResultTest {
    @Test
    public void testWithCommandResult() throws UnknownHostException {
        CommandResult commandResult = new CommandResult(new BasicDBObject("ok", 1).append("n", 0).append("connectionId", 431),
                                                        new ServerAddress("localhost", 27017));
        WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;
        WriteResult writeResult = new WriteResult(commandResult, writeConcern);
        assertEquals(commandResult, writeResult.getCachedLastError());
        assertEquals(commandResult, writeResult.getLastError());
        assertEquals(writeConcern, writeResult.getLastConcern());
        assertNull(writeResult.getError());
        assertEquals(0, writeResult.getN());
        assertEquals(431, writeResult.getField("connectionId"));
        assertFalse(writeResult.isLazy());
        assertTrue(writeResult.toString().startsWith("WriteResult"));
    }

    @Test
    public void testWithoutCommandResult() throws UnknownHostException {
        WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;
        WriteResult writeResult = new WriteResult(null, writeConcern);

        assertEquals(writeConcern, writeResult.getLastConcern());
        assertTrue(writeResult.isLazy());
        assertTrue(writeResult.toString().startsWith("WriteResult"));

        assertEquals(null, writeResult.getCachedLastError());
        try {
            writeResult.getLastError();
            fail();
        } catch (MongoException e) {
        }

        try {
            writeResult.getLastError();
            fail();
        } catch (MongoException e) {
        }
        try {
            writeResult.getError();
            fail();
        } catch (MongoException e) {
        }

        try {
            writeResult.getN();
            fail();
        } catch (MongoException e) {
        }

        try {
            writeResult.getField("connectionId");
            fail();
        } catch (MongoException e) {
        }
    }
}
