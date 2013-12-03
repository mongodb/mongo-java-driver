/*
 * Copyright (c) 2008 - 2013 MongoDB Inc., Inc. <http://mongodb.com>
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

import com.mongodb.util.TestCase;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 */
public class ErrorTest extends TestCase {
    @Test
    public void testLastError()
        throws MongoException {

        getDatabase().resetError();
        assertNull(getDatabase().getLastError().get("err"));

        getDatabase().forceError();

        assertNotNull(getDatabase().getLastError().get("err"));

        getDatabase().resetError();
        assertNull(getDatabase().getLastError().get("err"));
    }

    @Test
    public void testLastErrorWithConcern()
        throws MongoException {

        getDatabase().resetError();
        CommandResult cr = getDatabase().getLastError(WriteConcern.FSYNC_SAFE);
        assertNull(cr.get("err"));
        assertTrue(cr.containsField("fsyncFiles") || cr.containsField("waited"));
    }

    @Test
    public void testLastErrorWithConcernAndW()
        throws MongoException {
        if ( /* TODO: running with slaves */ false ){
            getDatabase().resetError();
            CommandResult cr = getDatabase().getLastError(WriteConcern.REPLICAS_SAFE);
            assertNull(cr.get("err"));
            assertTrue(cr.containsField("wtime"));
        }
    }

    @Test
    public void testPrevError()
        throws MongoException {

        getDatabase().resetError();

        assertNull(getDatabase().getLastError().get("err"));
        assertNull(getDatabase().getPreviousError().get("err"));

        getDatabase().forceError();

        assertNotNull(getDatabase().getLastError().get("err"));
        assertNotNull(getDatabase().getPreviousError().get("err"));

        getDatabase().getCollection("misc").insert(new BasicDBObject("foo", 1), WriteConcern.UNACKNOWLEDGED);

        assertNull(getDatabase().getLastError().get("err"));
        assertNotNull(getDatabase().getPreviousError().get("err"));

        getDatabase().resetError();

        assertNull(getDatabase().getLastError().get("err"));
        assertNull(getDatabase().getPreviousError().get("err"));
    }
}
