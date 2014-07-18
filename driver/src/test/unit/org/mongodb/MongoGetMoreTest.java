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

package org.mongodb;

import com.mongodb.ServerAddress;
import com.mongodb.operation.GetMore;
import org.junit.Test;

import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;

public class MongoGetMoreTest {

    @Test
    public void testGetters() throws UnknownHostException {
        GetMore getMore = new GetMore(new ServerCursor(1, new ServerAddress()), 20, -40, 5);
        assertEquals(20, getMore.getLimit());
        assertEquals(-40, getMore.getBatchSize());
        assertEquals(5, getMore.getNumFetchedSoFar());
    }

    @Test
    public void testNumberToReturn() throws UnknownHostException {
        GetMore getMore = new GetMore(new ServerCursor(1, new ServerAddress()), 0, 0, 5);
        assertEquals(0, getMore.getNumberToReturn());

        getMore = new GetMore(new ServerCursor(1, new ServerAddress()), 0, 40, 5);
        assertEquals(40, getMore.getNumberToReturn());

        getMore = new GetMore(new ServerCursor(1, new ServerAddress()), 0, -40, 5);
        assertEquals(-40, getMore.getNumberToReturn());

        getMore = new GetMore(new ServerCursor(1, new ServerAddress()), 20, 0, 5);
        assertEquals(15, getMore.getNumberToReturn());

        getMore = new GetMore(new ServerCursor(1, new ServerAddress()), 20, 10, 5);
        assertEquals(10, getMore.getNumberToReturn());

        getMore = new GetMore(new ServerCursor(1, new ServerAddress()), 20, -40, 5);
        assertEquals(15, getMore.getNumberToReturn());
    }
}
