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

package com.mongodb;

import org.junit.Ignore;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Ignore("Doesn't work offline")
@SuppressWarnings("deprecation")
public class DBAddressTest {

    @Test
    public void testConstructors() throws UnknownHostException {
        DBAddress first = new DBAddress("www.10gen.com:1000/some.host");
        assertTrue(first.sameHost("www.10gen.com:1000"));

        DBAddress second = new DBAddress(first, "some.other.host");
        assertEquals(first.getSocketAddress().hashCode(), second.getSocketAddress().hashCode());

        assertEquals("some.other.host", second.getDBName());
        DBAddress third = new DBAddress(InetAddress.getByName("localhost"), 27017, "some.db");
        assertEquals("some.db", third.getDBName());

        DBAddress fourth = third.getSister("some.other.db");
        assertEquals("some.other.db", fourth.getDBName());
    }

    @Test
    public void testInvalid() throws UnknownHostException {
        try {
            new DBAddress(null);
            fail();
        } catch (NullPointerException e) { // NOPMD
            // all good
        }

        try {
            new DBAddress("  \t\n");
            fail();
        } catch (IllegalArgumentException e) { // NOPMD
            // all good
        }
    }

    @Test
    public void testBasics() throws UnknownHostException {
        assertEquals(27017, new ServerAddress().getPort());
        assertEquals(27017, new ServerAddress("localhost").getPort());
        assertEquals(9999, new ServerAddress("localhost:9999").getPort());
    }

    @Test
    public void testCons3() throws UnknownHostException {
        DBAddress a = new DBAddress("9.9.9.9:9999", "abc");
        assertEquals("9.9.9.9", a.getHost());
        assertEquals(9999, a.getPort());
        assertEquals("abc", a.getDBName());
    }
}
