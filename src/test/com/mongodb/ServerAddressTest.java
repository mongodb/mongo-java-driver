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


import org.junit.Test;

import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;

public class ServerAddressTest {

    @Test
    public void testDefault() throws UnknownHostException {
        ServerAddress subject = new ServerAddress();

        assertEquals(ServerAddress.defaultHost(), subject.getHost());
        assertEquals(ServerAddress.defaultPort(), subject.getPort());
    }

    @Test
    public void testParseIPV4WithoutPort() throws UnknownHostException {
        ServerAddress subject = new ServerAddress("10.0.0.1");

        assertEquals("10.0.0.1", subject.getHost());
        assertEquals(ServerAddress.defaultPort(), subject.getPort());
    }

    @Test
    public void testParseIPV4WithPort() throws UnknownHostException {
        ServerAddress subject = new ServerAddress("10.0.0.1:1000");

        assertEquals("10.0.0.1", subject.getHost());
        assertEquals(1000, subject.getPort());
    }

    @Test
    public void testIPV4WithPort() throws UnknownHostException {
        ServerAddress subject = new ServerAddress("10.0.0.1", 1000);

        assertEquals("10.0.0.1", subject.getHost());
        assertEquals(1000, subject.getPort());
    }

    @Test
    public void testParseDnsNameWithoutPort() throws UnknownHostException {
        ServerAddress subject = new ServerAddress("somewhere");

        assertEquals("somewhere", subject.getHost());
        assertEquals(27017, subject.getPort());
    }

    @Test
    public void testParseIPV6WithoutPort() throws UnknownHostException {
        ServerAddress subject = new ServerAddress("[2010:836B:4179::836B:4179]");

        assertEquals("2010:836B:4179::836B:4179", subject.getHost());
        assertEquals(ServerAddress.defaultPort(), subject.getPort());
    }

    @Test
    public void testParseIPV6WithPort() throws UnknownHostException {
        ServerAddress subject = new ServerAddress("[2010:836B:4179::836B:4179]:1000");

        assertEquals("2010:836B:4179::836B:4179", subject.getHost());
        assertEquals(1000, subject.getPort());
    }

    @Test
    public void testIPV6WithPort() throws UnknownHostException {
        ServerAddress subject = new ServerAddress("[2010:836B:4179::836B:4179]", 1000);

        assertEquals("2010:836B:4179::836B:4179", subject.getHost());
        assertEquals(1000, subject.getPort());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseIPV6AddressMissingClosingBracket() throws UnknownHostException {
        new ServerAddress("[2010:836B:4179::836B:4179");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseIPV6WithPortWhenEquivalentPortIsAlsoSpecified() throws UnknownHostException {
        new ServerAddress("[2010:836B:4179::836B:4179]:80", 80);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseIPV6WithPortWhenNonEquivalentPortIsAlsoSpecified() throws UnknownHostException {
        new ServerAddress("[2010:836B:4179::836B:4179]:80", 1000);
    }

    @Test
    public void testDnsNameWithPort() throws UnknownHostException {
        ServerAddress subject = new ServerAddress("somewhere", 1000);

        assertEquals("somewhere", subject.getHost());
        assertEquals(1000, subject.getPort());
    }

    @Test
    public void testParseWithPortWhenDefaultPortIsAlsoSpecified() throws UnknownHostException {
        ServerAddress subject = new ServerAddress("somewhere:80", 27017);

        assertEquals("somewhere", subject.getHost());
        assertEquals(80, subject.getPort());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseWithPortWhenEquivalentPortIsAlsoSpecified() throws UnknownHostException {
        new ServerAddress("somewhere:80", 80);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParseWithPortWhenNonEquivalentPortIsAlsoSpecified() throws UnknownHostException {
        new ServerAddress("somewhere:80", 1000);
    }
}