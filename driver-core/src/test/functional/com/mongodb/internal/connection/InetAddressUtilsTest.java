/*
 * Copyright 2008-present MongoDB, Inc.
 * Copyright (C) 2008 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.mongodb.internal.connection;


import junit.framework.TestCase;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Tests for {@link InetAddressUtils}.
 */
public class InetAddressUtilsTest extends TestCase {
    public void testForStringBogusInput() {
        Set<String> bogusInputs =
                toSet(
                        "",
                        "016.016.016.016",
                        "016.016.016",
                        "016.016",
                        "016",
                        "000.000.000.000",
                        "000",
                        "0x0a.0x0a.0x0a.0x0a",
                        "0x0a.0x0a.0x0a",
                        "0x0a.0x0a",
                        "0x0a",
                        "42.42.42.42.42",
                        "42.42.42",
                        "42.42",
                        "42",
                        "42..42.42",
                        "42..42.42.42",
                        "42.42.42.42.",
                        "42.42.42.42...",
                        ".42.42.42.42",
                        ".42.42.42",
                        "...42.42.42.42",
                        "42.42.42.-0",
                        "42.42.42.+0",
                        ".",
                        "...",
                        "bogus",
                        "bogus.com",
                        "192.168.0.1.com",
                        "12345.67899.-54321.-98765",
                        "257.0.0.0",
                        "42.42.42.-42",
                        "42.42.42.ab",
                        "3ffe::1.net",
                        "3ffe::1::1",
                        "1::2::3::4:5",
                        "::7:6:5:4:3:2:", // should end with ":0"
                        ":6:5:4:3:2:1::", // should begin with "0:"
                        "2001::db:::1",
                        "FEDC:9878",
                        "+1.+2.+3.4",
                        "1.2.3.4e0",
                        "6:5:4:3:2:1:0", // too few parts
                        "::7:6:5:4:3:2:1:0", // too many parts
                        "7:6:5:4:3:2:1:0::", // too many parts
                        "9:8:7:6:5:4:3::2:1", // too many parts
                        "0:1:2:3::4:5:6:7", // :: must remove at least one 0.
                        "3ffe:0:0:0:0:0:0:0:1", // too many parts (9 instead of 8)
                        "3ffe::10000", // hextet exceeds 16 bits
                        "3ffe::goog",
                        "3ffe::-0",
                        "3ffe::+0",
                        "3ffe::-1",
                        ":",
                        ":::",
                        "::1.2.3",
                        "::1.2.3.4.5",
                        "::1.2.3.4:",
                        "1.2.3.4::",
                        "2001:db8::1:",
                        ":2001:db8::1",
                        ":1:2:3:4:5:6:7",
                        "1:2:3:4:5:6:7:",
                        ":1:2:3:4:5:6:");

        for (String bogusInput : bogusInputs) {
            try {
                InetAddressUtils.forString(bogusInput);
                fail("IllegalArgumentException expected for '" + bogusInput + "'");
            } catch (IllegalArgumentException expected) {
            }
            assertFalse(InetAddressUtils.isInetAddress(bogusInput));
        }
    }

    public void test3ff31() {
        try {
            InetAddressUtils.forString("3ffe:::1");
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException expected) {
        }
        assertFalse(InetAddressUtils.isInetAddress("016.016.016.016"));
    }

    public void testForStringIPv4Input() throws UnknownHostException {
        String ipStr = "192.168.0.1";
        // Shouldn't hit DNS, because it's an IP string literal.
        InetAddress ipv4Addr = InetAddress.getByName(ipStr);
        assertEquals(ipv4Addr, InetAddressUtils.forString(ipStr));
        assertTrue(InetAddressUtils.isInetAddress(ipStr));
    }

    public void testForStringIPv4NonAsciiInput() throws UnknownHostException {
        String ipStr = "૧૯૨.૧૬૮.૦.૧"; // 192.168.0.1 in Gujarati digits
        // Shouldn't hit DNS, because it's an IP string literal.
        InetAddress ipv4Addr;
        try {
            ipv4Addr = InetAddress.getByName(ipStr);
        } catch (UnknownHostException e) {
            // OK: this is probably Android, which is stricter.
            return;
        }
        assertEquals(ipv4Addr, InetAddressUtils.forString(ipStr));
        assertTrue(InetAddressUtils.isInetAddress(ipStr));
    }

    public void testForStringIPv6Input() throws UnknownHostException {
        String ipStr = "3ffe::1";
        // Shouldn't hit DNS, because it's an IP string literal.
        InetAddress ipv6Addr = InetAddress.getByName(ipStr);
        assertEquals(ipv6Addr, InetAddressUtils.forString(ipStr));
        assertTrue(InetAddressUtils.isInetAddress(ipStr));
    }

    public void testForStringIPv6NonAsciiInput() throws UnknownHostException {
        String ipStr = "૩ffe::૧"; // 3ffe::1 with Gujarati digits for 3 and 1
        // Shouldn't hit DNS, because it's an IP string literal.
        InetAddress ipv6Addr;
        try {
            ipv6Addr = InetAddress.getByName(ipStr);
        } catch (UnknownHostException e) {
            // OK: this is probably Android, which is stricter.
            return;
        }
        assertEquals(ipv6Addr, InetAddressUtils.forString(ipStr));
        assertTrue(InetAddressUtils.isInetAddress(ipStr));
    }

    public void testForStringIPv6EightColons() throws UnknownHostException {
        Set<String> eightColons =
                toSet("::7:6:5:4:3:2:1", "::7:6:5:4:3:2:0", "7:6:5:4:3:2:1::", "0:6:5:4:3:2:1::");

        for (String ipString : eightColons) {
            // Shouldn't hit DNS, because it's an IP string literal.
            InetAddress ipv6Addr = InetAddress.getByName(ipString);
            assertEquals(ipv6Addr, InetAddressUtils.forString(ipString));
            assertTrue(InetAddressUtils.isInetAddress(ipString));
        }
    }

    public void testConvertDottedQuadToHex() throws UnknownHostException {
        Set<String> ipStrings =
                toSet("7::0.128.0.127", "7::0.128.0.128", "7::128.128.0.127", "7::0.128.128.127");

        for (String ipString : ipStrings) {
            // Shouldn't hit DNS, because it's an IP string literal.
            InetAddress ipv6Addr = InetAddress.getByName(ipString);
            assertEquals(ipv6Addr, InetAddressUtils.forString(ipString));
            assertTrue(InetAddressUtils.isInetAddress(ipString));
        }
    }

    // see https://github.com/google/guava/issues/2587
    private static final Set<String> SCOPE_IDS =
            toSet("eno1", "en1", "eth0", "X", "1", "2", "14", "20");

    public void testIPv4AddressWithScopeId() {
        Set<String> ipStrings = toSet("1.2.3.4", "192.168.0.1");
        for (String ipString : ipStrings) {
            for (String scopeId : SCOPE_IDS) {
                String withScopeId = ipString + "%" + scopeId;
                assertFalse(
                        "InetAddresses.isInetAddress(" + withScopeId + ") should be false but was true",
                        InetAddressUtils.isInetAddress(withScopeId));
            }
        }
    }

    private static Set<String> toSet(final String... strings) {
        return new HashSet<>(Arrays.asList(strings));
    }

    public void testDottedQuadAddressWithScopeId() {
        Set<String> ipStrings =
                toSet("7::0.128.0.127", "7::0.128.0.128", "7::128.128.0.127", "7::0.128.128.127");
        for (String ipString : ipStrings) {
            for (String scopeId : SCOPE_IDS) {
                String withScopeId = ipString + "%" + scopeId;
                assertFalse(
                        "InetAddresses.isInetAddress(" + withScopeId + ") should be false but was true",
                        InetAddressUtils.isInetAddress(withScopeId));
            }
        }
    }

    public void testIPv6AddressWithScopeId() {
        Set<String> ipStrings =
                toSet(
                        "0:0:0:0:0:0:0:1",
                        "fe80::a",
                        "fe80::1",
                        "fe80::2",
                        "fe80::42",
                        "fe80::3dd0:7f8e:57b7:34d5",
                        "fe80::71a3:2b00:ddd3:753f",
                        "fe80::8b2:d61e:e5c:b333",
                        "fe80::b059:65f4:e877:c40");
        for (String ipString : ipStrings) {
            for (String scopeId : SCOPE_IDS) {
                String withScopeId = ipString + "%" + scopeId;
                assertTrue(
                        "InetAddresses.isInetAddress(" + withScopeId + ") should be true but was false",
                        InetAddressUtils.isInetAddress(withScopeId));
                assertEquals(InetAddressUtils.forString(withScopeId), InetAddressUtils.forString(ipString));
            }
        }
    }
}
