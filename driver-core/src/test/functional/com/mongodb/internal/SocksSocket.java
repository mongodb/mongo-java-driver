/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.internal;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import static com.mongodb.internal.connection.SocksSocket.identifyAddressType;

class SocksSocket {
    @ParameterizedTest
    @ValueSource(strings = {
            "2001:db8:85a3::8a2e:370:7334",
            "::5000",
            "5000::",
            "1:2:3:4:5:6:7:8",
            "0:0:0:0:0:0:0:2",
            "1:2:3:4:5:6::7",
            "::1:2:3:4:5:6:7",
            "1:2:3:4:5:6:7::",
            "::2",
            "0:000::0:2",
            "2001:db8:85a3::8a2e:370:7334",
            "1::",
            "0::1",
            "::0:0000:0",
            "::",
            "::1",
            "0:0:0:0:0:0:0:0",
            "0:0:0:0:0:0:0:1",
    })
    void shouldReturnIpv6Address(final String ipAddress) throws SocketException, UnknownHostException {
        InetSocketAddress remoteAddress = InetSocketAddress.createUnresolved(ipAddress, 0);
        Assertions.assertTrue(identifyAddressType(remoteAddress) instanceof Inet6Address);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "3letter.xyz",
            "my_domain.com",
            "hyphenated-name.net",
            "numbers123.org",
            "_underscored.site",
            "xn--80ak6aa92e.com (IDN)",
            "localhost",
            "www.ab--cd.com",
            ".invalid",
            "example.invalid",
            "test.-site.com",
            "subdomain..domain.com",
            "256charactersinthisdomainnamethatexceedsthemaximumallowedlengthfortld.com",
            ".xn--32-6kcakeb6cn8ak4b1d4dkswnqn.xn--pss-3p4d1dm5a.xn--jlq61u9w3b (Punycode)",
            "--doublehyphens.org",
            "subdomain.toolongtldddddddddddddd",
            "spaced out.site",
            "no_spaces.domain.com",
            "my hostname.com",
            "localhost:8080",
            "www.example.com.",
            "-startingwithhyphen.net",
            "this-domain-is-really-long-because-it-just-keeps-going-and-going-and-its-still-not-done-yet-because-theres-more.net",
            "sub--sub.domain.com",
            "mydomain.",
            "www..com",
            "subdomain.no_underscores_or_dots_allowed,",
    })
    void shouldReturnNullWhenHostnameIsProvided(final String ipAddress) throws SocketException, UnknownHostException {
        InetSocketAddress remoteAddress = InetSocketAddress.createUnresolved(ipAddress, 0);
        Assertions.assertNull(identifyAddressType(remoteAddress));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "0:1:2:3:4:5:6::7",
            "0::1:2:3:4:5:6:7",
            "0:1:2::3:4:5:6:7",
            "::1:2:3:4:5:6:7:8",
            "1:2:3:4:5:6:7:8::",
            "0:1:2:3:4:5:6:7:8:9",
            "::5000::",
            "5::3::4",
            "::5::4::",
            "::5::4",
            "4::5::",
            "1::2:3::4",
            "1:2",
            "2:::5",
            "1::2::5",
            ":4:",
            ":7",
            "7:",
            "1",
            ":5:2",
            "5:2:",
            "1:2:3:4:5:6:7",
            ":::::",
            ":::",
            "::n::",
            "2001:db8:85a3::8a2e:370:7334:",
            ":2001:db8:85a3::8a2e:370:7334",
            "20012:db8:85a3::8a2e:370:7334",
            "20012:20012:20012:20012:20012:20012:20012:20012"
    })
    void shouldReturnNullIfInvalidIpv6Address(final String ipAddress) throws SocketException, UnknownHostException {
        InetSocketAddress remoteAddress = InetSocketAddress.createUnresolved(ipAddress, 0);
        Assertions.assertNull(identifyAddressType(remoteAddress));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "192.168.0.1",
            "10.0.0.1",
            "172.16.0.1",
            "255.255.255.255",
            "127.0.0.1",
            "169.254.1.2",
            "110.010.20.030", // octal representation
            "008.8.8.8", // octal representation
            "1.2.3.4",
            "007.008.009.010" // octal representation
    })
    void shouldReturnInet4Address(final String ipAddress) throws SocketException, UnknownHostException {
        InetSocketAddress remoteAddress = InetSocketAddress.createUnresolved(ipAddress, 0);
        Assertions.assertTrue(identifyAddressType(remoteAddress) instanceof Inet4Address);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "256.0.0.1",
            "192.168.256.1",
            "192.168.0.",
            "300.300.300.300",
            "192.168.0.0.1",
            "localhost",
            "::::",
    })
    void shouldReturnNullIfInet4AddressIsInvalid(final String ipAddress) throws SocketException, UnknownHostException {
        InetSocketAddress remoteAddress = InetSocketAddress.createUnresolved(ipAddress, 0);
        Assertions.assertNull(identifyAddressType(remoteAddress));
    }

}
