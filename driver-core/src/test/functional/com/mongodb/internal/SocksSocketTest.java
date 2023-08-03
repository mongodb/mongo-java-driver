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

import java.net.SocketException;

import static com.mongodb.internal.connection.SocksSocket.createByteArrayFromIpAddress;
import static com.mongodb.internal.connection.SocksSocket.isDomainName;

class SocksSocketTest {
    private static final byte LENGTH_OF_IPV4 = 4;
    private static final byte LENGTH_OF_IPV6 = 16;
    private static final String IP_PARSING_ERROR_SUFFIX = " is not an IP string literal";

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
    void shouldReturnIpv6Address(final String ipAddress) throws SocketException {
        Assertions.assertEquals(LENGTH_OF_IPV6, createByteArrayFromIpAddress(ipAddress).length);
    }


    @ParameterizedTest
    @ValueSource(strings = {
            "hyphen-domain.com",
            "sub.domain.com",
            "sub.domain.c.com.com",
            "123numbers.com",
            "mixed-123domain.net",
            "longdomainnameabcdefghijk.com",
            "xn--frosch-6ya.com",
            "xn--emoji-grinning-3s0b.org",
            "xn--bcher-kva.ch",
            "localhost",
            "abcdefghijklmnopqrstuvwxyz0123456789-abcdefghijklmnopqrstuvwxyz.com",
            "xn--weihnachten-uzb.org",
    })
    void shouldReturnTrueWithValidHostName(final String hostname) {
        Assertions.assertTrue(isDomainName(hostname));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "xn--tst-0qa.example",
            "xn--frosch-6ya.w23",
            "-special_chars_$$.net",
            "special_chars_$$.net",
            "special_chars_$$.123",
            "subdomain..domain.com",
            "_subdomain..domain.com",
            "subdomain..domain._com",
            "subdomain..domain.com_",
            "notlocalhost",
            "abcdefghijklmnopqrstuvwxyz0123456789-abcdefghijklmnopqrstuvwxyzl.com",
            "this-domain-is-really-long-because-it-just-keeps-going-and-going-and-its-still-not-done-yet-because-theres-more.net",
            "verylongsubdomainnamethatisreallylongandmaycausetroubleforparsing.example"
    })
    void shouldReturnFalseWithInvalidHostName(final String hostname) {
        Assertions.assertFalse(isDomainName(hostname));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "192.168.0.1",
            "10.0.0.1",
            "172.16.0.1",
            "255.255.255.255",
            "127.0.0.1",
            "169.254.1.2",
            "1.2.3.4"
    })
    void shouldReturnIpv4Address(final String ipAddress) throws SocketException {
        Assertions.assertEquals(LENGTH_OF_IPV4, createByteArrayFromIpAddress(ipAddress).length);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            //Invalid IPV4 addresses
            "256.0.0.1",
            "192.168.256.1",
            "192.168.0.",
            "300.300.300.300",
            "192.168.0.0.1",
            "110.010.20.030", // octal representation
            "008.8.8.8", // octal representation
            "007.008.009.010", // octal representation

            //Invalid IPV6 addresses
            "::::",
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
            "20012:20012:20012:20012:20012:20012:20012:20012",

            //Domain names
            "localhost",
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
    void shouldThrowErrorWhenInvalidIpAddressIsProvided(final String ipAddress) {
        SocketException socketException = Assertions.assertThrows(SocketException.class, () -> createByteArrayFromIpAddress(ipAddress));
        Assertions.assertEquals(ipAddress + IP_PARSING_ERROR_SUFFIX, socketException.getMessage());
    }

}
