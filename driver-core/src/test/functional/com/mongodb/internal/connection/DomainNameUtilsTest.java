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

package com.mongodb.internal.connection;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static com.mongodb.internal.connection.DomainNameUtils.isDomainName;

class DomainNameUtilsTest {

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
            "sub.domain.com.sub.domain.com.sub.domain.com.sub.domain.com.sub.domain.com.sub.domain.com.sub.domain." +
                    "com.sub.domain.com.sub.domain.com.sub.domain.com.sub.domain.com.sub.domain.com.sub.domain.com.sub.domain.com.sub.domain." +
                    "com.domain.com.sub.domain.subb.com"  //255 characters
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
            "домен.com", //NON-ASCII
            "ẞẞ.com", //NON-ASCII
            "abcdefghijklmnopqrstuvwxyz0123456789-abcdefghijklmnopqrstuvwxyzl.com",
            "this-domain-is-really-long-because-it-just-keeps-going-and-going-and-its-still-not-done-yet-because-theres-more.net",
            "verylongsubdomainnamethatisreallylongandmaycausetroubleforparsing.example",
            "sub.domain.com.sub.domain.com.sub.domain.com.sub.domain.com.sub.domain.com.sub.domain.com.sub.domain." +
                    "com.sub.domain.com.sub.domain.com.sub.domain.com.sub.domain.com.sub.domain.com.sub.domain.com.sub.domain." +
                    "com.sub.domain.com.domain.com.sub.domain.subbb.com"   //256 characters
    })
    void shouldReturnFalseWithInvalidHostName(final String hostname) {
        Assertions.assertFalse(isDomainName(hostname));
    }
}
