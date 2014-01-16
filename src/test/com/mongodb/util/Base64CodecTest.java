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

package com.mongodb.util;


import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * @author Sjoerd Mulder
 */
public class Base64CodecTest {

    private static final byte[] allBytes = new byte[255];
    private static final byte[] abc = new byte[] { 97, 98, 99}  ;
    private static final byte[] abcd = new byte[] { 97, 98, 99, 100};
    private static final byte[] abcde = new byte[] { 97, 98, 99, 100, 101};

    static {
        for (byte b = -128; b != 127; b++) {
            allBytes[b + 128] = b;
        }
    }

    @Test
    public void testDecodeEncode() throws Exception {
        Base64Codec codec = new Base64Codec();
        assertArrayEquals(abc, codec.decode(codec.encode(abc)));
        assertArrayEquals(abcd, codec.decode(codec.encode(abcd)));
        assertArrayEquals(abcde, codec.decode(codec.encode(abcde)));
        assertArrayEquals(allBytes, codec.decode(codec.encode(allBytes)));
    }

}
