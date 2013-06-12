/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.connection.impl;

import org.mongodb.Document;
import org.mongodb.MongoInternalException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class NativeAuthenticationHelper {

    private static final Charset UTF_8_CHARSET = Charset.forName("UTF-8");

    public static Document getAuthCommand(final String userName, final char[] password, final String nonce) {
        return getAuthCommand(userName, createAuthenticationHash(userName, password), nonce);
    }

    static Document getAuthCommand(final String userName, final String authHash, final String nonce) {
        String key = nonce + userName + authHash;

        Document cmd = new Document();

        cmd.put("authenticate", 1);
        cmd.put("user", userName);
        cmd.put("nonce", nonce);
        cmd.put("key", hexMD5(key.getBytes(UTF_8_CHARSET)));

        return cmd;
    }

    public static Document getNonceCommand() {
        return new Document("getnonce", 1);
    }

    public static String createAuthenticationHash(final String userName, final char[] password) {
        ByteArrayOutputStream bout = new ByteArrayOutputStream(userName.length() + 20 + password.length);
        try {
            bout.write(userName.getBytes(UTF_8_CHARSET));
            bout.write(":mongo:".getBytes(UTF_8_CHARSET));
            for (final char ch : password) {
                if (ch >= 128) {
                    throw new IllegalArgumentException("can't handle non-ascii passwords yet");
                }
                bout.write((byte) ch);
            }
        } catch (IOException ioe) {
            throw new RuntimeException("impossible", ioe);
        }
        return hexMD5(bout.toByteArray());
    }

    /**
     * Produce hex representation of the MD5 digest of a byte array
     *
     * @param data bytes to digest
     * @return hex string of the MD5 digest
     */
    public static String hexMD5(final byte[] data) {

        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");

            md5.reset();
            md5.update(data);
            byte[] digest = md5.digest();

            return toHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new MongoInternalException("Error - this implementation of Java doesn't support MD5.", e);
        }
    }

    public static String toHex(final byte[] bytes) {
        StringBuilder sb = new StringBuilder();

        for (final byte aByte : bytes) {
            String s = Integer.toHexString(0xff & aByte);

            if (s.length() < 2) {
                sb.append("0");
            }
            sb.append(s);
        }

        return sb.toString();
    }

    private NativeAuthenticationHelper() {
    }
}
