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

package com.mongodb.internal.authentication;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static com.mongodb.internal.HexUtils.hexMD5;

/**
 * Utility class for working with MongoDB native authentication.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class NativeAuthenticationHelper {

    /**
     * Creates a hash of the given user name and password, which is the hex encoding of
     * {@code MD5( <userName> + ":mongo:" + <password> )}.
     *
     * @param userName the user name
     * @param password the password
     * @return the hash as a string
     * @mongodb.driver.manual ../meta-driver/latest/legacy/implement-authentication-in-driver/ Authentication
     */
    public static String createAuthenticationHash(final String userName, final char[] password) {
        ByteArrayOutputStream bout = new ByteArrayOutputStream(userName.length() + 20 + password.length);
        try {
            bout.write(userName.getBytes(StandardCharsets.UTF_8));
            bout.write(":mongo:".getBytes(StandardCharsets.UTF_8));
            bout.write(new String(password).getBytes(StandardCharsets.UTF_8));
        } catch (IOException ioe) {
            throw new RuntimeException("impossible", ioe);
        }
        return hexMD5(bout.toByteArray());
    }

    public static BsonDocument getAuthCommand(final String userName, final char[] password, final String nonce) {
        return getAuthCommand(userName, createAuthenticationHash(userName, password), nonce);
    }

    public static BsonDocument getAuthCommand(final String userName, final String authHash, final String nonce) {
        String key = nonce + userName + authHash;

        BsonDocument cmd = new BsonDocument();

        cmd.put("authenticate", new BsonInt32(1));
        cmd.put("user", new BsonString(userName));
        cmd.put("nonce", new BsonString(nonce));
        cmd.put("key", new BsonString(hexMD5(key.getBytes(StandardCharsets.UTF_8))));

        return cmd;
    }

    public static BsonDocument getNonceCommand() {
        return new BsonDocument("getnonce", new BsonInt32(1));
    }

    private NativeAuthenticationHelper() {
    }
}
