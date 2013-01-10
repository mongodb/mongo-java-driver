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
 *
 */

package com.mongodb;

import com.mongodb.util.Util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

class NativeAuthenticationHelper {

    static DBObject getAuthCommand(String userName, char[] password, String nonce) {
        return getAuthCommand(userName, createHash(userName, password), nonce);
    }

    static DBObject getAuthCommand(String userName, byte[] authHash, String nonce) {
        String key = nonce + userName + new String(authHash);

        BasicDBObject cmd = new BasicDBObject();

        cmd.put("authenticate", 1);
        cmd.put("user", userName);
        cmd.put("nonce", nonce);
        cmd.put("key", Util.hexMD5(key.getBytes()));

        return cmd;
    }

    static BasicDBObject getNonceCommand() {
        return new BasicDBObject("getnonce", 1);
    }

    static byte[] createHash(String userName, char[] password) {
        ByteArrayOutputStream bout = new ByteArrayOutputStream(userName.length() + 20 + password.length);
        try {
            bout.write(userName.getBytes());
            bout.write(":mongo:".getBytes());
            for (final char ch : password) {
                if (ch >= 128)
                    throw new IllegalArgumentException("can't handle non-ascii passwords yet");
                bout.write((byte) ch);
            }
        } catch (IOException ioe) {
            throw new RuntimeException("impossible", ioe);
        }
        return Util.hexMD5(bout.toByteArray()).getBytes();
    }

    private NativeAuthenticationHelper() {
    }
}
