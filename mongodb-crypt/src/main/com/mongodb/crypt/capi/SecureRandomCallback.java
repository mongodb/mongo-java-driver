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
 *
 */

package com.mongodb.crypt.capi;

import com.mongodb.crypt.capi.CAPI.cstring;
import com.mongodb.crypt.capi.CAPI.mongocrypt_binary_t;
import com.mongodb.crypt.capi.CAPI.mongocrypt_random_fn;
import com.mongodb.crypt.capi.CAPI.mongocrypt_status_t;
import com.sun.jna.Pointer;

import java.security.SecureRandom;

import static com.mongodb.crypt.capi.CAPI.MONGOCRYPT_STATUS_ERROR_CLIENT;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_status_set;
import static com.mongodb.crypt.capi.CAPIHelper.writeByteArrayToBinary;

class SecureRandomCallback implements mongocrypt_random_fn {
    private final SecureRandom secureRandom;

    SecureRandomCallback(final SecureRandom secureRandom) {
        this.secureRandom = secureRandom;
    }

    @Override
    public boolean random(final Pointer ctx, final mongocrypt_binary_t out, final int count, final mongocrypt_status_t status) {
        try {
            byte[] randomBytes = new byte[count];
            secureRandom.nextBytes(randomBytes);
            writeByteArrayToBinary(out, randomBytes);
            return true;
        } catch (Exception e) {
            mongocrypt_status_set(status, MONGOCRYPT_STATUS_ERROR_CLIENT, 0, new cstring(e.toString()), -1);
            return false;
        }
    }
}
