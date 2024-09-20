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
import com.mongodb.crypt.capi.CAPI.mongocrypt_hmac_fn;
import com.mongodb.crypt.capi.CAPI.mongocrypt_status_t;
import com.sun.jna.Pointer;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import static com.mongodb.crypt.capi.CAPI.MONGOCRYPT_STATUS_ERROR_CLIENT;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_status_set;
import static com.mongodb.crypt.capi.CAPIHelper.toByteArray;
import static com.mongodb.crypt.capi.CAPIHelper.writeByteArrayToBinary;

class MacCallback implements mongocrypt_hmac_fn {
    private final String algorithm;

    MacCallback(final String algorithm) {
        this.algorithm = algorithm;
    }

    @Override
    public boolean hmac(final Pointer ctx, final mongocrypt_binary_t key, final mongocrypt_binary_t in,
                        final mongocrypt_binary_t out, final mongocrypt_status_t status) {
        try {
            Mac mac = Mac.getInstance(algorithm);
            SecretKeySpec keySpec = new SecretKeySpec(toByteArray(key), algorithm);
            mac.init(keySpec);

            mac.update(toByteArray(in));

            byte[] result = mac.doFinal();
            writeByteArrayToBinary(out, result);

            return true;
        } catch (Exception e) {
            mongocrypt_status_set(status, MONGOCRYPT_STATUS_ERROR_CLIENT, 0, new cstring(e.toString()), -1);
            return false;
        }
    }
}
