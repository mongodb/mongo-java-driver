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
import com.mongodb.crypt.capi.CAPI.mongocrypt_crypto_fn;
import com.mongodb.crypt.capi.CAPI.mongocrypt_status_t;
import com.sun.jna.Pointer;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ConcurrentLinkedDeque;

import static com.mongodb.crypt.capi.CAPI.MONGOCRYPT_STATUS_ERROR_CLIENT;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_status_set;
import static com.mongodb.crypt.capi.CAPIHelper.toByteArray;
import static com.mongodb.crypt.capi.CAPIHelper.writeByteArrayToBinary;

class CipherCallback implements mongocrypt_crypto_fn {
    private final String algorithm;
    private final String transformation;
    private final int mode;
    private final CipherPool cipherPool;

    CipherCallback(final String algorithm, final String transformation, final int mode) {
        this.algorithm = algorithm;
        this.transformation = transformation;
        this.mode = mode;
        this.cipherPool = new CipherPool();
    }

    @Override
    public boolean crypt(final Pointer ctx, final mongocrypt_binary_t key, final mongocrypt_binary_t iv,
                         final mongocrypt_binary_t in, final mongocrypt_binary_t out,
                         final Pointer bytesWritten, final mongocrypt_status_t status) {
        Cipher cipher = null;
        try {
            IvParameterSpec ivParameterSpec = new IvParameterSpec(toByteArray(iv));
            SecretKeySpec secretKeySpec = new SecretKeySpec(toByteArray(key), algorithm);
            cipher = cipherPool.get();
            cipher.init(mode, secretKeySpec, ivParameterSpec);

            byte[] result = cipher.doFinal(toByteArray(in));
            writeByteArrayToBinary(out, result);
            bytesWritten.setInt(0, result.length);

            return true;
        } catch (Exception e) {
            mongocrypt_status_set(status, MONGOCRYPT_STATUS_ERROR_CLIENT, 0, new cstring(e.toString()), -1);
            return false;
        } finally {
            if (cipher != null) {
                cipherPool.release(cipher);
            }
        }
    }

    private class CipherPool {
        private final ConcurrentLinkedDeque<Cipher> available = new ConcurrentLinkedDeque<>();

        Cipher get() throws NoSuchAlgorithmException, NoSuchPaddingException {
            Cipher cipher = available.pollLast();
            if (cipher != null) {
                return cipher;
            }
            return Cipher.getInstance(transformation);
        }

        void release(final Cipher cipher) {
            available.addLast(cipher);
        }
    }
}
