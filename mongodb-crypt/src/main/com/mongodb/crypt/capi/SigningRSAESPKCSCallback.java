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

import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;

import static com.mongodb.crypt.capi.CAPI.MONGOCRYPT_STATUS_ERROR_CLIENT;
import static com.mongodb.crypt.capi.CAPI.mongocrypt_status_set;
import static com.mongodb.crypt.capi.CAPIHelper.toByteArray;
import static com.mongodb.crypt.capi.CAPIHelper.writeByteArrayToBinary;

class SigningRSAESPKCSCallback implements mongocrypt_hmac_fn {

    private static final String KEY_ALGORITHM = "RSA";
    private static final String SIGN_ALGORITHM = "SHA256withRSA";

    SigningRSAESPKCSCallback() {
    }

    @Override
    public boolean hmac(final Pointer ctx, final mongocrypt_binary_t key, final mongocrypt_binary_t in,
                        final mongocrypt_binary_t out, final mongocrypt_status_t status) {
        try {
            byte[] result = getSignature(toByteArray(key), toByteArray(in));
            writeByteArrayToBinary(out, result);
            return true;
        } catch (Exception e) {
            mongocrypt_status_set(status, MONGOCRYPT_STATUS_ERROR_CLIENT, 0, new cstring(e.toString()), -1);
            return false;
        }
    }

    static byte[] getSignature(final byte[] privateKeyBytes, final byte[] dataToSign) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SignatureException {
        KeySpec keySpec = new PKCS8EncodedKeySpec(privateKeyBytes);
        KeyFactory keyFactory = KeyFactory.getInstance(KEY_ALGORITHM);
        PrivateKey privateKey = keyFactory.generatePrivate(keySpec);

        Signature privateSignature = Signature.getInstance(SIGN_ALGORITHM);
        privateSignature.initSign(privateKey);
        privateSignature.update(dataToSign);

        return privateSignature.sign();
    }
}
