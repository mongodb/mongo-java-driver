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

package com.mongodb.connection;

import com.mongodb.MongoCredential;
import com.mongodb.MongoInternalException;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;

abstract class Authenticator {
    private final MongoCredential credential;

    Authenticator(@NonNull final MongoCredential credential) {
        this.credential = credential;
    }

    @NonNull
    MongoCredential getCredential() {
        return credential;
    }

    @NonNull
    String getUserNameNonNull() {
        String userName = credential.getUserName();
        if (userName == null) {
            throw new MongoInternalException("User name can not be null");
        }
        return userName;
    }

    @NonNull
    char[] getPasswordNonNull() {
        char[] password = credential.getPassword();
        if (password == null) {
            throw new MongoInternalException("Password can not be null");
        }
        return password;
    }

    @NonNull
    public <T> T getNonNullMechanismProperty(final String key, @Nullable final T defaultValue) {
        T mechanismProperty = credential.getMechanismProperty(key, defaultValue);
        if (mechanismProperty == null) {
            throw new MongoInternalException("Mechanism property can not be null");
        }
        return mechanismProperty;

    }

    abstract void authenticate(InternalConnection connection, ConnectionDescription connectionDescription);

    abstract void authenticateAsync(InternalConnection connection, ConnectionDescription connectionDescription,
                                    SingleResultCallback<Void> callback);
}
