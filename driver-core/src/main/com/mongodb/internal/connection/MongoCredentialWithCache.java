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

import com.mongodb.AuthenticationMechanism;
import com.mongodb.MongoCredential;

public class MongoCredentialWithCache {
    private final MongoCredential credential;
    private final Cache cache;

    public MongoCredentialWithCache(final MongoCredential credential) {
        this(credential, null);
    }

    public MongoCredentialWithCache(final MongoCredential credential, final Cache cache) {
        this.credential = credential;
        this.cache = cache != null ? cache : new Cache();
    }

    public MongoCredentialWithCache withMechanism(final AuthenticationMechanism mechanism) {
        return new MongoCredentialWithCache(credential.withMechanism(mechanism), cache);
    }

    public AuthenticationMechanism getAuthenticationMechanism() {
        return credential.getAuthenticationMechanism();
    }

    public MongoCredential getCredential() {
        return credential;
    }

    @SuppressWarnings("unchecked")
    public <T> T getFromCache(final Object key, final Class<T> clazz) {
        return clazz.cast(cache.get(key));
    }

    public void putInCache(final Object key, final Object value) {
        cache.set(key, value);
    }


    static class Cache {
        private Object cacheKey;
        private Object cacheValue;

        synchronized Object get(final Object key) {
            if (cacheKey != null && cacheKey.equals(key)) {
                return cacheValue;
            }
            return null;
        }

        synchronized void set(final Object key, final Object value) {
            cacheKey = key;
            cacheValue = value;
        }
    }
}

