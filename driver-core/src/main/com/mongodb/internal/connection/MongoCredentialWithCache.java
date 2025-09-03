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
import com.mongodb.lang.Nullable;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;

import static com.mongodb.internal.Locks.withInterruptibleLock;
import static com.mongodb.internal.connection.OidcAuthenticator.OidcCacheEntry;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class MongoCredentialWithCache {
    private final MongoCredential credential;
    private final Cache cache;

    public MongoCredentialWithCache(final MongoCredential credential) {
        this(credential, new Cache());
    }

    private MongoCredentialWithCache(final MongoCredential credential, final Cache cache) {
        this.credential = credential;
        this.cache = cache;
    }

    public MongoCredentialWithCache withMechanism(final AuthenticationMechanism mechanism) {
        return new MongoCredentialWithCache(credential.withMechanism(mechanism), cache);
    }

    @Nullable
    public AuthenticationMechanism getAuthenticationMechanism() {
        return credential.getAuthenticationMechanism();
    }

    public MongoCredential getCredential() {
        return credential;
    }

    @Nullable
    public <T> T getFromCache(final Object key, final Class<T> clazz) {
        return clazz.cast(cache.get(key));
    }

    public void putInCache(final Object key, final Object value) {
        cache.set(key, value);
    }

    OidcCacheEntry getOidcCacheEntry() {
        return cache.oidcCacheEntry;
    }

    void setOidcCacheEntry(final OidcCacheEntry oidcCacheEntry) {
        this.cache.oidcCacheEntry = oidcCacheEntry;
    }

    StampedLock getOidcLock() {
        return cache.oidcLock;
    }

    public Lock getLock() {
        return cache.lock;
    }

    /**
     * Stores any state associated with the credential.
     */
    static class Cache {
        private final ReentrantLock lock = new ReentrantLock();
        private Object cacheKey;
        private Object cacheValue;


        private final StampedLock oidcLock = new StampedLock();
        private volatile OidcCacheEntry oidcCacheEntry = new OidcCacheEntry();

        Object get(final Object key) {
            return withInterruptibleLock(lock, () -> {
                if (cacheKey != null && cacheKey.equals(key)) {
                    return cacheValue;
                }
                return null;
            });
        }

        void set(final Object key, final Object value) {
            withInterruptibleLock(lock, () -> {
                cacheKey = key;
                cacheValue = value;
            });
        }
    }
}
