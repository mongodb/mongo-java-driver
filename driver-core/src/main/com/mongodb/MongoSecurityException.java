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

package com.mongodb;

/**
 * This exception is thrown when there is an error reported by the underlying client authentication mechanism.
 *
 * @since 3.0
 */
public class MongoSecurityException extends MongoClientException {
    private static final long serialVersionUID = -7044790409935567275L;

    private final MongoCredential credential;

    /**
     * Construct an instance
     *
     * @param credential the credential
     * @param message the message
     * @param cause   the cause
     */
    public MongoSecurityException(final MongoCredential credential, final String message, final Throwable cause) {
        super(message, cause);
        this.credential = credential;
    }

    /**
     * Construct an instance
     *
     * @param credential the credential
     * @param message the message
     */
    public MongoSecurityException(final MongoCredential credential, final String message) {
        super(message);
        this.credential = credential;
    }

    /**
     * The credential being authenticated.
     *
     * @return the credential
     */
    public MongoCredential getCredential() {
        return credential;
    }
}
