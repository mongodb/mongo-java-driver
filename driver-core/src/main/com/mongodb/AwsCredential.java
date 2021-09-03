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

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A representation of Amazon Web Services credentials for API authentication.
 *
 * @see MongoCredential#createAwsCredential(String, char[])
 * @see MongoCredential#AWS_CREDENTIAL_PROVIDER_KEY
 * @since 4.4
 */
public final class AwsCredential {
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String sessionToken;

    /**
     * Construct a new instance.
     *
     * @param accessKeyId the non-null access key ID that identifies the temporary security credentials.
     * @param secretAccessKey the non-null secret access key that can be used to sign requests
     * @param sessionToken the non-null session token
     */
    public AwsCredential(final String accessKeyId, final String secretAccessKey, final String sessionToken) {
        this.accessKeyId = notNull("accessKeyId", accessKeyId);
        this.secretAccessKey = notNull("secretAccessKey", secretAccessKey);
        this.sessionToken = notNull("sessionToken", sessionToken);
    }

    /**
     * Gets the access key ID that identifies the temporary security credentials.
     *
     * @return the accessKeyId, which may not be null
     */
    public String getAccessKeyId() {
        return accessKeyId;
    }

    /**
     * Gets the secret access key that can be used to sign requests.
     *
     * @return the secretAccessKey, which may not be null
     */
    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    /**
     * Gets the session token.
     *
     * @return the sessionToken, which may not be null
     */
    public String getSessionToken() {
        return sessionToken;
    }
}
