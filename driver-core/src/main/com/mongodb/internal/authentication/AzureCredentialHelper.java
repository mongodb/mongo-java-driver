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

package com.mongodb.internal.authentication;

import com.mongodb.MongoClientException;
import com.mongodb.internal.ExpirableValue;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.json.JsonParseException;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.mongodb.internal.Locks.lockInterruptibly;
import static com.mongodb.internal.authentication.HttpHelper.getHttpContents;

/**
 * Utility class for working with Azure authentication.
 *
 * <p>This class should not be considered a part of the public API.</p>
 */
public final class AzureCredentialHelper {
    private static final String ACCESS_TOKEN_FIELD = "access_token";
    private static final String EXPIRES_IN_FIELD = "expires_in";
    private static final Lock CACHED_ACCESS_TOKEN_LOCK = new ReentrantLock();
    private static volatile ExpirableValue<String> cachedAccessToken = ExpirableValue.expired();

    public static BsonDocument obtainFromEnvironment() {
        String accessToken;
        Optional<String> cachedValue = cachedAccessToken.getValue();
        if (cachedValue.isPresent()) {
            accessToken = cachedValue.get();
        } else {
            lockInterruptibly(CACHED_ACCESS_TOKEN_LOCK);
            try {
                cachedValue = cachedAccessToken.getValue();
                if (cachedValue.isPresent()) {
                    accessToken = cachedValue.get();
                } else {
                    long startNanoTime = System.nanoTime();
                    CredentialInfo response = fetchAzureCredentialInfo("https://vault.azure.net", null);
                    accessToken = response.getAccessToken();
                    Duration duration = response.getExpiresIn().minus(Duration.ofMinutes(1));
                    cachedAccessToken = ExpirableValue.expirable(accessToken, duration, startNanoTime);
                }
            } finally {
                CACHED_ACCESS_TOKEN_LOCK.unlock();
            }
       }
       return new BsonDocument("accessToken", new BsonString(accessToken));
    }

    public static CredentialInfo fetchAzureCredentialInfo(final String resource, @Nullable final String objectId) {
        String endpoint = "http://169.254.169.254:80"
                + "/metadata/identity/oauth2/token?api-version=2018-02-01"
                + "&resource=" + resource
                + (objectId == null ? "" : "&object_id=" + objectId);

        Map<String, String> headers = new HashMap<>();
        headers.put("Metadata", "true");
        headers.put("Accept", "application/json");

        BsonDocument responseDocument;
        try {
            responseDocument = BsonDocument.parse(getHttpContents("GET", endpoint, headers));
        } catch (JsonParseException e) {
            throw new MongoClientException("Exception parsing JSON from Azure IMDS metadata response.", e);
        }

        if (!responseDocument.isString(ACCESS_TOKEN_FIELD)) {
            throw new MongoClientException(String.format(
                    "The %s field from Azure IMDS metadata response is missing or is not a string", ACCESS_TOKEN_FIELD));
        }
        if (!responseDocument.isString(EXPIRES_IN_FIELD)) {
            throw new MongoClientException(String.format(
                    "The %s field from Azure IMDS metadata response is missing or is not a string", EXPIRES_IN_FIELD));
        }
        String accessToken = responseDocument.getString(ACCESS_TOKEN_FIELD).getValue();
        int expiresInSeconds = Integer.parseInt(responseDocument.getString(EXPIRES_IN_FIELD).getValue());
        return new CredentialInfo(accessToken, Duration.ofSeconds(expiresInSeconds));
    }

    private AzureCredentialHelper() {
    }
}
