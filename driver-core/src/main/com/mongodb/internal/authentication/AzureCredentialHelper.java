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
import org.bson.BsonDocument;
import org.bson.json.JsonParseException;

import java.util.HashMap;
import java.util.Map;

import static com.mongodb.internal.authentication.HttpHelper.getHttpContents;

/**
 * Utility class for working with Azure authentication.
 *
 * <p>This class should not be considered a part of the public API.</p>
 */
public final class AzureCredentialHelper {
    public static BsonDocument obtainFromEnvironment() {
        String endpoint = "http://" + "169.254.169.254:80"
                + "/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://vault.azure.net";

        Map<String, String> headers = new HashMap<>();
        headers.put("Metadata", "true");
        headers.put("Accept", "application/json");

        String response = getHttpContents("GET", endpoint, headers);
        try {
            BsonDocument responseDocument = BsonDocument.parse(response);
            if (responseDocument.containsKey("access_token")) {
                return new BsonDocument("accessToken", responseDocument.get("access_token"));
            } else {
                throw new MongoClientException("The access_token is missing from Azure IMDS metadata response.");
            }
        } catch (JsonParseException e) {
            throw new MongoClientException("Exception parsing JSON from Azure IMDS metadata response.", e);
        }
    }

    private AzureCredentialHelper() {
    }
}
