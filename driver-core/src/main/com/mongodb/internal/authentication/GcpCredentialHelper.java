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

import java.util.HashMap;
import java.util.Map;

import static com.mongodb.internal.authentication.HttpHelper.getHttpContents;

/**
 * Utility class for working with GCP authentication.
 *
 * <p>This class should not be considered a part of the public API.</p>
 */
public final class GcpCredentialHelper {
    public static BsonDocument obtainFromEnvironment() {
        String endpoint = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";

        Map<String, String> header = new HashMap<>();
        header.put("Metadata-Flavor", "Google");
        String response = getHttpContents("GET", endpoint, header);
        BsonDocument responseDocument = BsonDocument.parse(response);
        if (responseDocument.containsKey("access_token")) {
            return new BsonDocument("accessToken", responseDocument.get("access_token"));
        } else {
            throw new MongoClientException("access_token is missing from GCE metadata response.  Full response is ''" + response);
        }
    }

    private GcpCredentialHelper() {
    }
}
