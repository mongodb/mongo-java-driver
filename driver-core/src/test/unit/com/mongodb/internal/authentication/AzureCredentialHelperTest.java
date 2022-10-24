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
import org.bson.BsonString;
import org.bson.json.JsonParseException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;

import static com.mongodb.internal.authentication.AzureCredentialHelper.obtainFromEnvironment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AzureCredentialHelperTest {
    private static final String MOCK_HOST = "localhost";
    private static final int MOCK_PORT = 8080;
    private static final String EXPECTED_URI = "/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://vault.azure.net";
    private static final String EXPECTED_URL = "http://" + MOCK_HOST + ":" + MOCK_PORT + EXPECTED_URI;

    // Case 1
    @Test
    void testSuccess() {
        BsonDocument credential = obtainAzureCredential(new HashMap<>());
        assertEquals(new BsonDocument("accessToken", new BsonString("magic-cookie")), credential);
    }

    // Case 2
    @Test
    void testEmptyJson() {
        MongoClientException exception =
                assertThrows(MongoClientException.class, () -> obtainAzureCredential(createHeaders("empty-json")));
        assertEquals("The access_token is missing from Azure IMDS metadata response.", exception.getMessage());
        assertNull(exception.getCause());
    }

    // Case 3
    @Test
    void testBadJson() {
        MongoClientException exception =
                assertThrows(MongoClientException.class, () -> obtainAzureCredential(createHeaders("bad-json")));
        assertEquals("Exception parsing JSON from Azure IMDS metadata response.", exception.getMessage());
        assertEquals(JsonParseException.class, exception.getCause().getClass());
    }

    // Case 4
    @Test
    void test404() {
        MongoClientException exception =
                assertThrows(MongoClientException.class, () -> obtainAzureCredential(createHeaders("404")));
        assertEquals("Unexpected IOException from endpoint " + EXPECTED_URL + ".", exception.getMessage());
        assertEquals(IOException.class, exception.getCause().getClass());
    }

    // Case 5
    @Test
    void test500() {
        MongoClientException exception =
                assertThrows(MongoClientException.class, () -> obtainAzureCredential(createHeaders("500")));
        assertEquals("Unexpected IOException from endpoint " + EXPECTED_URL + ".", exception.getMessage());
        assertEquals(IOException.class, exception.getCause().getClass());
    }

    // Case 6
    @Disabled("TODO: The 10 second timeout on HttpURLConnection doesn't work")
    @Test
    void testSlow() {
        MongoClientException exception =
                assertThrows(MongoClientException.class, () -> obtainAzureCredential(createHeaders("slow")));
        assertEquals("Unexpected IOException from endpoint " + EXPECTED_URL + ".", exception.getMessage());
        assertEquals(IOException.class, exception.getCause().getClass());
    }

    private static BsonDocument obtainAzureCredential(final HashMap<String, String> headers) {
        return obtainFromEnvironment(MOCK_HOST, MOCK_PORT, headers);
    }

    private static HashMap<String, String> createHeaders(final String caseValue) {
        HashMap<String, String> headers = new HashMap<>();
        headers.put("X-MongoDB-HTTP-TestParams", "case=" + caseValue);
        return headers;
    }
}
