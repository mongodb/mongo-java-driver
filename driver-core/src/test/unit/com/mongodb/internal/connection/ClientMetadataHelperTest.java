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


import com.mongodb.MongoDriverInformation;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import static com.mongodb.internal.connection.ClientMetadataHelper.createClientMetadataDocument;
import static com.mongodb.internal.connection.ClientMetadataHelperSpecification.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ClientMetadataHelperTest {
    private static final String APP_NAME = "app name";

    public String repeat(final int times, final String s) {
        StringBuilder builder = new StringBuilder(times);
        for (int i = 0; i < times; i++) {
            builder.append(s);
        }
        return builder.toString();
    }

    @Test
    public void testLimitForDriverVersion() {
        // should create client metadata document and exclude the extra driver info if its too verbose
        MongoDriverInformation driverInfo = MongoDriverInformation.builder()
                .driverName("mongo-spark")
                .driverVersion(repeat(512, "a"))
                .driverPlatform("Scala 2.10 / Spark 2.0.0")
                .build();

        BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME, driverInfo);
        BsonDocument expectedBase = createExpectedClientMetadataDocument(APP_NAME);
        expected.put("driver", expectedBase.get("driver"));

        BsonDocument actual = createClientMetadataDocument(APP_NAME, driverInfo);
        assertEquals(expected, actual);
    }

    @Test
    public void testLimitForOsName() {
        String name = "os.name";
        String original = System.getProperty(name);
        System.setProperty(name, repeat(512, "a"));

        BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME);
        expected.getDocument("os").remove("name");

        BsonDocument actual = createClientMetadataDocument(APP_NAME);
        assertEquals(expected, actual);

        System.setProperty(name, original);
    }

    @Test
    public void testLimitForPlatform() {
        MongoDriverInformation driverInfo = MongoDriverInformation.builder()
                .driverName("mongo-spark")
                .driverVersion("2.0.0")
                .driverPlatform(repeat(512, "a"))
                .build();

        BsonDocument expected = createExpectedClientMetadataDocument(APP_NAME, driverInfo);
        BsonDocument expectedBase = createExpectedClientMetadataDocument(APP_NAME);
        expected.put("platform", expectedBase.get("platform"));

        BsonDocument actual = createClientMetadataDocument(APP_NAME, driverInfo);
        assertEquals(expected, actual);
    }
}
