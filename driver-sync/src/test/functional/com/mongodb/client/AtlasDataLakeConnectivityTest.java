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

package com.mongodb.client;

import com.mongodb.MongoClientSettings;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.Before;
import org.junit.Test;

import static com.mongodb.ClusterFixture.getConnectionString;
import static com.mongodb.ClusterFixture.isDataLakeTest;
import static com.mongodb.MongoCredential.createScramSha1Credential;
import static com.mongodb.MongoCredential.createScramSha256Credential;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static java.util.Objects.requireNonNull;
import static org.junit.Assume.assumeTrue;

public class AtlasDataLakeConnectivityTest {

    @Before
    public void setUp() {
        assumeTrue(isDataLakeTest());
    }

    @Test
    public void testUnauthenticated() {
        testWithSettings(createSettingsBuilder()
                .build());
    }

    @Test
    public void testScramSha1() {
        testWithSettings(createSettingsBuilder().credential(createScramSha1Credential(getUserName(), getSource(), getPassword()))
                .build());
    }

    @Test
    public void testScramSha256() {
        testWithSettings(createSettingsBuilder().credential(createScramSha256Credential(getUserName(), getSource(), getPassword()))
                .build());
    }

    private void testWithSettings(final MongoClientSettings mongoClientSettings) {
        try (MongoClient mongoClient = MongoClients.create(mongoClientSettings)) {
            mongoClient.getDatabase("admin").runCommand(new BsonDocument("ping", new BsonInt32(1)));
        }
    }

    private MongoClientSettings.Builder createSettingsBuilder() {
        return MongoClientSettings.builder()
                .applyToClusterSettings(builder -> builder.hosts(getMongoClientSettings().getClusterSettings().getHosts()));
    }

    private char[] getPassword() {
        return requireNonNull(requireNonNull(getConnectionString().getCredential()).getPassword());
    }

    private String getSource() {
        return requireNonNull(getConnectionString().getCredential()).getSource();
    }

    private String getUserName() {
        return requireNonNull(requireNonNull(getConnectionString().getCredential()).getUserName());
    }
}
