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

package com.mongodb.embedded.client;


import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.ServerVersion;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;


/**
 * Helper class for the acceptance tests.
 */
public final class Fixture {
    private static final String CONNECTION_STRING_PROPERTY_NAME = "org.mongodb.test.uri";
    private static final String DEFAULT_CONNECTION_STRING = "mongodb://%2Ftmp%2Fembedded_mongo/?appName=testApp";
    private static final String EMBEDDED_PATH_PROPERTY_NAME = "org.mongodb.test.embedded.path";
    private static final String DEFAULT_DATABASE_NAME = "JavaDriverTest";

    private static MongoClient mongoClient;
    private static MongoClientSettings mongoClientSettings;
    private static MongoDatabase defaultDatabase;
    private static ServerVersion serverVersion;

    private Fixture() {
    }

    static synchronized MongoClient getMongoClient() {
        if (mongoClient == null) {
            MongoClients.init(MongoEmbeddedSettings.builder().build());
            mongoClient = MongoClients.create(getMongoClientSettings());
            Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        }
        return mongoClient;
    }

    static synchronized MongoDatabase getDefaultDatabase() {
        if (defaultDatabase == null) {
            defaultDatabase = getMongoClient().getDatabase(getDefaultDatabaseName());
        }
        return defaultDatabase;
    }

    static String getDefaultDatabaseName() {
        return DEFAULT_DATABASE_NAME;
    }

    static class ShutdownHook extends Thread {
        @Override
        public void run() {
            synchronized (Fixture.class) {
                if (mongoClient != null) {
                    if (defaultDatabase != null) {
                        defaultDatabase.drop();
                    }
                    mongoClient.close();
                    mongoClient = null;
                    MongoClients.close();
                }
            }
        }
    }

    static boolean serverVersionLessThan(final String versionString) {
        return getServerVersion().compareTo(new ServerVersion(getVersionList(versionString).subList(0, 3))) < 0;
    }

    static boolean serverVersionGreaterThan(final String versionString) {
        return getServerVersion().compareTo(new ServerVersion(getVersionList(versionString).subList(0, 3))) > 0;
    }

    private static ServerVersion getServerVersion() {
        if (serverVersion == null) {
            BsonDocument buildInfoResult = getMongoClient().getDatabase("admin")
                    .runCommand(new Document("buildInfo", 1), BsonDocument.class);
            List<BsonValue> versionArray = buildInfoResult.getArray("versionArray").subList(0, 3);

            serverVersion = new ServerVersion(asList(versionArray.get(0).asInt32().getValue(),
                    versionArray.get(1).asInt32().getValue(),
                    versionArray.get(2).asInt32().getValue()));
        }
        return serverVersion;
    }

    private static synchronized String getConnectionStringProperty() {
        String connectionString = System.getProperty(CONNECTION_STRING_PROPERTY_NAME);
        return connectionString == null || connectionString.isEmpty() ? DEFAULT_CONNECTION_STRING : connectionString;
    }

    static synchronized MongoClientSettings getMongoClientSettings() {
        if (mongoClientSettings == null) {
            MongoClientSettings.Builder builder = MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(getConnectionStringProperty()));
            builder.libraryPath(System.getProperty(EMBEDDED_PATH_PROPERTY_NAME));
            mongoClientSettings = builder.build();
        }
        return mongoClientSettings;
    }

    private static List<Integer> getVersionList(final String versionString) {
        List<Integer> versionList = new ArrayList<Integer>();
        for (String s : versionString.split("\\.")) {
            versionList.add(Integer.valueOf(s));
        }
        while (versionList.size() < 3) {
            versionList.add(0);
        }
        return versionList;
    }

}
