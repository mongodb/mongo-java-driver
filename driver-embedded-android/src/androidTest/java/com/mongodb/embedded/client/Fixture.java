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


import android.content.Context;
import android.support.test.InstrumentationRegistry;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.ServerVersion;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;


/**
 * Helper class for the acceptance tests.
 */
public final class Fixture {
    private static final String APPLICATION_NAME = "testApp";
    private static final String DATA_DIR = "dataDir";
    private static final String DEFAULT_DATABASE_NAME = "JavaDriverTest";

    private static MongoClient mongoClient;
    private static MongoEmbeddedSettings mongoEmbeddedSettings;
    private static MongoClientSettings mongoClientSettings;
    private static ServerVersion serverVersion;

    static synchronized MongoClient getMongoClient() {
        if (mongoClient == null) {
            cleanDBPath();
            MongoClients.init(getMongoEmbeddedSettings());
            mongoClient = MongoClients.create(getMongoClientSettings());
            Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        }
        return mongoClient;
    }

    static synchronized MongoClientSettings getMongoClientSettings() {
        if (mongoClientSettings == null) {
            File dataDir = InstrumentationRegistry.getContext().getDir(DATA_DIR, Context.MODE_WORLD_WRITEABLE);
            MongoClientSettings.Builder builder = MongoClientSettings.builder()
                    .applicationName(APPLICATION_NAME)
                    .dbPath(dataDir.getAbsolutePath());
            mongoClientSettings = builder.build();
        }
        return mongoClientSettings;
    }

    static synchronized MongoEmbeddedSettings getMongoEmbeddedSettings() {
        if (mongoEmbeddedSettings == null) {
            mongoEmbeddedSettings = MongoEmbeddedSettings.builder()
                    .logLevel(MongoEmbeddedLogLevel.LOGGER)
                    .build();
        }
        return mongoEmbeddedSettings;
    }

    static class ShutdownHook extends Thread {
        @Override
        public void run() {
            close();
        }
    }

    static synchronized void close() {
        if (mongoClient != null) {
            mongoClient.close();
            MongoClients.close();
            mongoClient = null;
        }
    }

    static synchronized MongoDatabase getDefaultDatabase() {
        return getMongoClient().getDatabase(getDefaultDatabaseName());
    }

    static String getDefaultDatabaseName() {
        return DEFAULT_DATABASE_NAME;
    }

    static boolean isNotAtLeastJava7() {
        return javaVersionStartsWith("1.6");
    }

    static boolean runEmbeddedTests() {
        return !(isNotAtLeastJava7());
    }

    static boolean serverVersionLessThan(final String versionString) {
        return getServerVersion().compareTo(new ServerVersion(getVersionList(versionString).subList(0, 3))) < 0;
    }

    static boolean serverVersionGreaterThan(final String versionString) {
        return getServerVersion().compareTo(new ServerVersion(getVersionList(versionString).subList(0, 3))) > 0;
    }

    private static synchronized ServerVersion getServerVersion() {
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

    private static boolean javaVersionStartsWith(final String versionPrefix) {
        return System.getProperty("java.version", "").startsWith(versionPrefix + ".");
    }

    private static void cleanDBPath() {
        Context context = InstrumentationRegistry.getContext();
        File dataDir = context.getDir(DATA_DIR, Context.MODE_WORLD_WRITEABLE);
        cleanDBPath(dataDir);
    }

    private static boolean cleanDBPath(final File file) {
        if (file.isDirectory()) {
            String[] children = file.list();
            if (children != null) {
                for (String child : children) {
                    boolean success = cleanDBPath(new File(file, child));
                    if (!success) {
                        return false;
                    }
                }
            }
        }
        return file.delete();
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

    private Fixture() {
    }

}
