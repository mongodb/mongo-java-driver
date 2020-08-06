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

package com.mongodb.workload;

import com.mongodb.client.JsonPoweredCrudTestHelper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.json.JsonWriterSettings;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A workload executor for the driver.  It will execute the provided workload in a loop, until the process is terminated.
 *
 * The following system properties are supported:
 *
 *  org.mongodb.test.uri: the connection string required to connect to the cluster under test
 *  org.mongodb.workload.spec: a JSON representation of the the workload
 *  org.mongodb.workload.output.directory: the output directory in which to write the results.json
 */
public class WorkloadExecutor {
    private static volatile boolean done;
    private static final AtomicInteger numSuccesses = new AtomicInteger();
    private static final AtomicInteger numFailures = new AtomicInteger();
    private static final AtomicInteger numErrors = new AtomicInteger();

    /**
     * The main entry point
     *
     * @param args no arguments are expected
     */
    public static void main(String[] args) {
        String outputDirectory =
                System.getProperty("org.mongodb.workload.output.directory", System.getProperty("user.dir"));
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                done = true;
                writeResults(outputDirectory);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));

        String connectionString = System.getProperty("org.mongodb.test.uri", "mongodb://localhost");

        MongoClient mongoClient = MongoClients.create(connectionString);

        String workloadSpec = System.getProperty("org.mongodb.workload.spec");
        if (workloadSpec == null) {
            throw new IllegalArgumentException("'org.mongodb.workload.spec' system property is required");
        }
        BsonDocument workload = BsonDocument.parse(workloadSpec);

        MongoDatabase database = mongoClient.getDatabase(workload.getString("database").getValue());
        MongoCollection<BsonDocument> collection = database.getCollection(workload.getString("collection").getValue(),
                BsonDocument.class);

        JsonPoweredCrudTestHelper helper = new JsonPoweredCrudTestHelper("Workload executor", database, collection);

        BsonArray operations = workload.getArray("operations");

        outer:
        while (!done) {
            for (BsonValue cur : operations.getValues()) {
                try {
                    if (done) {
                        break outer;
                    }
                    BsonDocument operation = cur.asDocument().clone();
                    BsonValue expectedResult = operation.get("result");
                    BsonDocument resultDocument = helper.getOperationResults(operation);

                    if (expectedResult != null && !resultDocument.get("result").equals(expectedResult)) {
                        numFailures.incrementAndGet();
                    } else {
                        numSuccesses.incrementAndGet();
                    }
                } catch (Exception e) {
                    numErrors.incrementAndGet();
                    e.printStackTrace();
                }
            }
        }
    }

    private static void writeResults(final String outputDirectory) throws IOException {
        BsonDocument resultsDocument = new BsonDocument()
                .append("numSuccesses", new BsonInt32(numSuccesses.intValue()))
                .append("numFailures", new BsonInt32(numFailures.intValue()))
                .append("numErrors", new BsonInt32(numErrors.intValue()));

        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(new File(outputDirectory, "results.json"))))) {
            writer.write(resultsDocument.toJson(JsonWriterSettings.builder().indent(true).build()));
            writer.newLine();
            writer.flush();
        }
    }
}
