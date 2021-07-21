/*
 * Copyright 2018 MongoDB, Inc.
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
 *
 */

package documentation;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static org.junit.Assume.assumeTrue;

public class TransactionExample {
    private MongoClient client;

    @Before
    public void setUp() {
        assumeTrue(canRunTest());
        MongoClientSettings.Builder builder = getMongoClientSettingsBuilder()
                .applyConnectionString(new ConnectionString(
                        "mongodb://localhost,localhost:27018,localhost:27019/?serverSelectionTimeoutMS=5000"));
        client = MongoClients.create(builder.build());

        createCollection(client, "hr", "employees");
        createCollection(client, "reporting", "events");
    }

    @After
    public void cleanUp() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void updateEmployeeInfoWithRetry() {
        runTransactionWithRetry(this::updateEmployeeInfo);
    }

    private void runTransactionWithRetry(final Runnable transactional) {
        while (true) {
            try {
                transactional.run();
                break;
            } catch (MongoException e) {
                System.out.println("Transaction aborted. Caught exception during transaction.");

                if (e.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                    System.out.println("TransientTransactionError, aborting transaction and retrying ...");
                } else {
                    throw e;
                }
            }
        }
    }

    private void commitWithRetry(final ClientSession clientSession) {
        while (true) {
            try {
                clientSession.commitTransaction();
                System.out.println("Transaction committed");
                break;
            } catch (MongoException e) {
                // can retry commit
                if (e.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
                    System.out.println("UnknownTransactionCommitResult, retrying commit operation ...");
                } else {
                    System.out.println("Exception during commit ...");
                    throw e;
                }
            }
        }
    }

    private void updateEmployeeInfo() {
        MongoCollection<Document> employeesCollection = client.getDatabase("hr").getCollection("employees");
        MongoCollection<Document> eventsCollection = client.getDatabase("reporting").getCollection("events");

        TransactionOptions txnOptions = TransactionOptions.builder()
                .readPreference(ReadPreference.primary())
                .readConcern(ReadConcern.MAJORITY)
                .writeConcern(WriteConcern.MAJORITY)
                .build();

        try (ClientSession clientSession = client.startSession()) {
            clientSession.startTransaction(txnOptions);

            employeesCollection.updateOne(clientSession,
                    Filters.eq("employee", 3),
                    Updates.set("status", "Inactive"));
            eventsCollection.insertOne(clientSession,
                    new Document("employee", 3).append("status", new Document("new", "Inactive").append("old", "Active")));

            commitWithRetry(clientSession);
        }
    }

    @Test
    public void updateEmployeeInfoUsingWithTransactionHelper() {
        MongoCollection<Document> employeesCollection = client.getDatabase("hr").getCollection("employees");
        MongoCollection<Document> eventsCollection = client.getDatabase("reporting").getCollection("events");

        TransactionOptions txnOptions = TransactionOptions.builder()
                .readPreference(ReadPreference.primary())
                .readConcern(ReadConcern.MAJORITY)
                .writeConcern(WriteConcern.MAJORITY)
                .build();

        try (ClientSession clientSession = client.startSession()) {
            clientSession.withTransaction(() -> {
                employeesCollection.updateOne(clientSession,
                        Filters.eq("employee", 3),
                        Updates.set("status", "Inactive"));
                eventsCollection.insertOne(clientSession,
                        new Document("employee", 3).append("status", new Document("new", "Inactive").append("old", "Active")));
                return null;
            }, txnOptions);
        } catch (MongoException e) {
            System.out.println("Transaction aborted. Caught exception during transaction.");
            throw e;
        }
    }

    private void createCollection(final MongoClient client, final String dbName, final String collectionName) {
        try {
            client.getDatabase(dbName).createCollection(collectionName);
        } catch (MongoCommandException e) {
            if (!e.getErrorCodeName().equals("NamespaceExists")) {
                throw e;
            }
        }
    }

    private boolean canRunTest() {
        if (isSharded()) {
            return serverVersionAtLeast(4, 2);
        } else if (isDiscoverableReplicaSet()) {
            return serverVersionAtLeast(4, 0);
        } else {
            return false;
        }
    }
}
