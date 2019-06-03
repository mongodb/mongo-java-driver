/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.async.client;

import com.mongodb.Block;
import com.mongodb.ConnectionString;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.async.client.gridfs.GridFSBucket;
import com.mongodb.async.client.gridfs.GridFSBuckets;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.event.CommandEvent;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DocumentCodec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import util.Hex;
import util.JsonPoweredTestHelper;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.getMultiMongosConnectionString;
import static com.mongodb.JsonTestServerVersionChecker.skipTest;
import static com.mongodb.async.client.Fixture.getConnectionString;
import static com.mongodb.async.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.async.client.Fixture.isSharded;
import static com.mongodb.client.CommandMonitoringTestHelper.assertEventsEquality;
import static com.mongodb.client.CommandMonitoringTestHelper.getExpectedEvents;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

// See https://github.com/mongodb/specifications/tree/master/source/transactions/tests
@RunWith(Parameterized.class)
public class RetryableReadsTest {
    private final String filename;
    private final String description;
    private final String databaseName;
    private final String collectionName;
    private final String gridFSBucketName;
    private final BsonDocument gridFSData;
    private final BsonArray data;
    private final BsonDocument definition;
    private final boolean skipTest;
    private JsonPoweredCrudTestHelper helper;
    private final TestCommandListener commandListener;
    private MongoClient mongoClient;
    private CollectionHelper<Document> collectionHelper;
    private boolean useMultipleMongoses = false;
    private ConnectionString connectionString;
    private GridFSBucket gridFSBucket;
    private MongoCollection<BsonDocument> filesCollection;
    private MongoCollection<BsonDocument> chunksCollection;

    private static final long MIN_HEARTBEAT_FREQUENCY_MS = 50L;

    public RetryableReadsTest(final String filename, final String description, final String databaseName,
                              final String collectionName, final BsonArray data, final BsonString bucketName,
                              final BsonDocument definition, final boolean skipTest) {
        this.filename = filename;
        this.description = description;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.definition = definition;
        this.gridFSBucketName = (bucketName != null ? bucketName.getValue() : null);
        this.gridFSData = (bucketName != null ? (BsonDocument) data.get(0) : null);
        this.data = (bucketName != null ? null : data);
        this.commandListener = new TestCommandListener();
        this.skipTest = skipTest;
    }

    @Before
    public void setUp() {
        assumeFalse(skipTest);
        assumeTrue("Skipping test: " + definition.getString("skipReason", new BsonString("")).getValue(),
                !definition.containsKey("skipReason"));
        collectionHelper = new CollectionHelper<Document>(new DocumentCodec(), new MongoNamespace(databaseName, collectionName));

        collectionHelper.killAllSessions();
        collectionHelper.create(collectionName, new CreateCollectionOptions(), WriteConcern.MAJORITY);

        if (data != null) {
            List<BsonDocument> documents = new ArrayList<BsonDocument>();
            for (BsonValue document : data) {
                documents.add(document.asDocument());
            }

            collectionHelper.drop();
            if (documents.size() > 0) {
                collectionHelper.insertDocuments(documents, WriteConcern.MAJORITY);
            }
        }

        final BsonDocument clientOptions = definition.getDocument("clientOptions", new BsonDocument());

        connectionString = getConnectionString();

        useMultipleMongoses = definition.getBoolean("useMultipleMongoses", BsonBoolean.FALSE).getValue();
        if (useMultipleMongoses) {
            assumeTrue(isSharded());
            connectionString = getMultiMongosConnectionString();
            assumeTrue("The system property org.mongodb.test.transaction.uri is not set.", connectionString != null);
        }

        MongoClientSettings.Builder builder = MongoClientSettings.builder().applyConnectionString(connectionString);

        if (System.getProperty("java.version").startsWith("1.6.")) {
            builder.applyToSslSettings(new Block<SslSettings.Builder>() {
                @Override
                public void apply(final SslSettings.Builder builder) {
                    builder.invalidHostNameAllowed(true);
                }
            });
        }
        builder.addCommandListener(commandListener)
                .applyToSocketSettings(new Block<SocketSettings.Builder>() {
                    @Override
                    public void apply(final SocketSettings.Builder builder) {
                        builder.readTimeout(5, TimeUnit.SECONDS);
                    }
                })
                .retryWrites(clientOptions.getBoolean("retryWrites", BsonBoolean.FALSE).getValue())
                .writeConcern(getWriteConcern(clientOptions))
                .readConcern(getReadConcern(clientOptions))
                .readPreference(getReadPreference(clientOptions))
                .retryReads(clientOptions.getBoolean("retryReads", BsonBoolean.TRUE).getValue())
                .applyToServerSettings(new Block<ServerSettings.Builder>() {
                    @Override
                    public void apply(final ServerSettings.Builder builder) {
                        builder.minHeartbeatFrequency(MIN_HEARTBEAT_FREQUENCY_MS, TimeUnit.MILLISECONDS);
                    }
                });

        if (clientOptions.containsKey("heartbeatFrequencyMS")) {
            builder.applyToServerSettings(new Block<ServerSettings.Builder>() {
                @Override
                public void apply(final ServerSettings.Builder builder) {
                    builder.heartbeatFrequency(clientOptions.getInt32("heartbeatFrequencyMS").intValue(), TimeUnit.MILLISECONDS);
                }
            });
        }

        mongoClient = MongoClients.create(builder.build());

        MongoDatabase database = mongoClient.getDatabase(databaseName);
        if (gridFSBucketName != null) {
            setupGridFSBuckets(database);
            commandListener.reset();
        }
        helper = new JsonPoweredCrudTestHelper(description, database, database.getCollection(collectionName, BsonDocument.class),
                gridFSBucket, mongoClient);
        if (definition.containsKey("failPoint")) {
            collectionHelper.runAdminCommand(definition.getDocument("failPoint"));
        }

    }

    private ReadConcern getReadConcern(final BsonDocument clientOptions) {
        if (clientOptions.containsKey("readConcernLevel")) {
            return new ReadConcern(ReadConcernLevel.fromString(clientOptions.getString("readConcernLevel").getValue()));
        } else {
            return ReadConcern.DEFAULT;
        }
    }

    private WriteConcern getWriteConcern(final BsonDocument clientOptions) {
        if (clientOptions.containsKey("w")) {
            if (clientOptions.isNumber("w")) {
                return new WriteConcern(clientOptions.getNumber("w").intValue());
            } else {
                return new WriteConcern(clientOptions.getString("w").getValue());
            }
        } else {
            return WriteConcern.ACKNOWLEDGED;
        }
    }

    private ReadPreference getReadPreference(final BsonDocument clientOptions) {
        if (clientOptions.containsKey("readPreference")) {
            return ReadPreference.valueOf(clientOptions.getString("readPreference").getValue());
        } else {
            return ReadPreference.primary();
        }
    }

    private void setupGridFSBuckets(final MongoDatabase database) {
        gridFSBucket = GridFSBuckets.create(database);
        filesCollection = Fixture.initializeCollection(new MongoNamespace(databaseName, "fs.files"))
                .withDocumentClass(BsonDocument.class);
        chunksCollection = Fixture.initializeCollection(new MongoNamespace(databaseName, "fs.chunks"))
                .withDocumentClass(BsonDocument.class);

        final List<BsonDocument> filesDocuments = processFiles(gridFSData.getArray("fs.files", new BsonArray()),
                new ArrayList<BsonDocument>());
        if (!filesDocuments.isEmpty()) {
            new MongoOperation<Void>() {
                @Override
                public void execute() {
                    filesCollection.insertMany(filesDocuments, getCallback());
                }
            }.get();
        }

        final List<BsonDocument> chunksDocuments = processChunks(gridFSData.getArray("fs.chunks", new BsonArray()),
                new ArrayList<BsonDocument>());
        if (!chunksDocuments.isEmpty()) {
            new MongoOperation<Void>() {
                @Override
                public void execute() {
                    chunksCollection.insertMany(chunksDocuments, getCallback());
                }
            }.get();
        }
    }

    @After
    public void cleanUp() {
        if (mongoClient != null) {
            mongoClient.close();
        }

        if (collectionHelper != null && definition.containsKey("failPoint")) {
            collectionHelper.runAdminCommand(new BsonDocument("configureFailPoint",
                    definition.getDocument("failPoint").getString("configureFailPoint"))
                    .append("mode", new BsonString("off")));
        }
    }

    @Test
    public void shouldPassAllOutcomes() {
        executeOperations(definition.getArray("operations"));

        if (definition.containsKey("expectations")) {
            // TODO: null operation may cause test failures, since it's used to grab the read preference
            // TODO: though read-pref.json doesn't declare expectations, so maybe not
            List<CommandEvent> expectedEvents = getExpectedEvents(definition.getArray("expectations"), databaseName, null);
            List<CommandEvent> events = commandListener.getCommandStartedEvents();

            assertEventsEquality(expectedEvents, events);
        }

        BsonDocument expectedOutcome = definition.getDocument("outcome", new BsonDocument());
        if (expectedOutcome.containsKey("collection")) {
            List<BsonDocument> collectionData = collectionHelper.find(new BsonDocumentCodec());
            assertEquals(expectedOutcome.getDocument("collection").getArray("data").getValues(), collectionData);
        }
    }

    private void executeOperations(final BsonArray operations) {
        for (BsonValue cur : operations) {
            final BsonDocument operation = cur.asDocument();
            BsonValue expectedResult = operation.get("result");

            try {
                BsonDocument actualOutcome = helper.getOperationResults(operation);
                if (expectedResult != null) {
                    BsonValue actualResult = actualOutcome.get("result");
                    if (actualResult.isDocument()) {
                        assertEquals("Expected operation result differs from actual", expectedResult, actualResult);
                    }
                }
            } catch (MongoException e) {
                assertTrue("Unexpected error in operation", operation.getBoolean("error", BsonBoolean.FALSE).getValue());
            }
        }
    }

    @Parameterized.Parameters(name = "{0}: {2}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/retryable-reads")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            for (BsonValue test : testDocument.getArray("tests")) {
                data.add(new Object[]{file.getName(), test.asDocument().getString("description").getValue(),
                        testDocument.getString("database_name", new BsonString(getDefaultDatabaseName())).getValue(),
                        testDocument.getString("collection_name",
                                new BsonString(file.getName().substring(0, file.getName().lastIndexOf(".")))).getValue(),
                        (testDocument.containsKey("bucket_name") ? new BsonArray(singletonList(testDocument.getDocument("data")))
                                : testDocument.getArray("data")),
                        testDocument.getString("bucket_name", null), test.asDocument(), skipTest(testDocument, test.asDocument())});
            }
        }
        return data;
    }

    private List<BsonDocument> processFiles(final BsonArray bsonArray, final List<BsonDocument> documents) {
        for (BsonValue rawDocument : bsonArray.getValues()) {
            if (rawDocument.isDocument()) {
                BsonDocument document = rawDocument.asDocument();
                if (document.get("length").isInt32()) {
                    document.put("length", new BsonInt64(document.getInt32("length").getValue()));
                }
                if (document.containsKey("metadata") && document.getDocument("metadata").isEmpty()) {
                    document.remove("metadata");
                }
                if (document.containsKey("aliases") && document.getArray("aliases").getValues().size() == 0) {
                    document.remove("aliases");
                }
                if (document.containsKey("contentType") && document.getString("contentType").getValue().length() == 0) {
                    document.remove("contentType");
                }
                documents.add(document);
            }
        }
        return documents;
    }

    private List<BsonDocument> processChunks(final BsonArray bsonArray, final List<BsonDocument> documents) {
        for (BsonValue rawDocument: bsonArray.getValues()) {
            if (rawDocument.isDocument()) {
                documents.add(parseHexDocument(rawDocument.asDocument()));
            }
        }
        return documents;
    }

    private BsonDocument parseHexDocument(final BsonDocument document) {
        return parseHexDocument(document, "data");
    }

    private BsonDocument parseHexDocument(final BsonDocument document, final String hexDocument) {
        if (document.containsKey(hexDocument) && document.get(hexDocument).isDocument()) {
            byte[] bytes = Hex.decode(document.getDocument(hexDocument).getString("$hex").getValue());
            document.put(hexDocument, new BsonBinary(bytes));
        }
        return document;
    }
}
