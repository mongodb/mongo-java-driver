/*
 * Copyright 2015 MongoDB, Inc.
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

package com.mongodb.async.client.gridfs;

import com.mongodb.MongoGridFSException;
import com.mongodb.MongoNamespace;
import com.mongodb.async.client.DatabaseTestCase;
import com.mongodb.async.client.Fixture;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import util.Hex;
import util.JsonPoweredTestHelper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.async.client.gridfs.helpers.AsyncStreamHelper.toAsyncInputStream;
import static com.mongodb.async.client.gridfs.helpers.AsyncStreamHelper.toAsyncOutputStream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

// See https://github.com/10gen/specifications/tree/master/source/gridfs/tests
@RunWith(Parameterized.class)
public class GridFSTest extends DatabaseTestCase {
    private final String filename;
    private final String description;
    private final BsonDocument data;
    private final BsonDocument definition;
    private MongoCollection<BsonDocument> filesCollection;
    private MongoCollection<BsonDocument> chunksCollection;
    private GridFSBucket gridFSBucket;

    public GridFSTest(final String filename, final String description, final BsonDocument data, final BsonDocument definition) {
        this.filename = filename;
        this.description = description;
        this.data = data;
        this.definition = definition;
    }

    @Before
    @Override
    public void setUp() {
        super.setUp();
        gridFSBucket = GridFSBuckets.create(database);
        filesCollection = Fixture.initializeCollection(new MongoNamespace(getDefaultDatabaseName(), "fs.files"))
                .withDocumentClass(BsonDocument.class);
        chunksCollection = Fixture.initializeCollection(new MongoNamespace(getDefaultDatabaseName(), "fs.chunks"))
                .withDocumentClass(BsonDocument.class);

        final List<BsonDocument> filesDocuments = processFiles(data.getArray("files", new BsonArray()), new ArrayList<BsonDocument>());
        if (!filesDocuments.isEmpty()) {
            new MongoOperation<Void>() {
                @Override
                public void execute() {
                    filesCollection.insertMany(filesDocuments, getCallback());
                }
            }.get();
        }

        final List<BsonDocument> chunksDocuments = processChunks(data.getArray("chunks", new BsonArray()), new ArrayList<BsonDocument>());
        if (!chunksDocuments.isEmpty()) {
            new MongoOperation<Void>() {
                @Override
                public void execute() {
                    chunksCollection.insertMany(chunksDocuments, getCallback());
                }
            }.get();
        }
    }

    @Test
    public void shouldPassAllOutcomes() {
        arrangeGridFS(definition.getDocument("arrange", new BsonDocument()));
        actionGridFS(definition.getDocument("act"), definition.getDocument("assert"));
    }

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/gridfs-tests")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            for (BsonValue test : testDocument.getArray("tests")) {
                data.add(new Object[]{file.getName(), test.asDocument().getString("description").getValue(),
                        testDocument.getDocument("data"), test.asDocument()});
            }
        }
        return data;
    }

    private void arrangeGridFS(final BsonDocument arrange) {
        if (arrange.isEmpty()) {
            return;
        }
        for (BsonValue fileToArrange : arrange.getArray("data", new BsonArray())) {
            final BsonDocument document = fileToArrange.asDocument();
            if (document.containsKey("delete") && document.containsKey("deletes")) {
                for (BsonValue toDelete : document.getArray("deletes")) {
                    final BsonDocument query = toDelete.asDocument().getDocument("q");
                    int limit = toDelete.asDocument().getInt32("limit").getValue();

                    final MongoCollection<BsonDocument> collection;
                    if (document.getString("delete").getValue().equals("fs.files")) {
                        collection = filesCollection;
                    } else {
                        collection = chunksCollection;
                    }

                    if (limit == 1) {
                        new MongoOperation<DeleteResult>() {
                            @Override
                            public void execute() {
                                collection.deleteOne(query, getCallback());
                            }
                        }.get();
                    } else {
                        new MongoOperation<DeleteResult>() {
                            @Override
                            public void execute() {
                                collection.deleteMany(query, getCallback());
                            }
                        }.get();
                    }
                }
            } else if (document.containsKey("insert") && document.containsKey("documents")) {
                if (document.getString("insert").getValue().equals("fs.files")) {
                    new MongoOperation<Void>() {
                        @Override
                        public void execute() {
                            filesCollection.insertMany(processFiles(document.getArray("documents"), new ArrayList<BsonDocument>()),
                                    getCallback());
                        }
                    }.get();
                } else {
                    new MongoOperation<Void>() {
                        @Override
                        public void execute() {
                            chunksCollection.insertMany(processChunks(document.getArray("documents"), new ArrayList<BsonDocument>()),
                                    getCallback());
                        }
                    }.get();
                }
            } else if (document.containsKey("update") && document.containsKey("updates")) {
                final MongoCollection<BsonDocument> collection;
                if (document.getString("update").getValue().equals("fs.files")) {
                    collection = filesCollection;
                } else {
                    collection = chunksCollection;
                }

                for (BsonValue rawUpdate : document.getArray("updates")) {
                    final BsonDocument query = rawUpdate.asDocument().getDocument("q");
                    final BsonDocument update = rawUpdate.asDocument().getDocument("u");
                    update.put("$set", parseHexDocument(update.getDocument("$set")));
                    new MongoOperation<UpdateResult>() {
                        @Override
                        public void execute() {
                            collection.updateMany(query, update, getCallback());
                        }
                    }.get();
                }
            } else {
                throw new IllegalArgumentException("Unsupported arrange: " + document);
            }
        }
    }

    private void actionGridFS(final BsonDocument action, final BsonDocument assertion) {
        if (action.isEmpty()) {
            return;
        }

        String operation = action.getString("operation").getValue();
        if (operation.equals("delete")) {
            doDelete(action.getDocument("arguments"), assertion);
        } else if (operation.equals("download")) {
            doDownload(action.getDocument("arguments"), assertion);
        } else if (operation.equals("download_by_name")) {
            doDownloadByName(action.getDocument("arguments"), assertion);
        } else if (operation.equals("upload")) {
            doUpload(action.getDocument("arguments"), assertion);
        } else {
            throw new IllegalArgumentException("Unknown operation: " + operation);
        }
    }

    private void doDelete(final BsonDocument arguments, final BsonDocument assertion) {
        Throwable error = null;

        try {
            new MongoOperation<Void>() {
                @Override
                public void execute() {
                    gridFSBucket.delete(arguments.getObjectId("id").getValue(), getCallback());
                }
            }.get();
        } catch (MongoGridFSException e) {
            error = e;
        }

        if (assertion.containsKey("error")) {
            assertNotNull("Should have thrown an exception", error);
        } else {
            assertNull("Should not have thrown an exception", error);
            for (BsonValue rawDataItem : assertion.getArray("data")) {
                BsonDocument dataItem = rawDataItem.asDocument();
                for (BsonValue deletedItem : dataItem.getArray("deletes", new BsonArray())) {
                    String delete = dataItem.getString("delete", new BsonString("none")).getValue();
                    BsonObjectId id = new BsonObjectId(new ObjectId());
                    if (delete.equals("expected.files")) {
                        id = deletedItem.asDocument().getDocument("q").getObjectId("_id");
                    } else if (delete.equals("expected.chunks")) {
                        id = deletedItem.asDocument().getDocument("q").getObjectId("files_id");
                    }
                    long filesCount = getFilesCount(new BsonDocument("_id", id));
                    long chunksCount = getChunksCount(new BsonDocument("files_id", id));

                    assertEquals(filesCount, 0);
                    assertEquals(chunksCount, 0);
                }
            }
        }
    }

    private void doDownload(final BsonDocument arguments, final BsonDocument assertion) {
        Throwable error = null;
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            new MongoOperation<Long>() {
                @Override
                public void execute() {
                    gridFSBucket.downloadToStream(arguments.getObjectId("id").getValue(), toAsyncOutputStream(outputStream), getCallback());
                }
            }.get();
            outputStream.close();
        } catch (Throwable e) {
            error = e;
        }

        if (assertion.containsKey("result")) {
            assertNull("Should not have thrown an exception", error);
            assertEquals(Hex.encode(outputStream.toByteArray()).toLowerCase(),
                    assertion.getDocument("result").getString("$hex").getValue());
        } else if (assertion.containsKey("error")) {
            assertNotNull("Should have thrown an exception", error);
        }
    }

    private void doDownloadByName(final BsonDocument arguments, final BsonDocument assertion) {
        Throwable error = null;
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        try {
            final GridFSDownloadOptions options = new GridFSDownloadOptions();
            if (arguments.containsKey("options")) {
                int revision = arguments.getDocument("options").getInt32("revision").getValue();
                options.revision(revision);
            }

            new MongoOperation<Long>() {
                @Override
                public void execute() {
                    gridFSBucket.downloadToStream(arguments.getString("filename").getValue(), toAsyncOutputStream(outputStream),
                            options, getCallback());
                }
            }.get();

            outputStream.close();
        } catch (Throwable e) {
            error = e;
        }
        if (assertion.containsKey("result")) {
            assertNull("Should not have thrown an exception", error);
            assertEquals(Hex.encode(outputStream.toByteArray()).toLowerCase(),
                    assertion.getDocument("result").getString("$hex").getValue());
        } else if (assertion.containsKey("error")) {
            assertNotNull("Should have thrown an exception", error);
        }
    }

    private void doUpload(final BsonDocument rawArguments, final BsonDocument assertion) {
        Throwable error = null;
        ObjectId objectId = null;
        BsonDocument arguments = parseHexDocument(rawArguments, "source");
        try {
            final String filename = arguments.getString("filename").getValue();
            final InputStream inputStream = new ByteArrayInputStream(arguments.getBinary("source").getData());
            final GridFSUploadOptions options = new GridFSUploadOptions();
            BsonDocument rawOptions = arguments.getDocument("options", new BsonDocument());
            if (rawOptions.containsKey("chunkSizeBytes")) {
                options.chunkSizeBytes(rawOptions.getInt32("chunkSizeBytes").getValue());
            }
            if (rawOptions.containsKey("metadata")) {
                options.metadata(Document.parse(rawOptions.getDocument("metadata").toJson()));
            }

            objectId = new MongoOperation<ObjectId>() {
                @Override
                public void execute() {
                    gridFSBucket.uploadFromStream(filename, toAsyncInputStream(inputStream), options, getCallback());
                }
            }.get();
        } catch (Throwable e) {
            error = e;
        }

        if (assertion.containsKey("error")) {
            // We don't need to read anything more so don't see the extra chunk
            if (!assertion.getString("error").getValue().equals("ExtraChunk")) {
                assertNotNull("Should have thrown an exception", error);
            }
        } else {
            assertNull("Should not have thrown an exception", error);
            for (BsonValue rawDataItem : assertion.getArray("data", new BsonArray())) {
                BsonDocument dataItem = rawDataItem.asDocument();
                String insert = dataItem.getString("insert", new BsonString("none")).getValue();
                if (insert.equals("expected.files")) {
                    List<BsonDocument> documents = processFiles(dataItem.getArray("documents", new BsonArray()),
                            new ArrayList<BsonDocument>());

                    assertEquals(getFilesCount(new BsonDocument()), documents.size());
                    BsonDocument actual = new MongoOperation<BsonDocument>() {
                        @Override
                        public void execute() {
                            filesCollection.find().first(getCallback());
                        }
                    }.get();
                    for (BsonDocument expected : documents) {
                        assertEquals(expected.get("length"), actual.get("length"));
                        assertEquals(expected.get("chunkSize"), actual.get("chunkSize"));
                        assertEquals(expected.get("md5"), actual.get("md5"));
                        assertEquals(expected.get("filename"), actual.get("filename"));

                        if (expected.containsKey("metadata")) {
                            assertEquals(expected.get("metadata"), actual.get("metadata"));
                        }
                    }
                } else if (insert.equals("expected.chunks")) {
                    List<BsonDocument> documents = processChunks(dataItem.getArray("documents", new BsonArray()),
                            new ArrayList<BsonDocument>());
                    assertEquals(getChunksCount(new BsonDocument()), documents.size());

                    List<BsonDocument> actualDocuments = new MongoOperation<List<BsonDocument>>() {
                        @Override
                        public void execute() {
                            chunksCollection.find().into(new ArrayList<BsonDocument>(), getCallback());
                        }
                    }.get();

                    for (int i = 0; i < documents.size(); i++) {
                        BsonDocument expected = documents.get(i);
                        BsonDocument actual;
                        actual = actualDocuments.get(i);
                        assertEquals(new BsonObjectId(objectId), actual.getObjectId("files_id"));
                        assertEquals(expected.get("n"), actual.get("n"));
                        assertEquals(expected.get("data"), actual.get("data"));
                    }
                }
            }
        }
    }

    private long getChunksCount(final BsonDocument filter) {
        return new MongoOperation<Long>() {
            @Override
            public void execute() {
                chunksCollection.count(filter, getCallback());
            }
        }.get();
    }

    private long getFilesCount(final BsonDocument filter) {
        return new MongoOperation<Long>() {
            @Override
            public void execute() {
                filesCollection.count(filter, getCallback());
            }
        }.get();
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
        for (BsonValue rawDocument : bsonArray.getValues()) {
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
