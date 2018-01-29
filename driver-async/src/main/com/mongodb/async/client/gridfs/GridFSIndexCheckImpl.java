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

package com.mongodb.async.client.gridfs;

import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.FindIterable;
import com.mongodb.async.client.ListIndexesIterable;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.session.ClientSession;
import org.bson.Document;

import java.util.ArrayList;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;

final class GridFSIndexCheckImpl implements GridFSIndexCheck {
    private static final Document PROJECTION = new Document("_id", 1);
    private final ClientSession clientSession;
    private final MongoCollection<GridFSFile> filesCollection;
    private final MongoCollection<Document> chunksCollection;

    GridFSIndexCheckImpl(final ClientSession clientSession, final MongoCollection<GridFSFile> filesCollection,
                         final MongoCollection<Document> chunksCollection) {
        this.clientSession = clientSession;
        this.filesCollection = notNull("files collection", filesCollection);
        this.chunksCollection = notNull("chunks collection", chunksCollection);
    }

    @Override
    public void checkAndCreateIndex(final SingleResultCallback<Void> callback) {
        MongoCollection<Document> collection = filesCollection.withDocumentClass(Document.class).withReadPreference(primary());
        FindIterable<Document> findIterable;
        if (clientSession != null) {
            findIterable = collection.find(clientSession);
        } else {
            findIterable = collection.find();
        }

        findIterable.projection(PROJECTION).first(
                new SingleResultCallback<Document>() {
                    @Override
                    public void onResult(final Document result, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else if (result == null) {
                            checkFilesIndex(callback);
                        } else {
                            callback.onResult(null, null);
                        }
                    }
                });
    }

    private <T> void hasIndex(final MongoCollection<T> collection, final Document index, final SingleResultCallback<Boolean> callback) {
        ListIndexesIterable<Document> listIndexesIterable;
        if (clientSession != null) {
            listIndexesIterable = collection.listIndexes(clientSession);
        } else {
            listIndexesIterable = collection.listIndexes();
        }

        listIndexesIterable.into(new ArrayList<Document>(), new SingleResultCallback<ArrayList<Document>>() {
            @Override
            public void onResult(final ArrayList<Document> indexes, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    boolean hasIndex = false;
                    for (Document indexDoc : indexes) {
                        if (indexDoc.get("key", Document.class).equals(index)) {
                            hasIndex = true;
                            break;
                        }
                    }
                    callback.onResult(hasIndex, null);
                }
            }
        });
    }

    private void checkFilesIndex(final SingleResultCallback<Void> callback) {
        final Document filesIndex = new Document("filename", 1).append("uploadDate", 1);
        hasIndex(filesCollection.withReadPreference(primary()), filesIndex,
                new SingleResultCallback<Boolean>() {
                    @Override
                    public void onResult(final Boolean result, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else if (!result) {
                            SingleResultCallback<String> createIndexCallback = new SingleResultCallback<String>() {
                                @Override
                                public void onResult(final String result, final Throwable t) {
                                    if (t != null) {
                                        callback.onResult(null, t);
                                    } else {
                                        checkChunksIndex(callback);
                                    }
                                }
                            };
                            if (clientSession != null) {
                                filesCollection.createIndex(clientSession, filesIndex, createIndexCallback);
                            } else {
                                filesCollection.createIndex(filesIndex, createIndexCallback);
                            }
                        } else {
                            checkChunksIndex(callback);
                        }
                    }
                });
    }

    private void checkChunksIndex(final SingleResultCallback<Void> callback) {
        final Document chunksIndex = new Document("files_id", 1).append("n", 1);
        hasIndex(chunksCollection.withReadPreference(primary()), chunksIndex, new SingleResultCallback<Boolean>() {
            @Override
            public void onResult(final Boolean result, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else if (!result) {
                    SingleResultCallback<String> createIndexCallback = new SingleResultCallback<String>() {
                        @Override
                        public void onResult(final String result, final Throwable t) {
                            if (t != null) {
                                callback.onResult(null, t);
                            } else {
                                callback.onResult(null, null);
                            }
                        }
                    };
                    if (clientSession != null) {
                        chunksCollection.createIndex(clientSession, chunksIndex, new IndexOptions().unique(true), createIndexCallback);
                    } else {
                        chunksCollection.createIndex(chunksIndex, new IndexOptions().unique(true), createIndexCallback);
                    }
                } else {
                    callback.onResult(null, null);
                }
            }
        });
    }
}
