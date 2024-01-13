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

package com.mongodb.client.unified;

import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.model.GridFSDownloadOptions;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.internal.HexUtils;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import util.Hex;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

final class UnifiedGridFSHelper extends UnifiedHelper{
    private final Entities entities;

    UnifiedGridFSHelper(final Entities entities) {
        this.entities = entities;
    }

    public OperationResult executeFind(final BsonDocument operation) {
        GridFSFindIterable iterable = createGridFSFindIterable(operation);
        try {
            ArrayList<GridFSFile> target = new ArrayList<>();
            iterable.into(target);

            if (target.isEmpty()) {
                return OperationResult.NONE;
            }

            throw new UnsupportedOperationException("expectResult is not implemented for Unified GridFS tests.");
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    public OperationResult executeRename(final BsonDocument operation) {
        GridFSBucket bucket = getGridFsBucket(operation);
        BsonDocument arguments = operation.getDocument("arguments");
        BsonValue id = arguments.get("id");
        String fileName = arguments.get("newFilename").asString().getValue();

        requireNonNull(id);
        requireNonNull(fileName);

        try {
            bucket.rename(id, fileName);
            return OperationResult.NONE;
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    OperationResult executeDelete(final BsonDocument operation) {
        GridFSBucket bucket = getGridFsBucket(operation);

        BsonDocument arguments = operation.getDocument("arguments");
        BsonValue id = arguments.get("id");

        if (arguments.size() > 1) {
            throw new UnsupportedOperationException("Unexpected arguments " + arguments);
        }

        requireNonNull(id);

        try {
            bucket.delete(id);
            return OperationResult.NONE;
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    public OperationResult executeDrop(final BsonDocument operation) {
        GridFSBucket bucket = getGridFsBucket(operation);
        BsonDocument arguments = operation.getDocument("arguments", new BsonDocument());
        if (arguments.size() > 0) {
            throw new UnsupportedOperationException("Unexpected arguments " + operation.get("arguments"));
        }

        try {
            bucket.drop();
            return OperationResult.NONE;
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    public OperationResult executeDownload(final BsonDocument operation) {
        GridFSBucket bucket = getGridFsBucket(operation);

        BsonDocument arguments = operation.getDocument("arguments");
        BsonValue id = arguments.get("id");

        if (arguments.size() > 1) {
            throw new UnsupportedOperationException("Unexpected arguments " + operation.get("arguments"));
        }

        requireNonNull(id);

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            bucket.downloadToStream(id, baos);
            return OperationResult.of(new BsonString(HexUtils.toHex(baos.toByteArray())));
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    public OperationResult executeDownloadByName(final BsonDocument operation) {
        GridFSBucket bucket = entities.getBucket(operation.getString("object").getValue());

        BsonDocument arguments = operation.getDocument("arguments");
        String filename = arguments.getString("filename").getValue();
        requireNonNull(filename);
        GridFSDownloadOptions options = getDownloadOptions(arguments);

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            bucket.downloadToStream(filename, baos, options);
            return OperationResult.of(new BsonString(HexUtils.toHex(baos.toByteArray())));
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    private GridFSDownloadOptions getDownloadOptions(final BsonDocument arguments) {
        GridFSDownloadOptions options = new GridFSDownloadOptions();

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "filename":
                    break;
                case "revision":
                    options.revision(cur.getValue().asNumber().intValue());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }
        return options;
    }

    public OperationResult executeUpload(final BsonDocument operation) {
        GridFSBucket bucket = getGridFsBucket(operation);

        BsonDocument arguments = operation.getDocument("arguments");
        String filename = null;
        byte[] bytes = null;
        GridFSUploadOptions options = new GridFSUploadOptions();

        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "filename":
                    filename = cur.getValue().asString().getValue();
                    break;
                case "source":
                    bytes = Hex.decode(cur.getValue().asDocument().getString("$$hexBytes").getValue());
                    break;
                case "chunkSizeBytes":
                    options.chunkSizeBytes(cur.getValue().asInt32().getValue());
                    break;
                case "disableMD5":
                    break;
                case "metadata":
                    options.metadata(asDocument(cur.getValue().asDocument()));
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }
        requireNonNull(filename);
        requireNonNull(bytes);

        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            BsonObjectId id = new BsonObjectId(bucket.uploadFromStream(filename, bais, options));

            if (operation.containsKey("saveResultAsEntity")) {
                entities.addResult(operation.getString("saveResultAsEntity").getValue(), id);
            }
            return OperationResult.of(id);
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    Document asDocument(final BsonDocument bsonDocument) {
        return new DocumentCodec().decode(new BsonDocumentReader(bsonDocument), DecoderContext.builder().build());
    }

    private GridFSBucket getGridFsBucket(final BsonDocument operation) {
        GridFSBucket bucket = entities.getBucket(operation.getString("object").getValue());
        Long timeoutMS = getAndRemoveTimeoutMS(operation.getDocument("arguments", new BsonDocument()));
        if (timeoutMS != null) {
            bucket = bucket.withTimeout(timeoutMS, TimeUnit.MILLISECONDS);
        }
        return bucket;
    }

    private GridFSFindIterable createGridFSFindIterable(final BsonDocument operation) {
        GridFSBucket bucket = getGridFsBucket(operation);

        BsonDocument arguments = operation.getDocument("arguments");
        BsonDocument filter = arguments.getDocument("filter");
        GridFSFindIterable iterable = bucket.find(filter);
        for (Map.Entry<String, BsonValue> cur : arguments.entrySet()) {
            switch (cur.getKey()) {
                case "session":
                case "filter":
                    break;
                case "sort":
                    iterable.sort(cur.getValue().asDocument());
                    break;
                case "batchSize":
                    iterable.batchSize(cur.getValue().asInt32().intValue());
                    break;
                case "maxTimeMS":
                    iterable.maxTime(cur.getValue().asInt32().longValue(), TimeUnit.MILLISECONDS);
                    break;
                case "skip":
                    iterable.skip(cur.getValue().asInt32().intValue());
                    break;
                case "limit":
                    iterable.limit(cur.getValue().asInt32().intValue());
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported argument: " + cur.getKey());
            }
        }
        return iterable;
    }
}
