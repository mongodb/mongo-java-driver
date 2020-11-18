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
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.internal.HexUtils;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import util.Hex;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;

import static java.util.Objects.requireNonNull;

final class UnifiedGridFSHelper {
    private final Entities entities;

    UnifiedGridFSHelper(final Entities entities) {
        this.entities = entities;
    }

    OperationResult executeDelete(final BsonDocument operation) {
        GridFSBucket bucket = entities.getBucket(operation.getString("object").getValue());

        BsonDocument arguments = operation.getDocument("arguments");
        BsonValue id = arguments.get("id");

        if (arguments.size() > 1) {
            throw new UnsupportedOperationException("Unexpected arguments");
        }

        requireNonNull(id);

        try {
            bucket.delete(id);
            return OperationResult.NONE;
        } catch (Exception e) {
            return OperationResult.of(e);
        }
    }

    public OperationResult executeDownload(final BsonDocument operation) {
        GridFSBucket bucket = entities.getBucket(operation.getString("object").getValue());

        BsonDocument arguments = operation.getDocument("arguments");
        BsonValue id = arguments.get("id");

        if (arguments.size() > 1) {
            throw new UnsupportedOperationException("Unexpected arguments");
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

    public OperationResult executeUpload(final BsonDocument operation) {
        GridFSBucket bucket = entities.getBucket(operation.getString("object").getValue());

        BsonDocument arguments = operation.getDocument("arguments");
        String filename = null;
        byte[] bytes = null;
        GridFSUploadOptions options = new GridFSUploadOptions();

        for (Map.Entry<String, BsonValue> cur : operation.getDocument("arguments").entrySet()) {
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
}
