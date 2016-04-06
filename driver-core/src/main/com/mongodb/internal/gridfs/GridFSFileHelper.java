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

package com.mongodb.internal.gridfs;

import com.mongodb.Function;
import com.mongodb.MongoGridFSException;
import com.mongodb.client.gridfs.model.GridFSFile;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Arrays.asList;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public final class GridFSFileHelper {

    public static Function<Document, GridFSFile> documentToGridFSFileMapper(final CodecRegistry codecRegistry) {
        return new Function<Document, GridFSFile>() {
            @Override
            public GridFSFile apply(final Document document) {
                if (document == null) {
                    return null;
                } else {
                    BsonValue id = getId(document, codecRegistry);
                    String filename = document.getString("filename");
                    long length = getAndValidateNumber("length", document).longValue();
                    int chunkSize = getAndValidateNumber("chunkSize", document).intValue();
                    Date uploadDate = document.getDate("uploadDate");

                    String md5 = document.getString("md5");
                    Document metadata = document.get("metadata", Document.class);
                    Set<String> extraElementKeys = new HashSet<String>(document.keySet());
                    extraElementKeys.removeAll(VALID_FIELDS);

                    if (extraElementKeys.size() > 0) {
                        Document extraElements = new Document();
                        for (String key : extraElementKeys) {
                            extraElements.append(key, document.get(key));
                        }
                        return new GridFSFile(id, filename, length, chunkSize, uploadDate, md5, metadata, extraElements);
                    } else {
                        return new GridFSFile(id, filename, length, chunkSize, uploadDate, md5, metadata);
                    }
                }
            }
        };
    }

    private static BsonValue getId(final Document document, final CodecRegistry codecRegistry) {
        Object rawId = document.get("_id");
        if (rawId instanceof ObjectId) {
            return new BsonObjectId((ObjectId) rawId);
        } else {
            return new Document("_id", document.get("_id")).toBsonDocument(BsonDocument.class, codecRegistry).get("_id");
        }
    }

    private static final List<String> VALID_FIELDS = asList("_id", "filename", "length", "chunkSize", "uploadDate", "md5", "metadata");

    private static Number getAndValidateNumber(final String fieldName, final Document document) {
        Number value = document.get(fieldName, Number.class);
        if ((value.floatValue() % 1) != 0){
            throw new MongoGridFSException(format("Invalid number format for %s", fieldName));
        }
        return value;
    }

    private GridFSFileHelper() {
    }
}
