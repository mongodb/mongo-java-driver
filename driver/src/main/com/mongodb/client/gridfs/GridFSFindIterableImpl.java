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

package com.mongodb.client.gridfs;

import com.mongodb.Block;
import com.mongodb.Function;
import com.mongodb.MongoClient;
import com.mongodb.MongoGridFSException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.gridfs.model.GridFSFile;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static java.lang.String.format;

class GridFSFindIterableImpl implements GridFSFindIterable {
    private static final CodecRegistry DEFAULT_CODEC_REGISTRY = MongoClient.getDefaultCodecRegistry();
    private final FindIterable<Document> underlying;

    public GridFSFindIterableImpl(final FindIterable<Document> underlying) {
        this.underlying = underlying;
    }

    @Override
    public GridFSFindIterable sort(final Bson sort) {
        underlying.sort(sort);
        return this;
    }

    @Override
    public GridFSFindIterable skip(final int skip) {
        underlying.skip(skip);
        return this;
    }

    @Override
    public GridFSFindIterable limit(final int limit) {
        underlying.limit(limit);
        return this;
    }

    @Override
    public GridFSFindIterable filter(final Bson filter) {
        underlying.filter(filter);
        return this;
    }

    @Override
    public GridFSFindIterable maxTime(final long maxTime, final TimeUnit timeUnit) {
        underlying.maxTime(maxTime, timeUnit);
        return this;
    }

    @Override
    public GridFSFindIterable batchSize(final int batchSize) {
        underlying.batchSize(batchSize);
        return this;
    }

    @Override
    public GridFSFindIterable noCursorTimeout(final boolean noCursorTimeout) {
        underlying.noCursorTimeout(noCursorTimeout);
        return this;
    }

    @Override
    public MongoCursor<GridFSFile> iterator() {
        return toGridFSFileIterable().iterator();
    }

    @Override
    public GridFSFile first() {
        return toGridFSFileIterable().first();
    }

    @Override
    public <U> MongoIterable<U> map(final Function<GridFSFile, U> mapper) {
        return toGridFSFileIterable().map(mapper);
    }

    @Override
    public void forEach(final Block<? super GridFSFile> block) {
        toGridFSFileIterable().forEach(block);
    }

    @Override
    public <A extends Collection<? super GridFSFile>> A into(final A target) {
        return toGridFSFileIterable().into(target);
    }

    @SuppressWarnings("unchecked")
    private MongoIterable<GridFSFile> toGridFSFileIterable() {
        return underlying.map(new Function<Document, GridFSFile>() {
            @Override
            public GridFSFile apply(final Document document) {
                BsonValue id = getId(document);
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
                    for (String key: extraElementKeys) {
                        extraElements.append(key, document.get(key));
                    }
                    return new GridFSFile(id, filename, length, chunkSize, uploadDate, md5, metadata, extraElements);
                } else {
                    return new GridFSFile(id, filename, length, chunkSize, uploadDate, md5, metadata);
                }
            }
        });
    }

    private Number getAndValidateNumber(final String fieldName, final Document document) {
        Number value = document.get(fieldName, Number.class);
        if ((value.floatValue() % 1) != 0){
            throw new MongoGridFSException(format("Invalid number format for %s", fieldName));
        }
        return value;
    }

    private BsonValue getId(final Document document) {
        Object rawId = document.get("_id");
        if (rawId instanceof ObjectId) {
            return new BsonObjectId((ObjectId) rawId);
        } else {
            return new Document("_id", document.get("_id")).toBsonDocument(BsonDocument.class, DEFAULT_CODEC_REGISTRY).get("_id");
        }
    }

    private static final List<String> VALID_FIELDS = asList("_id", "filename", "length", "chunkSize", "uploadDate", "md5", "metadata");

}
