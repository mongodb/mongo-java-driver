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

package com.mongodb.client.gridfs.codecs;

import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.lang.Nullable;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonReader;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Date;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Arrays.asList;


/**
 * A codec for GridFS Files
 *
 * @since 3.3
 */
public final class GridFSFileCodec implements Codec<GridFSFile> {
    private static final List<String> VALID_FIELDS = asList("_id", "filename", "length", "chunkSize", "uploadDate", "md5", "metadata");
    private final Codec<Document> documentCodec;
    private final Codec<BsonDocument> bsonDocumentCodec;

    /**
     * Create a new instance
     *
     * @param registry the codec registry
     */
    public GridFSFileCodec(final CodecRegistry registry) {
        this.documentCodec = notNull("DocumentCodec", notNull("registry", registry).get(Document.class));
        this.bsonDocumentCodec = notNull("BsonDocumentCodec", registry.get(BsonDocument.class));
    }

    @Override
    public GridFSFile decode(final BsonReader reader, final DecoderContext decoderContext) {
        BsonDocument bsonDocument = bsonDocumentCodec.decode(reader, decoderContext);

        BsonValue id = bsonDocument.get("_id");
        String filename = bsonDocument.get("filename", new BsonString("")).asString().getValue();
        long length = bsonDocument.getNumber("length").longValue();
        int chunkSize = bsonDocument.getNumber("chunkSize").intValue();
        Date uploadDate = new Date(bsonDocument.getDateTime("uploadDate").getValue());
        String md5 = bsonDocument.containsKey("md5") ? bsonDocument.getString("md5").getValue() : null;
        BsonDocument metadataBsonDocument = bsonDocument.getDocument("metadata", new BsonDocument());

        Document optionalMetadata = asDocumentOrNull(metadataBsonDocument);

        for (String key : VALID_FIELDS) {
            bsonDocument.remove(key);
        }
        Document deprecatedExtraElements = asDocumentOrNull(bsonDocument);

        return new GridFSFile(id, filename, length, chunkSize, uploadDate, md5, optionalMetadata, deprecatedExtraElements);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void encode(final BsonWriter writer, final GridFSFile value, final EncoderContext encoderContext) {
        BsonDocument bsonDocument = new BsonDocument();
        bsonDocument.put("_id", value.getId());
        bsonDocument.put("filename", new BsonString(value.getFilename()));
        bsonDocument.put("length", new BsonInt64(value.getLength()));
        bsonDocument.put("chunkSize", new BsonInt32(value.getChunkSize()));
        bsonDocument.put("uploadDate", new BsonDateTime(value.getUploadDate().getTime()));
        if (value.getMD5() != null) {
            bsonDocument.put("md5", new BsonString(value.getMD5()));
        }

        Document metadata = value.getMetadata();
        if (metadata != null) {
            bsonDocument.put("metadata", new BsonDocumentWrapper<Document>(metadata, documentCodec));
        }

        Document extraElements = value.getExtraElements();
        if (extraElements != null) {
            bsonDocument.putAll(new BsonDocumentWrapper<Document>(extraElements, documentCodec));
        }

        bsonDocumentCodec.encode(writer, bsonDocument, encoderContext);
    }

    @Override
    public Class<GridFSFile> getEncoderClass() {
        return GridFSFile.class;
    }

    @Nullable
    private Document asDocumentOrNull(final BsonDocument bsonDocument) {
        if (bsonDocument.isEmpty()) {
            return null;
        } else {
            BsonDocumentReader reader = new BsonDocumentReader(bsonDocument);
            return documentCodec.decode(reader, DecoderContext.builder().build());
        }
    }
}
