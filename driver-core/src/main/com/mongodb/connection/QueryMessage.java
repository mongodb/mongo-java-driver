/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

package com.mongodb.connection;

import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonDocument;
import org.bson.io.BsonOutput;

/**
 * An OP_QUERY message for an actual query (not a command).
 *
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
 */
class QueryMessage extends BaseQueryMessage {
    private final BsonDocument queryDocument;
    private final BsonDocument fields;

    QueryMessage(final String collectionName, final int skip, final int numberToReturn, final BsonDocument queryDocument,
                 final BsonDocument fields, final MessageSettings settings) {
        super(collectionName, skip, numberToReturn, settings);
        this.queryDocument = queryDocument;
        this.fields = fields;
    }

    @Override
    protected EncodingMetadata encodeMessageBodyWithMetadata(final BsonOutput bsonOutput) {
        writeQueryPrologue(bsonOutput);
        int firstDocumentStartPosition = bsonOutput.getPosition();
        addDocument(queryDocument, bsonOutput, new NoOpFieldNameValidator());
        if (fields != null) {
            addDocument(fields, bsonOutput, new NoOpFieldNameValidator());
        }
        return new EncodingMetadata(firstDocumentStartPosition);
    }
}
