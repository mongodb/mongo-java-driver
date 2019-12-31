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

package com.mongodb.internal.connection;

import com.mongodb.internal.bulk.InsertRequest;
import com.mongodb.internal.validator.CollectibleDocumentFieldNameValidator;
import org.bson.io.BsonOutput;

/**
 * An insert message.
 *
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-insert OP_INSERT
 */
class InsertMessage extends LegacyMessage {

    private final InsertRequest insertRequest;

    InsertMessage(final String collectionName, final InsertRequest insertRequest, final MessageSettings settings) {
        super(collectionName, OpCode.OP_INSERT, settings);
        this.insertRequest = insertRequest;
    }

    @Override
    protected EncodingMetadata encodeMessageBodyWithMetadata(final BsonOutput outputStream) {
        writeInsertPrologue(outputStream);
        int firstDocumentPosition = outputStream.getPosition();
        addCollectibleDocument(insertRequest.getDocument(), outputStream, new CollectibleDocumentFieldNameValidator());
        return new EncodingMetadata(firstDocumentPosition);
    }

    private void writeInsertPrologue(final BsonOutput outputStream) {
        outputStream.writeInt32(0);  // flags
        outputStream.writeCString(getCollectionName());
    }
}
