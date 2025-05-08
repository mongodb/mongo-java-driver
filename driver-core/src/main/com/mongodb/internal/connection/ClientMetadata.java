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

import com.mongodb.MongoDriverInformation;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

import static com.mongodb.internal.connection.ClientMetadataHelper.createClientMetadataDocument;
import static com.mongodb.internal.connection.ClientMetadataHelper.updateClientMedataDocument;

/**
 * Represents metadata of the current MongoClient.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class ClientMetadata {
    private volatile BsonDocument clientMetadataBsonDocument;

    public ClientMetadata(@Nullable final String applicationName, final MongoDriverInformation mongoDriverInformation) {
        this.clientMetadataBsonDocument = createClientMetadataDocument(applicationName, mongoDriverInformation);
    }

    /**
     * Returns mutable BsonDocument that represents the client metadata.
     */
    public BsonDocument getBsonDocument() {
        return clientMetadataBsonDocument;
    }

    public void append(final MongoDriverInformation mongoDriverInformation) {
        this.clientMetadataBsonDocument = updateClientMedataDocument(clientMetadataBsonDocument, mongoDriverInformation);
    }
}

