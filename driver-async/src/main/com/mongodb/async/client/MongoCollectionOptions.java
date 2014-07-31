/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.async.client;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.annotations.Immutable;
import org.bson.codecs.Codec;
import org.mongodb.Document;

/**
 * Default options for a Mongo collection.
 *
 * @since 3.0
 */
@Immutable
public final class MongoCollectionOptions extends MongoDatabaseOptions {
    public static Builder builder() {
        return new Builder();
    }

    public MongoCollectionOptions withDefaults(final MongoDatabaseOptions options) {
        Builder builder = new Builder();
        builder.writeConcern(getWriteConcern() != null ? getWriteConcern() : options.getWriteConcern());
        builder.readPreference(getReadPreference() != null ? getReadPreference() : options.getReadPreference());
        return builder.build();
    }

    public static final class Builder extends MongoDatabaseOptions.Builder {

        public MongoCollectionOptions build() {
            return new MongoCollectionOptions(getWriteConcern(), getReadPreference(), getDocumentCodec());
        }

        private Builder() {
        }
    }

    private MongoCollectionOptions(final WriteConcern writeConcern, final ReadPreference readPreference,
                                   final Codec<Document> documentCodec) {
        super(writeConcern, readPreference, documentCodec);
    }
}
