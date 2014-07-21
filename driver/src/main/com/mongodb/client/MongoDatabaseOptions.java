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

package com.mongodb.client;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.annotations.Immutable;
import com.mongodb.codecs.DocumentCodec;
import org.bson.codecs.Codec;
import org.mongodb.Document;

@Immutable
public class MongoDatabaseOptions {
    private final WriteConcern writeConcern;
    private final ReadPreference readPreference;
    private final Codec<Document> documentCodec;

    public static Builder builder() {
        return new Builder();
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    public ReadPreference getReadPreference() {
        return readPreference;
    }

    public Codec<Document> getDocumentCodec() {
        return documentCodec;
    }

    public MongoDatabaseOptions withDefaults(final MongoClientOptions options) {
        Builder builder = new Builder();
        builder.writeConcern = getWriteConcern() != null ? getWriteConcern() : options.getWriteConcern();
        builder.readPreference = getReadPreference() != null ? getReadPreference() : options.getReadPreference();
        builder.documentCodec = getDocumentCodec() != null ? getDocumentCodec()
                                                           : new DocumentCodec();
        return builder.build();
    }

    public static class Builder {
        //TODO: there is definitely a better way to share this state
        //CHECKSTYLE:OFF
        WriteConcern writeConcern;
        ReadPreference readPreference;
        Codec<Document> documentCodec;
        //CHECKSTYLE:ON

        public Builder writeConcern(final WriteConcern aWriteConcern) {
            this.writeConcern = aWriteConcern;
            return this;
        }

        public Builder readPreference(final ReadPreference aReadPreference) {
            this.readPreference = aReadPreference;
            return this;
        }

        public Builder documentCodec(final Codec<Document> aDocumentCodec) {
            this.documentCodec = aDocumentCodec;
            return this;
        }

        public MongoDatabaseOptions build() {
            return new MongoDatabaseOptions(writeConcern, readPreference, documentCodec);
        }

        Builder() {
        }
    }

    MongoDatabaseOptions(final WriteConcern writeConcern, final ReadPreference readPreference, final Codec<Document> documentCodec) {
        this.writeConcern = writeConcern;
        this.readPreference = readPreference;
        this.documentCodec = documentCodec;
    }

}
