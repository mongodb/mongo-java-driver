/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

package org.mongodb;

import org.bson.types.Document;
import org.mongodb.annotations.Immutable;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.Serializer;
import org.mongodb.serialization.serializers.DocumentSerializer;

@Immutable
public class MongoDatabaseOptions {
    private final PrimitiveSerializers primitiveSerializers;
    private final WriteConcern writeConcern;
    private final ReadPreference readPreference;
    private final Serializer<Document> documentSerializer;

    public static Builder builder() {
        return new Builder();
    }

    public PrimitiveSerializers getPrimitiveSerializers() {
        return primitiveSerializers;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    public ReadPreference getReadPreference() {
        return readPreference;
    }

    public Serializer<Document> getDocumentSerializer() {
        return documentSerializer;
    }

    public MongoDatabaseOptions withDefaults(final MongoClientOptions options) {
        final Builder builder = new Builder();
        builder.primitiveSerializers = getPrimitiveSerializers() != null ? getPrimitiveSerializers()
                                                                    : options.getPrimitiveSerializers();
        builder.writeConcern = getWriteConcern() != null ? getWriteConcern() : options.getWriteConcern();
        builder.readPreference = getReadPreference() != null ? getReadPreference() : options.getReadPreference();
        builder.documentSerializer = getDocumentSerializer() != null ? getDocumentSerializer()
                                                                : new DocumentSerializer(builder
                                                                                         .primitiveSerializers);
        return builder.build();
    }

    public static class Builder {
        //TODO: there is definitely a better way to share this state
        //CHECKSTYLE:OFF
        PrimitiveSerializers primitiveSerializers;
        WriteConcern writeConcern;
        ReadPreference readPreference;
        Serializer<Document> documentSerializer;
        //CHECKSTYLE:ON

        public Builder primitiveSerializers(final PrimitiveSerializers aPrimitiveSerializers) {
            this.primitiveSerializers = aPrimitiveSerializers;
            return this;
        }

        public Builder writeConcern(final WriteConcern aWriteConcern) {
            this.writeConcern = aWriteConcern;
            return this;
        }

        public Builder readPreference(final ReadPreference aReadPreference) {
            this.readPreference = aReadPreference;
            return this;
        }

        public Builder documentSerializer(final Serializer<Document> aDocumentSerializer) {
            this.documentSerializer = aDocumentSerializer;
            return this;
        }

        public MongoDatabaseOptions build() {
            return new MongoDatabaseOptions(primitiveSerializers, writeConcern, readPreference, documentSerializer);
        }

        Builder() {
        }
    }

    MongoDatabaseOptions(final PrimitiveSerializers primitiveSerializers, final WriteConcern writeConcern,
                         final ReadPreference readPreference, final Serializer<Document> documentSerializer) {
        this.primitiveSerializers = primitiveSerializers;
        this.writeConcern = writeConcern;
        this.readPreference = readPreference;
        this.documentSerializer = documentSerializer;
    }

}
