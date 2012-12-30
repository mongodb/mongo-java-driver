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
 *
 */

package org.mongodb;

import org.mongodb.serialization.PrimitiveSerializers;

public class MongoDatabaseOptions {
    final PrimitiveSerializers primitiveSerializers;
    final WriteConcern writeConcern;
    final ReadPreference readPreference;

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

    public MongoDatabaseOptions withDefaults(final MongoClientOptions options) {
        Builder builder = new Builder();
        builder.primitiveSerializers = primitiveSerializers != null ? primitiveSerializers : options.getPrimitiveSerializers();
        builder.writeConcern = writeConcern != null ? writeConcern : options.getWriteConcern();
        builder.readPreference = readPreference != null ? readPreference : options.getReadPreference();
        return builder.build();
    }

    public static class Builder {
        PrimitiveSerializers primitiveSerializers;
        WriteConcern writeConcern;
        ReadPreference readPreference;

        public Builder primitiveSerializers(PrimitiveSerializers primitiveSerializers) {
            this.primitiveSerializers = primitiveSerializers;
            return this;
        }

        public Builder writeConcern(WriteConcern writeConcern) {
            this.writeConcern = writeConcern;
            return this;
        }

        public Builder readPreference(ReadPreference readPreference) {
            this.readPreference = readPreference;
            return this;
        }

        public MongoDatabaseOptions build() {
            return new MongoDatabaseOptions(primitiveSerializers, writeConcern, readPreference);
        }
    }

    MongoDatabaseOptions(final PrimitiveSerializers primitiveSerializers, final WriteConcern writeConcern,
                         final ReadPreference readPreference) {
        this.primitiveSerializers = primitiveSerializers;
        this.writeConcern = writeConcern;
        this.readPreference = readPreference;
    }


}
