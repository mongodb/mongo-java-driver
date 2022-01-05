/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.bson.codecs.pojo;

import org.bson.BsonObjectId;
import org.bson.types.ObjectId;

/**
 * The default IdGenerators
 *
 * @see IdGenerator
 * @since 3.10
 */
public final class IdGenerators {

    /**
     * A IdGenerator for {@code ObjectId}
     */
    public static final IdGenerator<ObjectId> OBJECT_ID_GENERATOR = new IdGenerator<ObjectId>() {

        @Override
        public ObjectId generate() {
            return new ObjectId();
        }

        @Override
        public Class<ObjectId> getType() {
            return ObjectId.class;
        }
    };

    /**
     * A IdGenerator for {@code BsonObjectId}
     */
    public static final IdGenerator<BsonObjectId> BSON_OBJECT_ID_GENERATOR = new IdGenerator<BsonObjectId>() {

        @Override
        public BsonObjectId generate() {
            return new BsonObjectId();
        }

        @Override
        public Class<BsonObjectId> getType() {
            return BsonObjectId.class;
        }
    };

    /**
     * A IdGenerator for {@code String}
     */
    public static final IdGenerator<String> STRING_ID_GENERATOR = new IdGenerator<String>() {
        @Override
        public String generate() {
            return OBJECT_ID_GENERATOR.generate().toHexString();
        }

        @Override
        public Class<String> getType() {
            return String.class;
        }
    };

    private IdGenerators(){
    }
}
