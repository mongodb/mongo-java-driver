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

package com.mongodb.connection;

import org.bson.types.ObjectId;

/**
 * A client-generated identifier that uniquely identifies a connection to a MongoDB cluster, which could be sharded, replica set,
 * or standalone.
 *
 * @since 3.0
 */
public final class ClusterId {
    private final String value;

    /**
     * Construct an instance.
     *
     */
    public ClusterId() {
        this.value = new ObjectId().toHexString();
    }

    /**
     * Gets the value of the identifier.
     *
     * @return the value
     */
    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }
}
