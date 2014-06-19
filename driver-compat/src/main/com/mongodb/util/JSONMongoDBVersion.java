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

package com.mongodb.util;

/**
 * An enumeration of the supported MongoDB version modes for {@code JSONSerializers}.
 *
 * @see com.mongodb.util.JSONSerializers
 * @since 3.0
 */
enum JSONMongoDBVersion {

    /**
     * Conform to extended JSON and SHELL features present in MongoDB 2.0
     */
    MONGO_2_0,

    /**
     * Conform to extended JSON and SHELL features present in MongoDB 2.2
     */
    MONGO_2_2,

    /**
     * Conform to extended JSON and SHELL features present in MongoDB 2.4
     */
    MONGO_2_4,

    /**
     * Conform to extended JSON and SHELL features present in MongoDB 2.6
     */
    MONGO_2_6
}
