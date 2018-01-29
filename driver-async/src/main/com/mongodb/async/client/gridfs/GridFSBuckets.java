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

package com.mongodb.async.client.gridfs;

import com.mongodb.async.client.MongoDatabase;

/**
 * A factory for GridFSBucket instances.
 *
 * @since 3.3
 */
public final class GridFSBuckets {

    /**
     * Create a new GridFS bucket with the default {@code 'fs'} bucket name
     *
     * @param database the database instance to use with GridFS
     * @return the GridFSBucket
     */
    public static GridFSBucket create(final MongoDatabase database) {
        return new GridFSBucketImpl(database);
    }

    /**
     * Create a new GridFS bucket with a custom bucket name
     *
     * @param database   the database instance to use with GridFS
     * @param bucketName the custom bucket name to use
     * @return the GridFSBucket
     */
    public static GridFSBucket create(final MongoDatabase database, final String bucketName) {
        return new GridFSBucketImpl(database, bucketName);
    }

    private GridFSBuckets() {
    }
}

