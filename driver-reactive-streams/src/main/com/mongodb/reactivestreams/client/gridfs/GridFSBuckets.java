/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client.gridfs;

import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.internal.GridFSBucketImpl;
import com.mongodb.reactivestreams.client.internal.MongoDatabaseImpl;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A factory for GridFSBucket instances.
 *
 * <p>Requires the concrete {@link MongoDatabaseImpl} implementation of the MongoDatabase interface.</p>
 *
 * @since 1.3
 */
public final class GridFSBuckets {

    /**
     * Create a new GridFS bucket with the default {@code 'fs'} bucket name
     *
     * <p>Requires the concrete {@link MongoDatabaseImpl} implementation of the MongoDatabase interface.</p>
     *
     * @param database the database instance to use with GridFS.
     * @return the GridFSBucket
     */
    public static GridFSBucket create(final MongoDatabase database) {
        notNull("database", database);
        if (database instanceof MongoDatabaseImpl) {
            return new GridFSBucketImpl(com.mongodb.internal.async.client.gridfs.GridFSBuckets.create(((MongoDatabaseImpl) database)
                    .getWrapped()));
        } else {
            throw new IllegalArgumentException("GridFS requires the concrete MongoDatabaseImpl implementation.");
        }
    }

    /**
     * Create a new GridFS bucket with a custom bucket name
     *
     * <p>Requires the concrete {@link MongoDatabaseImpl} implementation of the MongoDatabase interface.</p>
     *
     * @param database   the database instance to use with GridFS
     * @param bucketName the custom bucket name to use
     * @return the GridFSBucket
     */
    public static GridFSBucket create(final MongoDatabase database, final String bucketName) {
        notNull("database", database);
        notNull("bucketName", bucketName);
        if (database instanceof MongoDatabaseImpl) {
            return new GridFSBucketImpl(com.mongodb.internal.async.client.gridfs.GridFSBuckets.create(((MongoDatabaseImpl) database)
                            .getWrapped(), bucketName));
        } else {
            throw new IllegalArgumentException("GridFS requires the concrete MongoDatabaseImpl implementation.");
        }
    }

    private GridFSBuckets() {
    }
}

