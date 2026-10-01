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

package com.mongodb.client.gridfs;

import com.mongodb.MongoGridFSException;
import com.mongodb.client.Fixture;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import org.bson.BsonDocument;
import org.bson.BsonMinKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * See
 * <a href="https://github.com/mongodb/specifications/blob/master/source/gridfs/tests/README.md">GridFS prose tests</a>.
 */
public class GridFSProseTest {

    private GridFSBucket bucket;

    @BeforeEach
    public void setUp() {
        MongoDatabase database = Fixture.getDefaultDatabase();
        bucket = GridFSBuckets.create(database);
        bucket.drop();
    }

    @AfterEach
    public void tearDown() {
        if (bucket != null) {
            bucket.drop();
        }
    }

    /**
     * Prose test 1: Aborting an upload with an injected file ID does not delete other files' chunks.
     */
    @Test
    public void shouldNotDeleteOtherFilesChunksWhenAbortingUploadWithInjectedFileId() {
        // Document values with "$"-prefixed keys are not supported by older servers.
        assumeTrue(serverVersionAtLeast(5, 0));

        byte[] file1Bytes = {0x11, 0x22};
        bucket.uploadFromStream("file1", new ByteArrayInputStream(file1Bytes));

        BsonDocument injectedId = new BsonDocument("$gt", new BsonMinKey());
        GridFSUploadStream uploadStream = bucket.openUploadStream(injectedId, "file2",
                new GridFSUploadOptions().chunkSizeBytes(2));
        uploadStream.write(new byte[] {0x33, 0x44, 0x55, 0x66});
        uploadStream.abort();

        ByteArrayOutputStream file1Download = new ByteArrayOutputStream();
        bucket.downloadToStream("file1", file1Download);
        assertArrayEquals(file1Bytes, file1Download.toByteArray());

        assertThrows(MongoGridFSException.class, () -> bucket.downloadToStream("file2", new ByteArrayOutputStream()));
    }
}
