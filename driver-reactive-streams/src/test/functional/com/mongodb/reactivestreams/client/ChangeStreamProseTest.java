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
package com.mongodb.reactivestreams.client;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.reactivestreams.client.internal.BatchCursorPublisher;
import com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.disableWaitForBatchCursorCreation;
import static com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient.enableWaitForBatchCursorCreation;

/**
 * Many of the tests from {@link com.mongodb.client.ChangeStreamProseTest} fail when run using {@link SyncMongoClient},
 * because it does not have all the necessary implementations. Such tests are disabled.
 */
final class ChangeStreamProseTest extends com.mongodb.client.ChangeStreamProseTest {
    @BeforeEach
    void beforeEach() {
        enableWaitForBatchCursorCreation();
    }

    @AfterEach
    void afterEach() {
        disableWaitForBatchCursorCreation();
    }

    @Override
    protected MongoClient createClient(final MongoClientSettings settings) {
        return new SyncMongoClient(settings);
    }

    @Test
    @Disabled
    @Override
    protected void test07CursorNotClosed() {
    }

    @Test
    @Disabled
    @Override
    protected void test11GetResumeTokenReturnsPostBatchResumeToken() {
    }

    @Test
    @Disabled
    @Override
    protected void test13GetResumeTokenEqualsIdOfPreviousDocument() {
    }

    @Test
    @Disabled
    @Override
    protected void test14GetResumeTokenReturnsStartAfter() {
    }

    @Test
    @Disabled
    @Override
    protected void test14GetResumeTokenReturnsResumeAfter() {
    }

    @Test
    @Disabled
    @Override
    protected void test15GetResumeTokenReturnsPostBatchResumeTokenAfterGetMore() {
    }

    @Test
    @Disabled
    @Override
    protected void test19SplitChangeStreamEvents() {
    }

    @Test
    @Disabled
    @Override
    protected void testNameSpaceTypeAbsentChangeStreamEvents() {
    }

    /**
     * Not a prose spec test.
     * <p>
     * {@link ChangeStreamPublisher#withDocumentClass(Class)} returns a new {@link BatchCursorPublisher},
     * which must handle batch size correctly.
     */
    @Test
    @Override
    protected void batchSizeWorksForWithDocumentClass() {
        super.batchSizeWorksForWithDocumentClass();
    }
}
