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

package com.mongodb.client.internal;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.model.GetStreamProcessorSamplesOptions;
import com.mongodb.client.model.GetStreamProcessorSamplesResult;
import com.mongodb.client.model.GetStreamProcessorStatsOptions;
import com.mongodb.client.model.StartStreamProcessorOptions;
import com.mongodb.internal.operation.DropStreamProcessorOperation;
import com.mongodb.internal.operation.GetMoreSampleStreamProcessorOperation;
import com.mongodb.internal.operation.GetStreamProcessorStatsOperation;
import com.mongodb.internal.operation.StartSampleStreamProcessorOperation;
import com.mongodb.internal.operation.StartStreamProcessorOperation;
import com.mongodb.internal.operation.StopStreamProcessorOperation;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

class StreamProcessorImplTest {
    private static final String PROCESSOR_NAME = "myProcessor";

    @Test
    void getName() {
        StreamProcessorImpl proc = new StreamProcessorImpl(PROCESSOR_NAME,
                new TestOperationExecutor(new ArrayList<>()), true);
        assertEquals(PROCESSOR_NAME, proc.getName());
    }

    @Test
    void startWithNoOptionsDispatchesStartOperation() {
        TestOperationExecutor executor = new TestOperationExecutor(new ArrayList<>(Collections.singletonList(null)));
        new StreamProcessorImpl(PROCESSOR_NAME, executor, true).start();
        assertInstanceOf(StartStreamProcessorOperation.class, executor.getWriteOperation());
    }

    @Test
    void startWithOptionsDispatchesStartOperation() {
        TestOperationExecutor executor = new TestOperationExecutor(new ArrayList<>(Collections.singletonList(null)));
        new StreamProcessorImpl(PROCESSOR_NAME, executor, true)
                .start(new StartStreamProcessorOptions());
        assertInstanceOf(StartStreamProcessorOperation.class, executor.getWriteOperation());
        assertEquals(ReadConcern.DEFAULT, executor.getReadConcern());
    }

    @Test
    void stopDispatchesStopOperation() {
        TestOperationExecutor executor = new TestOperationExecutor(new ArrayList<>(Collections.singletonList(null)));
        new StreamProcessorImpl(PROCESSOR_NAME, executor, true).stop();
        assertInstanceOf(StopStreamProcessorOperation.class, executor.getWriteOperation());
    }

    @Test
    void dropDispatchesDropOperation() {
        TestOperationExecutor executor = new TestOperationExecutor(new ArrayList<>(Collections.singletonList(null)));
        new StreamProcessorImpl(PROCESSOR_NAME, executor, true).drop();
        assertInstanceOf(DropStreamProcessorOperation.class, executor.getWriteOperation());
    }

    @Test
    void statsDispatchesGetStatsOperation() {
        Document statsDoc = new Document("ok", 1);
        TestOperationExecutor executor = new TestOperationExecutor(new ArrayList<>(Collections.singletonList(statsDoc)));
        Document result = new StreamProcessorImpl(PROCESSOR_NAME, executor, true).stats();
        assertEquals(statsDoc, result);
        assertInstanceOf(GetStreamProcessorStatsOperation.class, executor.getReadOperation());
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
        assertEquals(ReadConcern.DEFAULT, executor.getReadConcern());
    }

    @Test
    void statsWithOptionsDispatchesGetStatsOperation() {
        Document statsDoc = new Document("ok", 1);
        TestOperationExecutor executor = new TestOperationExecutor(new ArrayList<>(Collections.singletonList(statsDoc)));
        new StreamProcessorImpl(PROCESSOR_NAME, executor, true)
                .stats(new GetStreamProcessorStatsOptions().verbose(true));
        assertInstanceOf(GetStreamProcessorStatsOperation.class, executor.getReadOperation());
    }

    @Test
    void getStreamProcessorSamplesInitialCallExecutesStartThenGetMore() {
        GetStreamProcessorSamplesResult samplesResult = new GetStreamProcessorSamplesResult(0L, emptyList());
        TestOperationExecutor executor = new TestOperationExecutor(new ArrayList<>(Arrays.asList(42L, samplesResult)));
        GetStreamProcessorSamplesResult result =
                new StreamProcessorImpl(PROCESSOR_NAME, executor, true).getStreamProcessorSamples();
        assertEquals(samplesResult, result);
        assertInstanceOf(StartSampleStreamProcessorOperation.class, executor.getWriteOperation());
        assertInstanceOf(GetMoreSampleStreamProcessorOperation.class, executor.getWriteOperation());
    }

    @Test
    void getStreamProcessorSamplesWithNullCursorIdExecutesStartThenGetMore() {
        GetStreamProcessorSamplesResult samplesResult = new GetStreamProcessorSamplesResult(0L, emptyList());
        TestOperationExecutor executor = new TestOperationExecutor(new ArrayList<>(Arrays.asList(42L, samplesResult)));
        new StreamProcessorImpl(PROCESSOR_NAME, executor, true)
                .getStreamProcessorSamples(new GetStreamProcessorSamplesOptions());
        assertInstanceOf(StartSampleStreamProcessorOperation.class, executor.getWriteOperation());
        assertInstanceOf(GetMoreSampleStreamProcessorOperation.class, executor.getWriteOperation());
    }

    @Test
    void getStreamProcessorSamplesWithZeroCursorIdExecutesStartThenGetMore() {
        GetStreamProcessorSamplesResult samplesResult = new GetStreamProcessorSamplesResult(0L, emptyList());
        TestOperationExecutor executor = new TestOperationExecutor(new ArrayList<>(Arrays.asList(42L, samplesResult)));
        new StreamProcessorImpl(PROCESSOR_NAME, executor, true)
                .getStreamProcessorSamples(new GetStreamProcessorSamplesOptions().cursorId(0L));
        assertInstanceOf(StartSampleStreamProcessorOperation.class, executor.getWriteOperation());
        assertInstanceOf(GetMoreSampleStreamProcessorOperation.class, executor.getWriteOperation());
    }

    @Test
    void getStreamProcessorSamplesWithExistingCursorIdExecutesGetMoreOnly() {
        GetStreamProcessorSamplesResult samplesResult = new GetStreamProcessorSamplesResult(0L, emptyList());
        TestOperationExecutor executor = new TestOperationExecutor(new ArrayList<>(Collections.singletonList(samplesResult)));
        new StreamProcessorImpl(PROCESSOR_NAME, executor, true)
                .getStreamProcessorSamples(new GetStreamProcessorSamplesOptions().cursorId(42L));
        assertInstanceOf(GetMoreSampleStreamProcessorOperation.class, executor.getWriteOperation());
        assertNull(executor.getWriteOperation());
    }
}
