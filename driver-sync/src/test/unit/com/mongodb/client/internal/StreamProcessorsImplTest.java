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
import com.mongodb.client.model.CreateStreamProcessorOptions;
import com.mongodb.client.model.StreamProcessorInfo;
import com.mongodb.internal.operation.CreateStreamProcessorOperation;
import com.mongodb.internal.operation.GetStreamProcessorOperation;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class StreamProcessorsImplTest {
    private static final String PROCESSOR_NAME = "myProcessor";
    private static final List<Bson> PIPELINE = singletonList(
            new BsonDocument("$source", new BsonString("test")));

    @Test
    void createDispatchesCreateOperation() {
        TestOperationExecutor executor = new TestOperationExecutor(new ArrayList<>(Collections.singletonList(null)));
        new StreamProcessorsImpl(executor, true).create(PROCESSOR_NAME, PIPELINE);
        assertInstanceOf(CreateStreamProcessorOperation.class, executor.getWriteOperation());
        assertEquals(ReadConcern.DEFAULT, executor.getReadConcern());
    }

    @Test
    void createWithOptionsDispatchesCreateOperation() {
        TestOperationExecutor executor = new TestOperationExecutor(new ArrayList<>(Collections.singletonList(null)));
        new StreamProcessorsImpl(executor, true).create(PROCESSOR_NAME, PIPELINE,
                new CreateStreamProcessorOptions().tier("SP10"));
        assertInstanceOf(CreateStreamProcessorOperation.class, executor.getWriteOperation());
    }

    @Test
    void getReturnsStreamProcessorImpl() {
        TestOperationExecutor executor = new TestOperationExecutor(new ArrayList<>());
        assertInstanceOf(StreamProcessorImpl.class,
                new StreamProcessorsImpl(executor, true).get(PROCESSOR_NAME));
    }

    @Test
    void getInfoDispatchesGetStreamProcessorOperation() {
        StreamProcessorInfo info = minimalStreamProcessorInfo();
        TestOperationExecutor executor = new TestOperationExecutor(new ArrayList<>(Collections.singletonList(info)));
        StreamProcessorInfo result = new StreamProcessorsImpl(executor, true).getInfo(PROCESSOR_NAME);
        assertEquals(info, result);
        assertInstanceOf(GetStreamProcessorOperation.class, executor.getReadOperation());
        assertEquals(ReadPreference.primary(), executor.getReadPreference());
        assertEquals(ReadConcern.DEFAULT, executor.getReadConcern());
    }

    private static StreamProcessorInfo minimalStreamProcessorInfo() {
        return StreamProcessorInfo.builder()
                .id("id-1")
                .name(PROCESSOR_NAME)
                .state("CREATED")
                .pipeline(emptyList())
                .pipelineVersion(1)
                .enableAutoScaling(false)
                .failoverEnabled(false)
                .activeRegion("US_EAST_1")
                .workspaceDefaultRegion("US_EAST_1")
                .modifiedBy("user@example.com")
                .hasStarted(false)
                .errorMsg("")
                .errorRetryable(false)
                .build();
    }
}
