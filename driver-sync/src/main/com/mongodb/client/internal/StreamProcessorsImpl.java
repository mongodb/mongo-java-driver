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

import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.client.StreamProcessor;
import com.mongodb.client.StreamProcessors;
import com.mongodb.client.model.CreateStreamProcessorOptions;
import com.mongodb.client.model.StreamProcessorInfo;
import com.mongodb.internal.operation.CreateStreamProcessorOperation;
import com.mongodb.internal.operation.GetStreamProcessorOperation;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
@ThreadSafe
public final class StreamProcessorsImpl implements StreamProcessors {
    private final OperationExecutor executor;
    private final boolean retryReads;

    StreamProcessorsImpl(final OperationExecutor executor, final boolean retryReads) {
        this.executor = notNull("executor", executor);
        this.retryReads = retryReads;
    }

    @Override
    public void create(final String name, final List<? extends Bson> pipeline) {
        create(name, pipeline, new CreateStreamProcessorOptions());
    }

    @Override
    public void create(final String name, final List<? extends Bson> pipeline, final CreateStreamProcessorOptions options) {
        notNull("name", name);
        notNull("pipeline", pipeline);
        notNull("options", options);
        Bson dlq = options.getDlq();
        BsonDocument dlqDoc = dlq != null
                ? dlq.toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry())
                : null;
        executor.execute(
                new CreateStreamProcessorOperation(name, toBsonDocumentList(pipeline), dlqDoc,
                        options.getStreamMetaFieldName(), options.getTier(), options.getFailover()),
                ReadConcern.DEFAULT);
    }

    @Override
    public StreamProcessor get(final String name) {
        notNull("name", name);
        return new StreamProcessorImpl(name, executor, retryReads);
    }

    @Override
    public StreamProcessorInfo getInfo(final String name) {
        notNull("name", name);
        return executor.execute(
                new GetStreamProcessorOperation(name, retryReads),
                ReadPreference.primary(), ReadConcern.DEFAULT);
    }

    private static List<BsonDocument> toBsonDocumentList(final List<? extends Bson> pipeline) {
        List<BsonDocument> result = new ArrayList<>(pipeline.size());
        for (Bson stage : pipeline) {
            notNull("pipeline stage", stage);
            result.add(stage.toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry()));
        }
        return result;
    }
}
