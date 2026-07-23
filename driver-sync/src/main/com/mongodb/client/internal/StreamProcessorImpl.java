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
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.client.StreamProcessor;
import com.mongodb.client.model.FailoverOptions;
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

import static com.mongodb.assertions.Assertions.notNull;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
@ThreadSafe
public final class StreamProcessorImpl implements StreamProcessor {
    private final String name;
    private final OperationExecutor executor;
    private final boolean retryReads;

    StreamProcessorImpl(final String name, final OperationExecutor executor, final boolean retryReads) {
        this.name = notNull("name", name);
        this.executor = notNull("executor", executor);
        this.retryReads = retryReads;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void start() {
        executor.execute(
                new StartStreamProcessorOperation(name, null, null, null, null, null, null, null, null),
                ReadConcern.DEFAULT);
    }

    @Override
    public void start(final StartStreamProcessorOptions options) {
        notNull("options", options);
        FailoverOptions failover = options.getFailover();
        String failoverRegion = failover != null ? failover.getRegion() : null;
        String failoverMode = failover != null ? failover.getMode() : null;
        Boolean failoverDryRun = failover != null ? failover.getDryRun() : null;
        executor.execute(
                new StartStreamProcessorOperation(name, options.getWorkers(), options.getClearCheckpoints(),
                        options.getStartAtOperationTime(), options.getTier(), options.getEnableAutoScaling(),
                        failoverRegion, failoverMode, failoverDryRun),
                ReadConcern.DEFAULT);
    }

    @Override
    public void stop() {
        executor.execute(new StopStreamProcessorOperation(name), ReadConcern.DEFAULT);
    }

    @Override
    public void drop() {
        executor.execute(new DropStreamProcessorOperation(name), ReadConcern.DEFAULT);
    }

    @Override
    public Document stats() {
        return executor.execute(
                new GetStreamProcessorStatsOperation(name, retryReads, null),
                ReadPreference.primary(), ReadConcern.DEFAULT);
    }

    @Override
    public Document stats(final GetStreamProcessorStatsOptions options) {
        notNull("options", options);
        return executor.execute(
                new GetStreamProcessorStatsOperation(name, retryReads, options.getVerbose()),
                ReadPreference.primary(), ReadConcern.DEFAULT);
    }

    @Override
    public GetStreamProcessorSamplesResult getStreamProcessorSamples() {
        return getStreamProcessorSamples(new GetStreamProcessorSamplesOptions());
    }

    @Override
    public GetStreamProcessorSamplesResult getStreamProcessorSamples(final GetStreamProcessorSamplesOptions options) {
        notNull("options", options);
        Long cursorId = options.getCursorId();
        if (cursorId == null || cursorId == 0L) {
            long openedCursorId = executor.execute(
                    new StartSampleStreamProcessorOperation(name, options.getLimit()),
                    ReadConcern.DEFAULT);
            return executor.execute(
                    new GetMoreSampleStreamProcessorOperation(name, openedCursorId, null),
                    ReadConcern.DEFAULT);
        } else {
            return executor.execute(
                    new GetMoreSampleStreamProcessorOperation(name, cursorId, options.getBatchSize()),
                    ReadConcern.DEFAULT);
        }
    }
}
