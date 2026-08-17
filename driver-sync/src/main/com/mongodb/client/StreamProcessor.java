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

package com.mongodb.client;

import com.mongodb.annotations.Alpha;
import com.mongodb.annotations.Reason;
import com.mongodb.annotations.Sealed;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.client.model.GetStreamProcessorSamplesOptions;
import com.mongodb.client.model.GetStreamProcessorSamplesResult;
import com.mongodb.client.model.GetStreamProcessorStatsOptions;
import com.mongodb.client.model.StartStreamProcessorOptions;
import org.bson.Document;

/**
 * A handle for a named stream processor in an Atlas Stream Processing workspace.
 *
 * <p>Obtaining this handle does not imply the processor currently exists on the server.
 * Obtain an instance via {@link StreamProcessors#get(String)}.</p>
 *
 * @since 5.5
 */
@Alpha(Reason.CLIENT)
@Sealed
@ThreadSafe
public interface StreamProcessor {

    /**
     * Gets the name of this stream processor.
     *
     * @return the processor name
     */
    String getName();

    /**
     * Starts the stream processor.
     *
     * <p>Sends the {@code startStreamProcessor} command. The processor must be in the
     * {@code STOPPED} or {@code FAILED} state.</p>
     */
    void start();

    /**
     * Starts the stream processor with the given options.
     *
     * <p>Sends the {@code startStreamProcessor} command. The processor must be in the
     * {@code STOPPED} or {@code FAILED} state.</p>
     *
     * @param options the options for starting the processor
     */
    void start(StartStreamProcessorOptions options);

    /**
     * Stops the stream processor.
     *
     * <p>Sends the {@code stopStreamProcessor} command. The processor transitions to the
     * {@code STOPPED} state and can be restarted.</p>
     */
    void stop();

    /**
     * Permanently drops the stream processor.
     *
     * <p>Sends the {@code dropStreamProcessor} command. A dropped processor cannot be recovered.</p>
     */
    void drop();

    /**
     * Returns runtime statistics for the stream processor.
     *
     * <p>Sends the {@code getStreamProcessorStats} command. Returns an error if the processor
     * is not in the {@code STARTED} state.</p>
     *
     * @return a document containing the processor's runtime statistics
     */
    Document stats();

    /**
     * Returns runtime statistics for the stream processor with the given options.
     *
     * <p>Sends the {@code getStreamProcessorStats} command. Returns an error if the processor
     * is not in the {@code STARTED} state.</p>
     *
     * @param options the options for retrieving statistics
     * @return a document containing the processor's runtime statistics
     */
    Document stats(GetStreamProcessorStatsOptions options);

    /**
     * Retrieves a batch of sampled documents from a running stream processor.
     *
     * <p>On the first call, sends {@code startSampleStreamProcessor} to open a new sample cursor.
     * On subsequent calls, sends {@code getMoreSampleStreamProcessor} to fetch the next batch.</p>
     *
     * <p>Callers MUST check {@link GetStreamProcessorSamplesResult#getCursorId()}: a value of
     * {@code 0} means the cursor is exhausted and no further calls should be made.</p>
     *
     * @return the result containing the sampled documents and the cursor ID for the next call
     */
    GetStreamProcessorSamplesResult getStreamProcessorSamples();

    /**
     * Retrieves a batch of sampled documents from a running stream processor with the given options.
     *
     * <p>If {@link GetStreamProcessorSamplesOptions#getCursorId()} is absent or zero, sends
     * {@code startSampleStreamProcessor} to open a new sample cursor. Otherwise, sends
     * {@code getMoreSampleStreamProcessor} using the provided cursor ID.</p>
     *
     * <p>Callers MUST check {@link GetStreamProcessorSamplesResult#getCursorId()}: a value of
     * {@code 0} means the cursor is exhausted and no further calls should be made.</p>
     *
     * @param options the options controlling cursor ID, limit, and batch size
     * @return the result containing the sampled documents and the cursor ID for the next call
     */
    GetStreamProcessorSamplesResult getStreamProcessorSamples(GetStreamProcessorSamplesOptions options);
}
