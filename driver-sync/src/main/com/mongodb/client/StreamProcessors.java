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
import com.mongodb.client.model.CreateStreamProcessorOptions;
import com.mongodb.client.model.StreamProcessorInfo;
import org.bson.conversions.Bson;

import java.util.List;

/**
 * A handle for managing stream processors in an Atlas Stream Processing workspace.
 *
 * <p>Obtain an instance via {@link StreamProcessingClient#streamProcessors()}.</p>
 *
 * @since 5.5
 */
@Alpha(Reason.CLIENT)
@Sealed
@ThreadSafe
public interface StreamProcessors {

    /**
     * Creates a new stream processor with the given name and pipeline.
     *
     * <p>Sends the {@code createStreamProcessor} command.</p>
     *
     * @param name     the name of the stream processor
     * @param pipeline the aggregation pipeline defining the processor's logic
     */
    void create(String name, List<? extends Bson> pipeline);

    /**
     * Creates a new stream processor with the given name, pipeline, and options.
     *
     * <p>Sends the {@code createStreamProcessor} command.</p>
     *
     * @param name     the name of the stream processor
     * @param pipeline the aggregation pipeline defining the processor's logic
     * @param options  the options for creating the processor
     */
    void create(String name, List<? extends Bson> pipeline, CreateStreamProcessorOptions options);

    /**
     * Returns a handle for an existing stream processor by name.
     *
     * <p>This method does not send any commands to the server; the returned handle is
     * a local reference. Use it to call {@link StreamProcessor#start()},
     * {@link StreamProcessor#stop()}, {@link StreamProcessor#drop()}, etc.</p>
     *
     * @param name the name of the stream processor
     * @return a {@link StreamProcessor} handle for the named processor
     */
    StreamProcessor get(String name);

    /**
     * Returns information about a single stream processor.
     *
     * <p>Sends the {@code getStreamProcessor} command.</p>
     *
     * @param name the name of the stream processor
     * @return information about the named processor
     */
    StreamProcessorInfo getInfo(String name);
}
