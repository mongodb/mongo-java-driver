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

package com.mongodb.connection;

import com.mongodb.annotations.ThreadSafe;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.types.ObjectId;

/**
 * The topology version of a cluster.
 *
 * @since 4.1
 * @mongodb.server.release 4.4
 */
@ThreadSafe
public final class TopologyVersion {
    private final ObjectId processId;
    private final long counter;

    /**
     * Construct a new instance from a document description
     *
     * @param topologyVersionDocument a document description of the topology version
     */
    public TopologyVersion(final BsonDocument topologyVersionDocument) {
        processId = topologyVersionDocument.getObjectId("processId").getValue();
        counter = topologyVersionDocument.getInt64("counter").getValue();
    }

    /**
     * Construct a new instance from process identifier and counter
     *
     * @param processId the process identifer
     * @param counter   the counter
     */
    public TopologyVersion(final ObjectId processId, final long counter) {
        this.processId = processId;
        this.counter = counter;
    }

    /**
     * Get the process identifier
     *
     * @return the process identifier
     */
    public ObjectId getProcessId() {
        return processId;
    }

    /**
     * Get the counter
     *
     * @return the counter
     */
    public long getCounter() {
        return counter;
    }

    /**
     * Get the document representation of the topology version
     *
     * @return the document representation of the topology version
     */
    public BsonDocument asDocument() {
        return new BsonDocument("processId", new BsonObjectId(processId))
                .append("counter", new BsonInt64(counter));

    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TopologyVersion that = (TopologyVersion) o;

        if (counter != that.counter) {
            return false;
        }
        return processId.equals(that.processId);
    }

    @Override
    public int hashCode() {
        int result = processId.hashCode();
        result = 31 * result + (int) (counter ^ (counter >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "TopologyVersion{"
                + "processId=" + processId
                + ", counter=" + counter
                + '}';
    }
}
