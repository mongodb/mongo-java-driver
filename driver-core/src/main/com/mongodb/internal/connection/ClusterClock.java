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

package com.mongodb.internal.connection;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import java.util.concurrent.locks.ReentrantLock;

import static com.mongodb.internal.Locks.runWithLock;
import static com.mongodb.internal.Locks.supplyWithLock;

public class ClusterClock {
    private static final String CLUSTER_TIME_KEY = "clusterTime";
    private final ReentrantLock lock = new ReentrantLock();
    private BsonDocument clusterTime;

    public BsonDocument getCurrent() {
        return supplyWithLock(lock, () -> clusterTime);
    }

    public BsonTimestamp getClusterTime() {
        return supplyWithLock(lock, () -> clusterTime != null ? clusterTime.getTimestamp(CLUSTER_TIME_KEY) : null);
    }

    public void advance(final BsonDocument other) {
        runWithLock(lock, () -> this.clusterTime = greaterOf(other));
    }

    public BsonDocument greaterOf(final BsonDocument other) {
        return supplyWithLock(lock, () -> {
            if (other == null) {
                return clusterTime;
            } else if (clusterTime == null) {
                return other;
            } else {
                return other.getTimestamp(CLUSTER_TIME_KEY).compareTo(clusterTime.getTimestamp(CLUSTER_TIME_KEY)) > 0 ? other : clusterTime;
            }
        });
    }
}
