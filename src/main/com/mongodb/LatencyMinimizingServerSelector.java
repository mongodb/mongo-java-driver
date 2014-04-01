/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

class LatencyMinimizingServerSelector implements ServerSelector {

    private final long acceptableLatencyDifferenceNanos;

    LatencyMinimizingServerSelector(final long acceptableLatencyDifference, final TimeUnit timeUnit) {
        this.acceptableLatencyDifferenceNanos = NANOSECONDS.convert(acceptableLatencyDifference, timeUnit);
    }

    @Override
    public List<ServerDescription> choose(final ClusterDescription clusterDescription) {
        return getServersWithAcceptableLatencyDifference(clusterDescription.getAll(), getBestPingTimeNanos(clusterDescription.getAll()));
    }

    @Override
    public String toString() {
        return "LatencyMinimizingServerSelector{"
               + "acceptableLatencyDifference=" + MILLISECONDS.convert(acceptableLatencyDifferenceNanos, NANOSECONDS) + " ms"
               + '}';
    }

    private long getBestPingTimeNanos(final Set<ServerDescription> members) {
        long bestPingTime = Long.MAX_VALUE;
        for (final ServerDescription cur : members) {
            if (!cur.isOk()) {
                continue;
            }
            if (cur.getAveragePingTimeNanos() < bestPingTime) {
                bestPingTime = cur.getAveragePingTimeNanos();
            }
        }
        return bestPingTime;
    }

    private List<ServerDescription> getServersWithAcceptableLatencyDifference(final Set<ServerDescription> servers,
                                                                              final long bestPingTime) {
        final List<ServerDescription> goodSecondaries = new ArrayList<ServerDescription>(servers.size());
        for (final ServerDescription cur : servers) {
            if (!cur.isOk()) {
                continue;
            }
            if (cur.getAveragePingTimeNanos() - acceptableLatencyDifferenceNanos <= bestPingTime) {
                goodSecondaries.add(cur);
            }
        }
        return goodSecondaries;
    }
}
