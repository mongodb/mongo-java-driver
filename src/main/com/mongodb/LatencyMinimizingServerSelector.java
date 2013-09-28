package com.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

class LatencyMinimizingServerSelector implements ServerSelector {

    private final long acceptableLatencyDifference;
    private final TimeUnit timeUnit;

    public LatencyMinimizingServerSelector() {
        this(15, TimeUnit.MILLISECONDS);
    }

    public LatencyMinimizingServerSelector(final long acceptableLatencyDifference, final TimeUnit timeUnit) {
        this.acceptableLatencyDifference = acceptableLatencyDifference;
        this.timeUnit = timeUnit;
    }

    @Override
    public List<ServerDescription> choose(final ClusterDescription clusterDescription) {
        return getServersWithAcceptableLatencyDifference(clusterDescription.getAll(), getBestPingTimeNanos(clusterDescription.getAll()));
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
            if (cur.getAveragePingTimeNanos() - TimeUnit.NANOSECONDS.convert(acceptableLatencyDifference, timeUnit) <= bestPingTime) {
                goodSecondaries.add(cur);
            }
        }
        return goodSecondaries;
    }
}
