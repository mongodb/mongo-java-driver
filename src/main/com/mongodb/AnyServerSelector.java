package com.mongodb;

import java.util.List;

class AnyServerSelector implements ServerSelector {
    @Override
    public List<ServerDescription> choose(final ClusterDescription clusterDescription) {
        return clusterDescription.getAny();
    }

    @Override
    public String toString() {
        return "AnyServerSelector{}";
    }
}
