/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.connection;

import java.util.List;

public abstract class ChainingServerSelector implements ServerSelector {
    private final ServerSelector chainedSelector;

    protected ChainingServerSelector(final ServerSelector chainedSelector) {
        this.chainedSelector = chainedSelector;
    }

    @Override
    public final List<ServerDescription> choose(final ClusterDescription clusterDescription) {
        List<ServerDescription> serverDescriptions = chooseStep(clusterDescription);
        if (chainedSelector == null) {
            return serverDescriptions;
        }

        return chainedSelector.choose(new ClusterDescription(clusterDescription.getConnectionMode(), clusterDescription.getType(),
                serverDescriptions));
    }

    protected abstract List<ServerDescription> chooseStep(final ClusterDescription clusterDescription);
}
