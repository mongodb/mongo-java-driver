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

package com.mongodb;

import java.util.List;
import java.util.concurrent.TimeUnit;

class ReadPreferenceServerSelector extends ChainingServerSelector {
    private final ReadPreference readPreference;

    public ReadPreferenceServerSelector(final ReadPreference readPreference) {
        super(new LatencyMinimizingServerSelector());
        this.readPreference = readPreference;
    }

    public ReadPreferenceServerSelector(final ReadPreference readPreference, final int acceptableLatency, final TimeUnit timeUnit) {
        super(new LatencyMinimizingServerSelector(acceptableLatency, timeUnit));
        this.readPreference = readPreference;
    }

    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    protected List<ServerDescription> chooseStep(final ClusterDescription clusterDescription) {
        return readPreference.choose(clusterDescription);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ReadPreferenceServerSelector that = (ReadPreferenceServerSelector) o;

        if (!readPreference.equals(that.readPreference)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return readPreference.hashCode();
    }

    @Override
    public String toString() {
        return "ReadPreferenceServerSelector{"
               + "readPreference=" + readPreference
               + '}';
    }
}