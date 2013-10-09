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

package org.mongodb.operation;

import org.mongodb.ReadPreference;
import org.mongodb.connection.ClusterDescription;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.ServerSelector;
import org.mongodb.session.ChainingServerSelector;
import org.mongodb.session.LatencyMinimizingServerSelector;

import java.util.List;

public class ReadPreferenceServerSelector extends ChainingServerSelector {
    private final ReadPreference readPreference;

    public ReadPreferenceServerSelector(final ReadPreference readPreference) {
        this(readPreference, new LatencyMinimizingServerSelector());
    }

    public ReadPreferenceServerSelector(final ReadPreference readPreference, final ServerSelector chainedSelector) {
        super(chainedSelector);
        // TODO: this is hiding bugs:
        // notNull("readPreference", readPreference);
        this.readPreference = readPreference == null ? ReadPreference.primary() : readPreference;
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

        ReadPreferenceServerSelector that = (ReadPreferenceServerSelector) o;

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
