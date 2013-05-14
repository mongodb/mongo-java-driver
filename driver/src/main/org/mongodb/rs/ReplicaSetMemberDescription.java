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

package org.mongodb.rs;

import org.mongodb.Document;
import org.mongodb.ServerAddress;
import org.mongodb.ServerDescription;
import org.mongodb.annotations.Immutable;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the state of a node in the replica set.  Instances of this class are immutable.
 * <p/>
 * NOT PART OF PUBLIC API YET
 */
@Immutable
public class ReplicaSetMemberDescription {
    private final ServerAddress serverAddress;
    private final ServerDescription serverDescription;
    private final float normalizedPingTime;

    public ReplicaSetMemberDescription(final ServerAddress serverAddress, final ServerDescription serverDescription,
                                       final float latencySmoothFactor, final ReplicaSetMemberDescription previous) {
        this.serverAddress = serverAddress;
        this.serverDescription = serverDescription;
        this.normalizedPingTime = previous == null || !previous.getServerDescription().isOk()
                ? serverDescription.getElapsedMillis()
                : previous.getNormalizedPingTime()
                + ((serverDescription.getElapsedMillis() - previous.getNormalizedPingTime()) / latencySmoothFactor);
    }

    public ServerDescription getServerDescription() {
        return serverDescription;
    }

    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    public float getNormalizedPingTime() {
        return normalizedPingTime;
    }

    public String toJSON() {
        final StringBuilder buf = new StringBuilder();
        buf.append("{ address:'").append(getServerAddress()).append("', ");
        buf.append("ok:").append(getServerDescription().isOk()).append(", ");
        buf.append("ping:").append(getNormalizedPingTime()).append(", ");
        buf.append("isPrimary:").append(getServerDescription().isPrimary()).append(", ");
        buf.append("isSecondary:").append(getServerDescription().isSecondary()).append(", ");
        buf.append("setName:").append(getServerDescription().getSetName()).append(", ");
        buf.append("maxBSONObjectSize:").append(getServerDescription().getMaxBSONObjectSize()).append(", ");
        if (!serverDescription.getTags().isEmpty()) {
            final List<Document> tagObjects = new ArrayList<Document>();
            for (final Tag tag : serverDescription.getTags()) {
                tagObjects.add(tag.toDBObject());
            }

            buf.append(new Document("tags", tagObjects));
        }

        buf.append("}");

        return buf.toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ReplicaSetMemberDescription that = (ReplicaSetMemberDescription) o;

        if (Float.compare(that.normalizedPingTime, normalizedPingTime) != 0) {
            return false;
        }
        if (!serverAddress.equals(that.serverAddress)) {
            return false;
        }
        if (!serverDescription.equals(that.serverDescription)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = serverAddress.hashCode();
        result = 31 * result + serverDescription.hashCode();
        result = 31 * result + (normalizedPingTime != +0.0f ? Float.floatToIntBits(normalizedPingTime) : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ReplicaSetMemberDescription{"
                + "serverAddress=" + serverAddress
                + ", serverDescription=" + serverDescription
                + ", normalizedPingTime=" + normalizedPingTime
                + '}';
    }
}
