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

import org.bson.types.Document;
import org.mongodb.ServerAddress;
import org.mongodb.annotations.Immutable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Represents the state of a node in the replica set.  Instances of this class are immutable.
 * <p/>
 * NOT PART OF PUBLIC API YET
 */
@Immutable
public class ReplicaSetMember extends Node {
    private final Set<Tag> tags;
    private final boolean isPrimary;
    private final boolean isSecondary;
    private final String setName;

    public ReplicaSetMember(final ServerAddress serverAddress, final String setName, final float pingTime,
                            final boolean ok, final boolean isPrimary, final boolean isSecondary,
                            final Set<Tag> tags, final int maxBsonObjectSize) {
        super(pingTime, serverAddress, maxBsonObjectSize, ok);
        this.setName = setName;
        this.isPrimary = isPrimary;
        this.isSecondary = isSecondary;
        this.tags = Collections.unmodifiableSet(new HashSet<Tag>(tags));
    }

    public ReplicaSetMember(final ServerAddress serverAddress) {
        this(serverAddress, null, 0, false, false, false, new HashSet<Tag>(), 0);
    }

    public boolean primary() {
        return isOk() && isPrimary;
    }

    public boolean secondary() {
        return isOk() && isSecondary;
    }

    public String getSetName() {
        return setName;
    }

    public Set<Tag> getTags() {
        return tags;
    }

    public String toJSON() {
        final StringBuilder buf = new StringBuilder();
        buf.append("{ address:'").append(getAddress()).append("', ");
        buf.append("ok:").append(isOk()).append(", ");
        buf.append("ping:").append(super.getPingTime()).append(", ");
        buf.append("isPrimary:").append(isPrimary).append(", ");
        buf.append("isSecondary:").append(isSecondary).append(", ");
        buf.append("setName:").append(setName).append(", ");
        buf.append("maxBsonObjectSize:").append(getMaxBsonObjectSize()).append(", ");
        if (tags != null && tags.size() > 0) {
            final List<Document> tagObjects = new ArrayList<Document>();
            for (final Tag tag : tags) {
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
        if (!super.equals(o)) {
            return false;
        }

        final ReplicaSetMember that = (ReplicaSetMember) o;

        if (isPrimary != that.isPrimary) {
            return false;
        }
        if (isSecondary != that.isSecondary) {
            return false;
        }
        if (setName != null ? !setName.equals(that.setName) : that.setName != null) {
            return false;
        }
        if (!tags.equals(that.tags)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + tags.hashCode();
        result = 31 * result + (isPrimary ? 1 : 0);
        result = 31 * result + (isSecondary ? 1 : 0);
        result = 31 * result + (setName != null ? setName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ReplicaSetMember{" +
                "tags=" + tags +
                ", isPrimary=" + isPrimary +
                ", isSecondary=" + isSecondary +
                ", setName='" + setName + '\'' +
                "} " + super.toString();
    }
}
