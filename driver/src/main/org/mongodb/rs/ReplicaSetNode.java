/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package org.mongodb.rs;

import org.mongodb.annotations.Immutable;
import org.bson.types.Document;
import org.mongodb.ServerAddress;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Represents the state of a node in the replica set.  Instances of this class are immutable.
 * <p/>
 * NOT PART OF PUBLIC API YET
 */
@Immutable
public class ReplicaSetNode extends Node {
    public ReplicaSetNode(final ServerAddress addr, final Set<String> names, final String setName, final float pingTime,
                   final boolean ok, final boolean isMaster, final boolean isSecondary,
                   final LinkedHashMap<String, String> tags, final int maxBsonObjectSize) {
        super(pingTime, addr, maxBsonObjectSize, ok);
        this._names = Collections.unmodifiableSet(new HashSet<String>(names));
        this._setName = setName;
        this._isMaster = isMaster;
        this._isSecondary = isSecondary;
        this._tags = Collections.unmodifiableSet(getTagsFromMap(tags));
    }

    private static Set<Tag> getTagsFromMap(final LinkedHashMap<String, String> tagMap) {
        final Set<Tag> tagSet = new HashSet<Tag>();
        for (final Map.Entry<String, String> curEntry : tagMap.entrySet()) {
            tagSet.add(new Tag(curEntry.getKey(), curEntry.getValue()));
        }
        return tagSet;
    }

    public boolean master() {
        return _ok && _isMaster;
    }

    public boolean secondary() {
        return _ok && _isSecondary;
    }

    public Set<String> getNames() {
        return _names;
    }

    public String getSetName() {
        return _setName;
    }

    public Set<Tag> getTags() {
        return _tags;
    }

    public float getPingTime() {
        return _pingTime;
    }

    public String toJSON() {
        final StringBuilder buf = new StringBuilder();
        buf.append("{ address:'").append(_addr).append("', ");
        buf.append("ok:").append(_ok).append(", ");
        buf.append("ping:").append(_pingTime).append(", ");
        buf.append("isMaster:").append(_isMaster).append(", ");
        buf.append("isSecondary:").append(_isSecondary).append(", ");
        buf.append("setName:").append(_setName).append(", ");
        buf.append("maxBsonObjectSize:").append(_maxBsonObjectSize).append(", ");
        if (_tags != null && _tags.size() > 0) {
            final List<Document> tagObjects = new ArrayList<Document>();
            for (final Tag tag : _tags) {
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

        final ReplicaSetNode node = (ReplicaSetNode) o;

        if (_isMaster != node._isMaster) {
            return false;
        }
        if (_maxBsonObjectSize != node._maxBsonObjectSize) {
            return false;
        }
        if (_isSecondary != node._isSecondary) {
            return false;
        }
        if (_ok != node._ok) {
            return false;
        }
        if (Float.compare(node._pingTime, _pingTime) != 0) {
            return false;
        }
        if (!_addr.equals(node._addr)) {
            return false;
        }
        if (!_names.equals(node._names)) {
            return false;
        }
        if (!_tags.equals(node._tags)) {
            return false;
        }
        if (!_setName.equals(node._setName)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = _addr.hashCode();
        result = 31 * result + (_pingTime != +0.0f ? Float.floatToIntBits(_pingTime) : 0);
        result = 31 * result + _names.hashCode();
        result = 31 * result + _tags.hashCode();
        result = 31 * result + (_ok ? 1 : 0);
        result = 31 * result + (_isMaster ? 1 : 0);
        result = 31 * result + (_isSecondary ? 1 : 0);
        result = 31 * result + _setName.hashCode();
        result = 31 * result + _maxBsonObjectSize;
        return result;
    }

    private final Set<String> _names;
    private final Set<Tag> _tags;
    private final boolean _isMaster;
    private final boolean _isSecondary;
    private final String _setName;
}
